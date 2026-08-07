import {
  createHash,
  createHmac,
  randomUUID,
  timingSafeEqual
} from "node:crypto";
import Fastify from "fastify";
import pg from "pg";

const { Pool } = pg;
const SHOPIFY_API_VERSION = "2026-07";
const ROUTE_METAFIELD_NAMESPACE = "kvb";
const ROUTE_METAFIELD_KEY = "fulfilment_route";
const VAN_METAFIELD_NAMESPACE = "kvb";
const VAN_METAFIELD_KEY = "van_number";
const SHOPIFY_BATCH_SIZE = 50;
const SHOPIFY_METAFIELDS_SET_BATCH_SIZE = 25;
const PREVIEW_LIMIT = 1000;
const CSV_COLUMNS = [
  "orderReference",
  "contactEmail",
  "customerLocationName",
  "FIRST_NAME",
  "ADD_1",
  "ADD_2",
  "customerLocationAddress",
  "ADD_3",
  "TOWN",
  "POSTCODE",
  "Territory",
  "contactNumber",
  "BBOX-INC",
  "Column45",
  "UNIQUE"
];

let shopifyAccessToken = null;
let exportWorkerBusy = false;

const app = Fastify({
  logger: true
});

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

function configuredShopDomain() {
  const shop = process.env.SHOPIFY_SHOP_DOMAIN?.trim().toLowerCase();

  if (!shop || !/^[a-z0-9][a-z0-9-]*\.myshopify\.com$/.test(shop)) {
    throw new Error("SHOPIFY_SHOP_DOMAIN is not configured correctly");
  }

  return shop;
}

async function getShopifyAccessToken() {
  const now = Date.now();

  if (shopifyAccessToken?.expiresAt > now + 5 * 60 * 1000) {
    return shopifyAccessToken.token;
  }

  const response = await fetch(
    `https://${configuredShopDomain()}/admin/oauth/access_token`,
    {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body: new URLSearchParams({
        grant_type: "client_credentials",
        client_id: process.env.SHOPIFY_API_KEY || "",
        client_secret: process.env.SHOPIFY_API_SECRET || ""
      }),
      signal: AbortSignal.timeout(15000)
    }
  );
  const body = await response.json().catch(() => ({}));

  if (!response.ok || typeof body.access_token !== "string") {
    throw new Error("Unable to obtain a Shopify access token");
  }

  shopifyAccessToken = {
    token: body.access_token,
    expiresAt: now + Number(body.expires_in || 86399) * 1000
  };

  return shopifyAccessToken.token;
}

async function shopifyGraphql(query, variables, mayRefreshToken = true) {
  const response = await fetch(
    `https://${configuredShopDomain()}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": await getShopifyAccessToken()
      },
      body: JSON.stringify({ query, variables }),
      signal: AbortSignal.timeout(30000)
    }
  );

  if (response.status === 401 && mayRefreshToken) {
    shopifyAccessToken = null;
    return shopifyGraphql(query, variables, false);
  }

  const body = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(`Shopify request failed with status ${response.status}`);
  }

  if (Array.isArray(body.errors) && body.errors.length) {
    throw new Error(body.errors.map((error) => error.message).join("; "));
  }

  return body.data;
}

async function requireShopifySession(request, reply) {
  const apiKey = process.env.SHOPIFY_API_KEY;
  const apiSecret = process.env.SHOPIFY_API_SECRET;
  const authorization = request.headers.authorization;

  if (!apiKey || !apiSecret) {
    return reply.code(503).send({ error: "Shopify authentication is not configured" });
  }

  if (!authorization?.startsWith("Bearer ")) {
    return reply
      .header("X-Shopify-Retry-Invalid-Session-Request", "1")
      .code(401)
      .send({ error: "Unauthorized" });
  }

  try {
    const token = authorization.slice(7);
    const [header, payload, signature] = token.split(".");
    const expected = createHmac("sha256", apiSecret)
      .update(`${header}.${payload}`)
      .digest();
    const received = Buffer.from(signature, "base64url");

    if (
      received.length !== expected.length ||
      !timingSafeEqual(received, expected)
    ) {
      throw new Error("Invalid signature");
    }

    const claims = JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
    const audiences = Array.isArray(claims.aud) ? claims.aud : [claims.aud];
    const now = Math.floor(Date.now() / 1000);
    const shop = new URL(claims.dest).hostname.toLowerCase();

    if (
      !audiences.includes(apiKey) ||
      !claims.exp ||
      claims.exp < now ||
      (claims.nbf && claims.nbf > now) ||
      shop !== configuredShopDomain()
    ) {
      throw new Error("Invalid token");
    }

    request.shopifySession = {
      shop,
      userId: claims.sub == null ? null : String(claims.sub)
    };
  } catch {
    return reply
      .header("X-Shopify-Retry-Invalid-Session-Request", "1")
      .code(401)
      .send({ error: "Unauthorized" });
  }
}

function chunks(values, size) {
  const result = [];

  for (let index = 0; index < values.length; index += size) {
    result.push(values.slice(index, index + size));
  }

  return result;
}

function text(value) {
  return String(value || "").trim();
}

function upper(value) {
  return text(value).toUpperCase();
}

function limitedText(value, maximumCharacters) {
  return Array.from(text(value)).slice(0, maximumCharacters).join("");
}

function numericShopifyId(gid) {
  const match = String(gid || "").match(/\/(\d+)$/);
  return match ? match[1] : null;
}

function formatUkPostcode(value) {
  const compact = upper(value).replace(/\s/g, "");
  const inwardCode = compact.slice(-3);
  const outwardCode = compact.slice(0, -3);
  const validOutwardCode = /^(?:[A-Z]\d{1,2}|[A-Z]\d[A-Z]|[A-Z]{2}\d{1,2}|[A-Z]{2}\d[A-Z]|GIR)$/;

  if (
    !/^\d[ABD-HJLNP-UW-Z]{2}$/.test(inwardCode) ||
    !validOutwardCode.test(outwardCode)
  ) {
    return null;
  }

  return `${outwardCode} ${inwardCode}`;
}

function previewRow(order) {
  const address = order.shippingAddress || {};
  const customer = order.customer || {};
  const email = text(order.email || customer.email).toLowerCase();
  const firstName = upper(address.firstName || customer.firstName);
  const lastName = upper(address.lastName || customer.lastName);
  const address1 = upper(address.address1);
  const address2 = upper(address.address2);
  const address3 = "";
  const town = upper(address.city);
  const postcode = formatUkPostcode(address.zip);
  const routeCode = upper(order.metafield?.value);
  const customerNote = text(customer.customerNote?.value);

  return {
    row: {
      orderReference: text(order.name),
      contactEmail: email,
      customerLocationName: lastName,
      FIRST_NAME: firstName,
      ADD_1: address1,
      ADD_2: address2,
      customerLocationAddress: [address1, address2, address3, town, postcode || ""].join(","),
      ADD_3: address3,
      TOWN: town,
      POSTCODE: postcode || upper(address.zip),
      Territory: routeCode,
      contactNumber: text(address.phone || order.phone || customer.phone),
      "BBOX-INC": "0",
      Column45: "",
      UNIQUE: email || text(order.name)
    },
    postcode,
    routeCode,
    customerNote
  };
}

async function fetchOrdersById(orderIds) {
  const orders = new Map();

  for (const idBatch of chunks(orderIds, SHOPIFY_BATCH_SIZE)) {
    const data = await shopifyGraphql(
      `query MaxOptraPreviewOrders($ids: [ID!]!) {
        nodes(ids: $ids) {
          ... on Order {
            id
            legacyResourceId
            name
            email
            phone
            customer {
              id
              legacyResourceId
              firstName
              lastName
              email
              phone
              customerNote: metafield(
                namespace: "customer_fields"
                key: "customernote"
              ) {
                value
              }
            }
            shippingAddress {
              firstName
              lastName
              address1
              address2
              city
              zip
              phone
            }
            metafield(namespace: "${ROUTE_METAFIELD_NAMESPACE}", key: "${ROUTE_METAFIELD_KEY}") {
              value
            }
            fulfillmentOrders(first: 50) {
              nodes {
                id
                status
              }
            }
          }
        }
      }`,
      { ids: idBatch }
    );

    for (const order of data.nodes.filter(Boolean)) {
      orders.set(order.id, order);
    }
  }

  return orders;
}

function normalizedGroupPart(value) {
  return upper(value).replace(/\s+/g, " ");
}

function compareOrderReferences(left, right) {
  return left.shopify_order_reference.localeCompare(
    right.shopify_order_reference,
    "en-GB",
    { numeric: true, sensitivity: "base" }
  );
}

function deliveryGroupKey(order) {
  const row = order.csv;
  const customerKey = order.customer_id || `order:${order.order_id}`;
  const identity = {
    customer: customerKey,
    address: [
      row.ADD_1,
      row.ADD_2,
      row.ADD_3,
      row.TOWN,
      row.POSTCODE
    ].map(normalizedGroupPart),
    deliveryDate: maxOptraOrderDate(row.Territory),
    route: normalizedGroupPart(row.Territory)
  };

  return createHash("sha256")
    .update(JSON.stringify(identity))
    .digest("hex");
}

async function prepareOrders(orderIds) {
  const currentOrders = await fetchOrdersById(orderIds);
  const validOrders = [];
  const errors = [];

  for (const orderId of orderIds) {
    const order = currentOrders.get(orderId);

    if (!order) {
      errors.push({ order_id: orderId, error: "Shopify order could not be found" });
      continue;
    }

    if (!order.fulfillmentOrders.nodes.some((item) => item.status === "IN_PROGRESS")) {
      errors.push({
        order_id: orderId,
        order_name: order.name,
        error: "Order is no longer In Progress"
      });
      continue;
    }

    const preview = previewRow(order);

    if (!preview.postcode) {
      errors.push({
        order_id: orderId,
        order_name: order.name,
        error: "A valid UK delivery postcode is required"
      });
      continue;
    }

    if (!preview.routeCode) {
      errors.push({
        order_id: orderId,
        order_name: order.name,
        error: "A route assignment is required"
      });
      continue;
    }

    validOrders.push({
      order_id: order.id,
      order_numeric_id: String(
        order.legacyResourceId || numericShopifyId(order.id) || ""
      ) || null,
      customer_id: order.customer
        ? String(
          order.customer.legacyResourceId ||
          numericShopifyId(order.customer.id) ||
          ""
        ) || null
        : null,
      fulfillment_order_ids: order.fulfillmentOrders.nodes
        .filter((item) => item.status === "IN_PROGRESS")
        .map((item) => item.id),
      shopify_order_reference: text(order.name),
      customer_note: preview.customerNote,
      csv: preview.row
    });
  }

  const grouped = new Map();

  for (const order of validOrders) {
    const key = deliveryGroupKey(order);
    let group = grouped.get(key);

    if (!group) {
      group = {
        delivery_group_key: key,
        delivery_date: maxOptraOrderDate(order.csv.Territory),
        customer_id: order.customer_id,
        customer_note: order.customer_note,
        csv: { ...order.csv },
        members: []
      };
      grouped.set(key, group);
    }

    group.members.push({
      order_id: order.order_id,
      order_numeric_id: order.order_numeric_id,
      customer_id: order.customer_id,
      fulfillment_order_ids: order.fulfillment_order_ids,
      shopify_order_reference: order.shopify_order_reference
    });
  }

  const orders = Array.from(grouped.values());

  for (const group of orders) {
    group.members.sort(compareOrderReferences);
    const primary = group.members[0];
    group.order_id = primary.order_id;
    group.order_numeric_id = primary.order_numeric_id;
    group.shopify_order_count = group.members.length;
    group.shopify_order_references = group.members.map(
      (member) => member.shopify_order_reference
    );
    group.order_reference = group.shopify_order_references.join(", ");
    group.csv.orderReference = group.order_reference;
  }

  return { orders, errors };
}

function validateOrderIds(value) {
  if (!Array.isArray(value)) {
    return null;
  }

  const orderIds = Array.from(new Set(value));

  if (
    orderIds.length === 0 ||
    orderIds.length > PREVIEW_LIMIT ||
    orderIds.some(
      (orderId) =>
        typeof orderId !== "string" ||
        !/^gid:\/\/shopify\/Order\/\d+$/.test(orderId)
    )
  ) {
    return null;
  }

  return orderIds;
}

function payloadHash(order, distributionCentreReference) {
  return createHash("sha256")
    .update(JSON.stringify({
      deliveryGroupKey: order.delivery_group_key,
      shopifyOrderIds: order.members.map((member) => member.order_id).sort(),
      distributionCentreReference,
      customerNote: order.customer_note,
      csv: order.csv
    }))
    .digest("hex");
}

async function queueOrders(orders, session) {
  const distributionCentreReference =
    process.env.MAXOPTRA_DISTRIBUTION_CENTRE_REFERENCE;
  const batchId = randomUUID();
  const client = await pool.connect();
  const queued = [];
  const alreadyQueued = [];

  try {
    await client.query("BEGIN");

    for (const order of orders) {
      const row = order.csv;
      const hash = payloadHash(order, distributionCentreReference);

      await client.query(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        [`${session.shop}:${order.delivery_group_key}`]
      );

      const existing = await client.query(
        `SELECT id
         FROM maxoptra_export_queue
         WHERE shop_domain = $1
           AND delivery_group_key = $2
           AND status IN ('pending', 'processing')
         ORDER BY id DESC
         LIMIT 1`,
        [session.shop, order.delivery_group_key]
      );

      if (existing.rowCount) {
        alreadyQueued.push({
          queue_id: existing.rows[0].id,
          order_id: order.order_id,
          order_reference: row.orderReference,
          shopify_order_count: order.shopify_order_count
        });
        continue;
      }

      const result = await client.query(
        `INSERT INTO maxoptra_export_queue (
           batch_id,
           shop_domain,
           shopify_order_id,
           shopify_order_numeric_id,
           customer_id,
           requested_by_shopify_user_id,
           order_reference,
           contact_email,
           customer_location_name,
           first_name,
           address_1,
           address_2,
           customer_location_address,
           address_3,
           town,
           postcode,
           territory,
           contact_number,
           bbox_inc,
           column45,
           unique_reference,
           customer_note,
           distribution_centre_reference,
           csv_row,
           api_payload,
           payload_hash,
           delivery_group_key,
           shopify_order_count,
           status
         ) VALUES (
           $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
           $11, $12, $13, $14, $15, $16, $17, $18, $19,
           $20, $21, $22, $23, $24::jsonb, NULL, $25, $26, $27, 'pending'
         )
         RETURNING id`,
        [
          batchId,
          session.shop,
          order.order_id,
          order.order_numeric_id,
          order.customer_id,
          session.userId,
          row.orderReference,
          row.contactEmail || null,
          row.customerLocationName || null,
          row.FIRST_NAME || null,
          row.ADD_1 || null,
          row.ADD_2 || null,
          row.customerLocationAddress || null,
          row.ADD_3 || null,
          row.TOWN || null,
          row.POSTCODE || null,
          row.Territory || null,
          row.contactNumber || null,
          Number(row["BBOX-INC"]) || 0,
          row.Column45 || null,
          row.UNIQUE || null,
          order.customer_note || null,
          distributionCentreReference,
          JSON.stringify(row),
          hash,
          order.delivery_group_key,
          order.shopify_order_count
        ]
      );

      for (const member of order.members) {
        await client.query(
          `INSERT INTO maxoptra_export_members (
             queue_id,
             shopify_order_id,
             shopify_order_numeric_id,
             customer_id,
             shopify_order_reference
           ) VALUES ($1, $2, $3, $4, $5)`,
          [
            result.rows[0].id,
            member.order_id,
            member.order_numeric_id,
            member.customer_id,
            member.shopify_order_reference
          ]
        );
      }

      queued.push({
        queue_id: result.rows[0].id,
        order_id: order.order_id,
        order_reference: row.orderReference,
        shopify_order_count: order.shopify_order_count
      });
    }

    await client.query("COMMIT");
    return { batchId, queued, alreadyQueued };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

function maxOptraSettings() {
  return {
    configured: Boolean(
      process.env.MAXOPTRA_BASE_URL && process.env.MAXOPTRA_API_KEY
    ),
    distributionCentreReference:
      process.env.MAXOPTRA_DISTRIBUTION_CENTRE_REFERENCE || null,
    sendEnabled:
      String(process.env.MAXOPTRA_SEND_ENABLED).toLowerCase() === "true"
  };
}

function maxOptraUrl(path) {
  const baseUrl = process.env.MAXOPTRA_BASE_URL?.replace(/\/$/, "");

  if (!baseUrl) {
    throw new Error("MAXOPTRA_BASE_URL is not configured");
  }

  return `${baseUrl}${path}`;
}

function maxOptraHeaders() {
  return {
    Accept: "application/json",
    Authorization: `Bearer ${process.env.MAXOPTRA_API_KEY}`,
    "Content-Type": "application/json"
  };
}

function maxOptraOrderDate(routeCode) {
  const configuredDate = text(process.env.MAXOPTRA_ORDER_DATE);

  if (configuredDate) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(configuredDate)) {
      throw new Error("MAXOPTRA_ORDER_DATE must use YYYY-MM-DD format");
    }

    return configuredDate;
  }

  const routeDays = {
    SU: 0,
    MO: 1,
    TU: 2,
    WE: 3,
    TH: 4,
    FR: 5,
    SA: 6
  };
  const routePrefix = upper(routeCode).slice(0, 2);
  const targetDay = routeDays[routePrefix];

  if (targetDay == null) {
    throw new Error(`Cannot derive an order date from route ${routeCode}`);
  }

  const londonParts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Europe/London",
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).formatToParts(new Date());
  const part = (type) => Number(
    londonParts.find((item) => item.type === type)?.value
  );
  const date = new Date(Date.UTC(part("year"), part("month") - 1, part("day")));
  const daysAhead = (targetDay - date.getUTCDay() + 7) % 7;

  date.setUTCDate(date.getUTCDate() + daysAhead);
  return date.toISOString().slice(0, 10);
}

function maxOptraOrderPayload(row) {
  const contactPerson = [row.first_name, row.customer_location_name]
    .filter(Boolean)
    .join(" ")
    .trim();

  return {
    referenceNumber: row.order_reference,
    distributionCentreReference: row.distribution_centre_reference,
    task: "DELIVERY",
    priority: "NORMAL",
    additionalInstructions: limitedText(row.customer_note, 500) || undefined,
    orderDate: maxOptraOrderDate(row.territory),
    clientName: row.customer_location_name || contactPerson || row.order_reference,
    contactPerson: contactPerson || row.customer_location_name || undefined,
    contactNumber: row.contact_number || undefined,
    contactEmail: row.contact_email || undefined,
    customerLocation: {
      referenceNumber:
        row.unique_reference || row.customer_id || row.order_reference,
      name: row.customer_location_name || contactPerson || row.order_reference,
      address: row.customer_location_address
    }
  };
}

async function responseBody(response) {
  const responseText = await response.text();

  if (!responseText) {
    return {};
  }

  try {
    return JSON.parse(responseText);
  } catch {
    return { raw: responseText };
  }
}

async function claimNextExport() {
  const result = await pool.query(`
    UPDATE maxoptra_export_queue
    SET status = 'processing',
        attempt_count = attempt_count + 1,
        processing_started_at = NOW(),
        updated_at = NOW(),
        last_error = NULL
    WHERE id = (
      SELECT id
      FROM maxoptra_export_queue
      WHERE status = 'pending'
      ORDER BY requested_at, id
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    RETURNING *
  `);

  return result.rows[0] || null;
}

async function saveExportResult(id, values) {
  await pool.query(
    `UPDATE maxoptra_export_queue
     SET status = $2,
         api_payload = COALESCE($3::jsonb, api_payload),
         maxoptra_http_status = $4,
         maxoptra_response = COALESCE($5::jsonb, maxoptra_response),
         last_error = $6,
         sent_at = CASE WHEN $7 THEN COALESCE(sent_at, NOW()) ELSE sent_at END,
         confirmed_at = CASE WHEN $8 THEN NOW() ELSE confirmed_at END,
         completed_at = CASE WHEN $9 THEN NOW() ELSE completed_at END,
         updated_at = NOW()
     WHERE id = $1`,
    [
      id,
      values.status,
      values.payload ? JSON.stringify(values.payload) : null,
      values.httpStatus ?? null,
      values.response ? JSON.stringify(values.response) : null,
      values.error || null,
      Boolean(values.sent),
      Boolean(values.confirmed),
      Boolean(values.completed)
    ]
  );
}

async function fetchMaxOptraOrder(orderReference) {
  const response = await fetch(
    maxOptraUrl(`/orders/${encodeURIComponent(orderReference)}`),
    {
      method: "GET",
      headers: maxOptraHeaders(),
      signal: AbortSignal.timeout(20000)
    }
  );

  return {
    response,
    body: await responseBody(response)
  };
}

async function processExport(row) {
  const payload = maxOptraOrderPayload(row);

  await pool.query(
    `UPDATE maxoptra_export_queue
     SET api_payload = $2::jsonb, updated_at = NOW()
     WHERE id = $1`,
    [row.id, JSON.stringify(payload)]
  );

  const existing = await fetchMaxOptraOrder(row.order_reference);

  if (!existing.response.ok && existing.response.status !== 404) {
    await saveExportResult(row.id, {
      status: "failed",
      payload,
      httpStatus: existing.response.status,
      response: { lookup: existing.body },
      error: `Unable to check MaxOptra order; HTTP ${existing.response.status}`,
      completed: true
    });
    return;
  }

  const updating = existing.response.ok;
  const writeResponse = await fetch(
    updating
      ? maxOptraUrl(`/orders/${encodeURIComponent(row.order_reference)}`)
      : maxOptraUrl("/orders"),
    {
    method: updating ? "PUT" : "POST",
    headers: maxOptraHeaders(),
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(30000)
    }
  );
  const writeBody = await responseBody(writeResponse);

  if (!writeResponse.ok) {
    await saveExportResult(row.id, {
      status: "failed",
      payload,
      httpStatus: writeResponse.status,
      response: {
        operation: updating ? "update" : "create",
        lookup: existing.body,
        write: writeBody
      },
      error: `MaxOptra rejected the ${updating ? "update" : "order"} with HTTP ${writeResponse.status}`,
      completed: true
    });
    return;
  }

  await saveExportResult(row.id, {
    status: "confirmed",
    payload,
    httpStatus: writeResponse.status,
    response: {
      operation: updating ? "update" : "create",
      lookup: existing.body,
      write: writeBody
    },
    error: null,
    sent: true,
    confirmed: true,
    completed: true
  });
}

function validShiftDate(value) {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return false;
  }

  const parsed = new Date(`${value}T00:00:00.000Z`);
  return !Number.isNaN(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
}

async function fetchMaxOptraSchedule(shiftDate) {
  const distributionCentreReference =
    process.env.MAXOPTRA_DISTRIBUTION_CENTRE_REFERENCE;
  const response = await fetch(
    maxOptraUrl(
      `/schedules/dc/${encodeURIComponent(distributionCentreReference)}/${shiftDate}`
    ),
    {
      method: "GET",
      headers: maxOptraHeaders(),
      signal: AbortSignal.timeout(30000)
    }
  );

  return {
    response,
    body: await responseBody(response)
  };
}

function scheduleAllocations(body) {
  const driverShifts = Array.isArray(body?.driverShifts)
    ? body.driverShifts
    : Array.isArray(body?.data?.driverShifts)
      ? body.data.driverShifts
      : [];
  const allocations = new Map();

  for (const shift of driverShifts) {
    const vehicleName = text(shift?.vehicleName);
    const driverName = text(shift?.driverName);

    for (const run of Array.isArray(shift?.runs) ? shift.runs : []) {
      const runAllocations = Array.isArray(run?.allocations) ? run.allocations : [];

      for (const allocation of runAllocations) {
        const orderReference = text(allocation?.orderReference);
        if (!orderReference) continue;

        const current = allocations.get(orderReference);

        if (current) {
          if (current.vehicle_name !== vehicleName) {
            current.conflict = true;
            current.last_error =
              `MaxOptra returned more than one vehicle for ${orderReference}`;
          }
          continue;
        }

        allocations.set(orderReference, {
          maxoptra_order_reference: orderReference,
          vehicle_name: vehicleName,
          driver_name: driverName,
          run_reference: text(run?.reference),
          run_number: Number.isInteger(run?.runNumber) ? run.runNumber : null,
          stop_number: Number.isInteger(allocation?.sequenceNumber)
            ? allocation.sequenceNumber
            : null,
          total_stops: runAllocations.length,
          planned_arrival_at: allocation?.plannedArrivalTime || null,
          planned_completion_at: allocation?.plannedCompletionTime || null,
          status: text(allocation?.status) || null,
          conflict: false,
          last_error: null
        });
      }
    }
  }

  return Array.from(allocations.values());
}

async function loadExportMembers(shop, orderReferences) {
  if (!orderReferences.length) return new Map();

  const result = await pool.query(
    `WITH latest_queue AS (
       SELECT DISTINCT ON (order_reference)
         id,
         order_reference
       FROM maxoptra_export_queue
       WHERE shop_domain = $1
         AND order_reference = ANY($2::text[])
         AND status IN ('confirmed', 'sent', 'already_present')
       ORDER BY order_reference, id DESC
     )
     SELECT
       latest_queue.id AS queue_id,
       latest_queue.order_reference AS maxoptra_order_reference,
       member.shopify_order_id,
       member.shopify_order_numeric_id,
       member.shopify_order_reference
     FROM latest_queue
     JOIN maxoptra_export_members AS member
       ON member.queue_id = latest_queue.id
     ORDER BY latest_queue.id, member.id`,
    [shop, orderReferences]
  );
  const members = new Map();

  for (const row of result.rows) {
    if (!members.has(row.maxoptra_order_reference)) {
      members.set(row.maxoptra_order_reference, []);
    }
    members.get(row.maxoptra_order_reference).push(row);
  }

  return members;
}

async function saveDeliveryOperations(
  allocations,
  shiftDate,
  shop,
  membersByReference
) {
  if (!allocations.length) return;

  const distributionCentreReference =
    process.env.MAXOPTRA_DISTRIBUTION_CENTRE_REFERENCE;
  const syncedAt = new Date().toISOString();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    for (const allocation of allocations) {
      const members = membersByReference.get(
        allocation.maxoptra_order_reference
      );
      const queueId = members?.[0]?.queue_id || null;

      await client.query(
        `INSERT INTO maxoptra_delivery_operations (
           shop_domain,
           shift_date,
           distribution_centre_reference,
           maxoptra_order_reference,
           queue_id,
           vehicle_name,
           driver_name,
           run_reference,
           run_number,
           stop_number,
           total_stops,
           planned_arrival_at,
           planned_completion_at,
           status,
           last_synced_at
         ) VALUES (
           $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
           $11, $12, $13, $14, $15
         )
         ON CONFLICT (
           shop_domain,
           shift_date,
           maxoptra_order_reference
         ) DO UPDATE SET
           distribution_centre_reference = EXCLUDED.distribution_centre_reference,
           queue_id = COALESCE(EXCLUDED.queue_id, maxoptra_delivery_operations.queue_id),
           vehicle_name = EXCLUDED.vehicle_name,
           driver_name = EXCLUDED.driver_name,
           run_reference = EXCLUDED.run_reference,
           run_number = EXCLUDED.run_number,
           stop_number = EXCLUDED.stop_number,
           total_stops = EXCLUDED.total_stops,
           planned_arrival_at = EXCLUDED.planned_arrival_at,
           planned_completion_at = EXCLUDED.planned_completion_at,
           status = EXCLUDED.status,
           last_synced_at = EXCLUDED.last_synced_at`,
        [
          shop,
          shiftDate,
          distributionCentreReference,
          allocation.maxoptra_order_reference,
          queueId,
          allocation.vehicle_name || null,
          allocation.driver_name || null,
          allocation.run_reference || null,
          allocation.run_number,
          allocation.stop_number,
          allocation.total_stops,
          allocation.planned_arrival_at,
          allocation.planned_completion_at,
          allocation.status,
          syncedAt
        ]
      );
    }

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

async function fetchCurrentVanNumbers(orderIds) {
  const orders = new Map();

  for (const idBatch of chunks(orderIds, SHOPIFY_BATCH_SIZE)) {
    const data = await shopifyGraphql(
      `query CurrentVanNumbers($ids: [ID!]!) {
        nodes(ids: $ids) {
          ... on Order {
            id
            name
            vanNumber: metafield(
              namespace: "${VAN_METAFIELD_NAMESPACE}"
              key: "${VAN_METAFIELD_KEY}"
            ) {
              value
              compareDigest
            }
          }
        }
      }`,
      { ids: idBatch }
    );

    for (const order of data.nodes.filter(Boolean)) {
      orders.set(order.id, {
        order_name: order.name,
        value: text(order.vanNumber?.value),
        compare_digest: order.vanNumber?.compareDigest ?? null
      });
    }
  }

  return orders;
}

async function fetchVehicleAssignmentOrderDetails(orderIds) {
  const orders = new Map();

  for (const idBatch of chunks(orderIds, SHOPIFY_BATCH_SIZE)) {
    const data = await shopifyGraphql(
      `query VehicleAssignmentOrderDetails($ids: [ID!]!) {
        nodes(ids: $ids) {
          ... on Order {
            id
            name
            customer {
              id
              legacyResourceId
              firstName
              lastName
            }
          }
        }
      }`,
      { ids: idBatch }
    );

    for (const order of data.nodes.filter(Boolean)) {
      orders.set(order.id, {
        order_name: order.name,
        customer_id: order.customer
          ? String(
            order.customer.legacyResourceId ||
            numericShopifyId(order.customer.id) ||
            ""
          ) || null
          : null,
        customer_name: order.customer
          ? [order.customer.firstName, order.customer.lastName]
            .filter(Boolean)
            .join(" ")
            .trim()
          : ""
      });
    }
  }

  return orders;
}

async function listVehicleAssignments(shiftDate, shop) {
  const result = await pool.query(
    `SELECT DISTINCT ON (log.shopify_order_id)
       log.id,
       log.queue_id,
       log.shopify_order_id,
       log.shopify_order_numeric_id,
       log.shopify_order_reference,
       log.maxoptra_order_reference,
       log.van_number,
       log.assignment_result,
       log.imported_at,
       log.shopify_confirmed_at,
       member.customer_id,
       operations.driver_name,
       operations.run_number,
       operations.stop_number,
       operations.total_stops,
       operations.planned_arrival_at,
       operations.planned_completion_at,
       operations.status,
       operations.last_synced_at
     FROM maxoptra_vehicle_assignment_log AS log
     LEFT JOIN maxoptra_export_members AS member
       ON member.queue_id = log.queue_id
      AND member.shopify_order_id = log.shopify_order_id
     LEFT JOIN maxoptra_delivery_operations AS operations
       ON operations.shop_domain = log.shop_domain
      AND operations.shift_date = log.shift_date
      AND operations.maxoptra_order_reference = log.maxoptra_order_reference
     WHERE log.shop_domain = $1
       AND log.shift_date = $2
       AND log.shopify_order_id IS NOT NULL
       AND log.assignment_result IN (
         'written',
         'overwritten',
         'already_present'
       )
     ORDER BY
       log.shopify_order_id,
       log.imported_at DESC,
       log.id DESC`,
    [shop, shiftDate]
  );
  const details = await fetchVehicleAssignmentOrderDetails(
    result.rows.map((row) => row.shopify_order_id)
  );

  return result.rows.map((row) => {
    const order = details.get(row.shopify_order_id) || {};
    const customerId = order.customer_id || row.customer_id || null;
    const orderNumericId =
      row.shopify_order_numeric_id || numericShopifyId(row.shopify_order_id);

    return {
      customer_id: customerId,
      customer_name: order.customer_name || "",
      customer_admin_url: customerId
        ? `https://${shop}/admin/customers/${customerId}`
        : null,
      order_id: row.shopify_order_id,
      order_numeric_id: orderNumericId,
      order_name: order.order_name || row.shopify_order_reference,
      order_admin_url: orderNumericId
        ? `https://${shop}/admin/orders/${orderNumericId}`
        : null,
      maxoptra_order_reference: row.maxoptra_order_reference,
      van_number: row.van_number,
      driver_name: row.driver_name,
      run_number: row.run_number,
      stop_number: row.stop_number,
      total_stops: row.total_stops,
      planned_arrival_at: row.planned_arrival_at,
      planned_completion_at: row.planned_completion_at,
      status: row.status,
      last_synced_at: row.last_synced_at,
      assignment_result: row.assignment_result,
      imported_at: row.imported_at,
      shopify_confirmed_at: row.shopify_confirmed_at
    };
  });
}

async function writeVanNumberBatch(targets) {
  const data = await shopifyGraphql(
    `mutation SetOrderVanNumbers($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields {
          id
          value
        }
        userErrors {
          field
          message
          code
        }
      }
    }`,
    {
      metafields: targets.map((target) => ({
        ownerId: target.shopify_order_id,
        namespace: VAN_METAFIELD_NAMESPACE,
        key: VAN_METAFIELD_KEY,
        type: "single_line_text_field",
        value: target.van_number,
        compareDigest: target.compare_digest
      }))
    }
  );
  const payload = data.metafieldsSet;

  if (payload.userErrors.length) {
    throw new Error(
      payload.userErrors.map((error) => error.message).join("; ")
    );
  }
}

async function saveVehicleAssignmentLogs(records) {
  if (!records.length) return;

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    for (const record of records) {
      await client.query(
        `INSERT INTO maxoptra_vehicle_assignment_log (
           import_batch_id,
           queue_id,
           shop_domain,
           shift_date,
           distribution_centre_reference,
           maxoptra_order_reference,
           shopify_order_id,
           shopify_order_numeric_id,
           shopify_order_reference,
           previous_van_number,
           van_number,
           assignment_result,
           requested_by_shopify_user_id,
           shopify_confirmed_at,
           last_error
         ) VALUES (
           $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
           $11, $12, $13,
           CASE WHEN $14 THEN NOW() ELSE NULL END,
           $15
         )`,
        [
          record.import_batch_id,
          record.queue_id || null,
          record.shop_domain,
          record.shift_date,
          record.distribution_centre_reference,
          record.maxoptra_order_reference,
          record.shopify_order_id || null,
          record.shopify_order_numeric_id || null,
          record.shopify_order_reference || null,
          record.previous_van_number || null,
          record.van_number || null,
          record.assignment_result,
          record.requested_by_shopify_user_id || null,
          Boolean(record.shopify_confirmed),
          record.last_error || null
        ]
      );
    }

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

function vehicleImportCounts(records) {
  const counts = {
    written: 0,
    overwritten: 0,
    already_present: 0,
    unmapped: 0,
    unassigned: 0,
    failed: 0
  };

  for (const record of records) {
    if (Object.hasOwn(counts, record.assignment_result)) {
      counts[record.assignment_result] += 1;
    }
  }

  return counts;
}

async function importVehicleAssignments(shiftDate, session) {
  const schedule = await fetchMaxOptraSchedule(shiftDate);

  if (!schedule.response.ok) {
    throw new Error(
      `MaxOptra schedule request failed with HTTP ${schedule.response.status}`
    );
  }

  const allocations = scheduleAllocations(schedule.body);
  const membersByReference = await loadExportMembers(
    session.shop,
    allocations.map((allocation) => allocation.maxoptra_order_reference)
  );
  await saveDeliveryOperations(
    allocations,
    shiftDate,
    session.shop,
    membersByReference
  );
  const batchId = randomUUID();
  const distributionCentreReference =
    process.env.MAXOPTRA_DISTRIBUTION_CENTRE_REFERENCE;
  const common = {
    import_batch_id: batchId,
    shop_domain: session.shop,
    shift_date: shiftDate,
    distribution_centre_reference: distributionCentreReference,
    requested_by_shopify_user_id: session.userId
  };
  const records = [];
  const targets = [];
  const targetByOrderId = new Map();

  for (const allocation of allocations) {
    const members = membersByReference.get(
      allocation.maxoptra_order_reference
    );

    if (allocation.conflict) {
      records.push({
        ...common,
        maxoptra_order_reference: allocation.maxoptra_order_reference,
        van_number: allocation.vehicle_name,
        assignment_result: "failed",
        last_error: allocation.last_error
      });
      continue;
    }

    if (!allocation.vehicle_name) {
      records.push({
        ...common,
        maxoptra_order_reference: allocation.maxoptra_order_reference,
        assignment_result: "unassigned",
        last_error: "MaxOptra has not assigned a vehicle name"
      });
      continue;
    }

    if (!members?.length) {
      records.push({
        ...common,
        maxoptra_order_reference: allocation.maxoptra_order_reference,
        van_number: allocation.vehicle_name,
        assignment_result: "unmapped",
        last_error: "No matching confirmed MaxOptra export was found"
      });
      continue;
    }

    for (const member of members) {
      const existingTarget = targetByOrderId.get(member.shopify_order_id);

      if (existingTarget && existingTarget.van_number !== allocation.vehicle_name) {
        records.push({
          ...common,
          ...member,
          maxoptra_order_reference: allocation.maxoptra_order_reference,
          van_number: allocation.vehicle_name,
          assignment_result: "failed",
          last_error: "Shopify order was allocated to conflicting vehicles"
        });
        continue;
      }

      if (!existingTarget) {
        const target = {
          ...common,
          ...member,
          maxoptra_order_reference: allocation.maxoptra_order_reference,
          van_number: allocation.vehicle_name
        };
        targetByOrderId.set(member.shopify_order_id, target);
        targets.push(target);
      }
    }
  }

  const currentOrders = await fetchCurrentVanNumbers(
    targets.map((target) => target.shopify_order_id)
  );
  const writeTargets = [];

  for (const target of targets) {
    const current = currentOrders.get(target.shopify_order_id);

    if (!current) {
      records.push({
        ...target,
        assignment_result: "failed",
        last_error: "Shopify order could not be found"
      });
      continue;
    }

    target.shopify_order_reference =
      target.shopify_order_reference || current.order_name;
    target.previous_van_number = current.value;
    target.compare_digest = current.compare_digest;

    if (current.value === target.van_number) {
      records.push({
        ...target,
        assignment_result: "already_present",
        shopify_confirmed: true
      });
    } else {
      writeTargets.push(target);
    }
  }

  for (const targetBatch of chunks(
    writeTargets,
    SHOPIFY_METAFIELDS_SET_BATCH_SIZE
  )) {
    try {
      await writeVanNumberBatch(targetBatch);

      for (const target of targetBatch) {
        records.push({
          ...target,
          assignment_result: target.previous_van_number
            ? "overwritten"
            : "written",
          shopify_confirmed: true
        });
      }
    } catch (error) {
      for (const target of targetBatch) {
        records.push({
          ...target,
          assignment_result: "failed",
          last_error: error.message
        });
      }
    }
  }

  await saveVehicleAssignmentLogs(records);

  return {
    import_batch_id: batchId,
    shift_date: shiftDate,
    schedule_allocations: allocations.length,
    shopify_orders_matched: targets.length,
    ...vehicleImportCounts(records)
  };
}

async function runExportWorker() {
  if (exportWorkerBusy || !maxOptraSettings().sendEnabled) {
    return;
  }

  exportWorkerBusy = true;

  try {
    const row = await claimNextExport();

    if (row) {
      await processExport(row);
    }
  } catch (error) {
    app.log.error(error, "MaxOptra export worker failed");
  } finally {
    exportWorkerBusy = false;
  }
}

function itemsFrom(value) {
  if (Array.isArray(value)) {
    return value;
  }

  for (const key of ["items", "content", "data", "distributionCentres"]) {
    if (Array.isArray(value?.[key])) {
      return value[key];
    }
  }

  return [];
}

function safeDistributionCentre(value) {
  if (!value || typeof value !== "object") {
    return null;
  }

  return {
    reference:
      value.reference ??
      value.referenceNumber ??
      value.distributionCentreReference ??
      null,
    name: value.name ?? value.description ?? null
  };
}

app.addHook("onRequest", async (request, reply) => {
  const origin = request.headers.origin;

  if (origin) {
    reply.header("Access-Control-Allow-Origin", origin);
    reply.header("Vary", "Origin");
  }

  reply.header("Access-Control-Allow-Headers", "Authorization, Content-Type");
  reply.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
});

app.options("/*", async (_request, reply) => reply.code(204).send());

app.get("/", async () => {
  return {
    message: "KVB MaxOptra export service is running",
    maxoptra: maxOptraSettings()
  };
});

app.get("/health", async (request, reply) => {
  try {
    const result = await pool.query(`
      SELECT
        COUNT(*)::integer AS total,
        COUNT(*) FILTER (WHERE status = 'pending')::integer AS pending,
        COUNT(*) FILTER (WHERE status = 'processing')::integer AS processing,
        COUNT(*) FILTER (WHERE status = 'sent')::integer AS sent,
        COUNT(*) FILTER (WHERE status = 'confirmed')::integer AS confirmed,
        COUNT(*) FILTER (WHERE status = 'already_present')::integer
          AS already_present,
        COUNT(*) FILTER (WHERE status = 'failed')::integer AS failed,
        COUNT(*) FILTER (WHERE status = 'skipped')::integer AS skipped
      FROM maxoptra_export_queue
    `);

    const queue = result.rows[0];

    return {
      ok: true,
      database: "connected",
      maxoptra: maxOptraSettings(),
      queue: {
        total: queue.total,
        pending: queue.pending,
        processing: queue.processing,
        sent: queue.sent,
        confirmed: queue.confirmed,
        alreadyPresent: queue.already_present,
        failed: queue.failed,
        skipped: queue.skipped
      }
    };
  } catch (error) {
    request.log.error(error, "MaxOptra export healthcheck failed");

    return reply.code(503).send({
      ok: false,
      database: "disconnected",
      error: "Database health check failed"
    });
  }
});

app.get("/health/maxoptra", async (request, reply) => {
  const settings = maxOptraSettings();

  if (!settings.configured) {
    return reply.code(503).send({
      ok: false,
      maxoptra: "not configured"
    });
  }

  try {
    const response = await fetch(maxOptraUrl("/distributionCentres"), {
      method: "GET",
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${process.env.MAXOPTRA_API_KEY}`
      },
      signal: AbortSignal.timeout(15000)
    });

    if (!response.ok) {
      request.log.error(
        { statusCode: response.status },
        "MaxOptra connection check was rejected"
      );

      return reply.code(502).send({
        ok: false,
        maxoptra: "connection failed",
        statusCode: response.status
      });
    }

    const body = await response.json();
    const distributionCentres = itemsFrom(body);

    return {
      ok: true,
      maxoptra: "connected",
      distributionCentres: distributionCentres.length,
      distributionCentre: safeDistributionCentre(distributionCentres[0]),
      sendEnabled: settings.sendEnabled
    };
  } catch (error) {
    request.log.error(error, "MaxOptra connection check failed");

    return reply.code(502).send({
      ok: false,
      maxoptra: "connection failed"
    });
  }
});

app.post(
  "/api/exports/preview",
  { preHandler: requireShopifySession },
  async (request, reply) => {
    const orderIds = validateOrderIds(request.body?.order_ids);

    if (!orderIds) {
      return reply.code(400).send({
        error: `Select between 1 and ${PREVIEW_LIMIT} valid Shopify orders`
      });
    }

    try {
      const { orders, errors } = await prepareOrders(orderIds);

      return {
        preview: true,
        sendEnabled: maxOptraSettings().sendEnabled,
        distributionCentreReference:
          process.env.MAXOPTRA_DISTRIBUTION_CENTRE_REFERENCE,
        columns: CSV_COLUMNS,
        requested: orderIds.length,
        ready: orders.length,
        shopify_orders_ready: orders.reduce(
          (total, order) => total + order.shopify_order_count,
          0
        ),
        skipped: errors.length,
        orders,
        errors
      };
    } catch (error) {
      request.log.error(error, "Unable to prepare MaxOptra export preview");
      return reply.code(502).send({
        error: "Unable to prepare MaxOptra export preview"
      });
    }
  }
);

app.post(
  "/api/exports/queue",
  { preHandler: requireShopifySession },
  async (request, reply) => {
    const orderIds = validateOrderIds(request.body?.order_ids);

    if (!orderIds) {
      return reply.code(400).send({
        error: `Select between 1 and ${PREVIEW_LIMIT} valid Shopify orders`
      });
    }

    if (!process.env.MAXOPTRA_DISTRIBUTION_CENTRE_REFERENCE) {
      return reply.code(503).send({
        error: "MAXOPTRA_DISTRIBUTION_CENTRE_REFERENCE is not configured"
      });
    }

    try {
      const { orders, errors } = await prepareOrders(orderIds);
      const result = await queueOrders(orders, request.shopifySession);

      return {
        sendEnabled: maxOptraSettings().sendEnabled,
        batch_id: result.batchId,
        requested: orderIds.length,
        queued: result.queued.length,
        shopify_orders_queued: result.queued.reduce(
          (total, item) => total + item.shopify_order_count,
          0
        ),
        already_queued: result.alreadyQueued.length,
        shopify_orders_already_queued: result.alreadyQueued.reduce(
          (total, item) => total + item.shopify_order_count,
          0
        ),
        skipped: errors.length,
        queue_items: result.queued,
        already_queued_items: result.alreadyQueued,
        errors
      };
    } catch (error) {
      request.log.error(error, "Unable to queue MaxOptra export");
      return reply.code(500).send({ error: "Unable to queue MaxOptra export" });
    }
  }
);

app.post(
  "/api/vehicle-assignments/import",
  { preHandler: requireShopifySession },
  async (request, reply) => {
    const shiftDate = request.body?.shift_date;

    if (!validShiftDate(shiftDate)) {
      return reply.code(400).send({
        error: "shift_date must be a valid date in YYYY-MM-DD format"
      });
    }

    if (!maxOptraSettings().configured) {
      return reply.code(503).send({ error: "MaxOptra is not configured" });
    }

    if (!process.env.MAXOPTRA_DISTRIBUTION_CENTRE_REFERENCE) {
      return reply.code(503).send({
        error: "MAXOPTRA_DISTRIBUTION_CENTRE_REFERENCE is not configured"
      });
    }

    try {
      return await importVehicleAssignments(
        shiftDate,
        request.shopifySession
      );
    } catch (error) {
      request.log.error(error, "Unable to import MaxOptra vehicle assignments");
      return reply.code(502).send({
        error: error.message || "Unable to import MaxOptra vehicle assignments"
      });
    }
  }
);

app.get(
  "/api/vehicle-assignments",
  { preHandler: requireShopifySession },
  async (request, reply) => {
    const shiftDate = request.query?.shift_date;

    if (!validShiftDate(shiftDate)) {
      return reply.code(400).send({
        error: "shift_date must be a valid date in YYYY-MM-DD format"
      });
    }

    try {
      return await listVehicleAssignments(
        shiftDate,
        request.shopifySession.shop
      );
    } catch (error) {
      request.log.error(error, "Unable to list imported van assignments");
      return reply.code(502).send({
        error: "Unable to list imported van assignments"
      });
    }
  }
);

app.addHook("onClose", async () => {
  await pool.end();
});

const exportWorkerTimer = setInterval(runExportWorker, 5000);
exportWorkerTimer.unref();
runExportWorker();

const port = Number(process.env.PORT || 3000);

await app.listen({
  port,
  host: "0.0.0.0"
});
