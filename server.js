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
const SHOPIFY_BATCH_SIZE = 50;
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
    routeCode
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

async function reuseSavedGroupReferences(groups, shop) {
  if (!groups.length) return;

  const result = await pool.query(
    `SELECT DISTINCT ON (delivery_group_key)
       delivery_group_key,
       order_reference
     FROM maxoptra_export_queue
     WHERE shop_domain = $1
       AND delivery_group_key = ANY($2::text[])
     ORDER BY delivery_group_key, id DESC`,
    [shop, groups.map((group) => group.delivery_group_key)]
  );
  const savedReferences = new Map(
    result.rows.map((row) => [row.delivery_group_key, row.order_reference])
  );

  for (const group of groups) {
    const orderReference =
      savedReferences.get(group.delivery_group_key) ||
      group.members[0].shopify_order_reference;

    group.order_reference = orderReference;
    group.csv.orderReference = orderReference;
  }
}

async function prepareOrders(orderIds, shop) {
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
  }

  await reuseSavedGroupReferences(orders, shop);

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
           $20, $21, $22, $23::jsonb, NULL, $24, $25, $26, 'pending'
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

function maxOptraOrderPayload(row, shopifyOrderReferences = []) {
  const contactPerson = [row.first_name, row.customer_location_name]
    .filter(Boolean)
    .join(" ")
    .trim();

  return {
    referenceNumber: row.order_reference,
    distributionCentreReference: row.distribution_centre_reference,
    task: "DELIVERY",
    priority: "NORMAL",
    additionalInstructions: shopifyOrderReferences.length
      ? `Shopify orders: ${shopifyOrderReferences.join(", ")}`.slice(0, 500)
      : undefined,
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
  const members = await pool.query(
    `SELECT shopify_order_reference
     FROM maxoptra_export_members
     WHERE queue_id = $1
     ORDER BY shopify_order_reference`,
    [row.id]
  );
  const payload = maxOptraOrderPayload(
    row,
    members.rows.map((member) => member.shopify_order_reference)
  );

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
      const { orders, errors } = await prepareOrders(
        orderIds,
        request.shopifySession.shop
      );

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
      const { orders, errors } = await prepareOrders(
        orderIds,
        request.shopifySession.shop
      );
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
