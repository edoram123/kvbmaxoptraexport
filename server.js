import { createHmac, randomUUID, timingSafeEqual } from "node:crypto";
import Fastify from "fastify";
import pg from "pg";

const { Pool } = pg;
const SHOPIFY_API_VERSION = "2026-07";
const ROUTE_METAFIELD_NAMESPACE = "kvb";
const ROUTE_METAFIELD_KEY = "fulfilment_route";
const SHOPIFY_PAGE_SIZE = 100;
const SHOPIFY_METAFIELD_BATCH_SIZE = 25;

let accessTokenCache = null;

const app = Fastify({
  logger: true
});

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

function escapeHtml(value = "") {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll('"', "&quot;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function configuredShopDomain() {
  const shop = process.env.SHOPIFY_SHOP_DOMAIN?.trim().toLowerCase();

  if (!shop || !/^[a-z0-9][a-z0-9-]*\.myshopify\.com$/.test(shop)) {
    throw new Error("SHOPIFY_SHOP_DOMAIN is not configured correctly");
  }

  return shop;
}

async function getShopifyAccessToken() {
  const now = Date.now();

  if (accessTokenCache && accessTokenCache.expiresAt > now + 5 * 60 * 1000) {
    return accessTokenCache.token;
  }

  const form = new URLSearchParams({
    grant_type: "client_credentials",
    client_id: process.env.SHOPIFY_API_KEY || "",
    client_secret: process.env.SHOPIFY_API_SECRET || ""
  });
  const response = await fetch(
    "https://" + configuredShopDomain() + "/admin/oauth/access_token",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Accept: "application/json"
      },
      body: form,
      signal: AbortSignal.timeout(15000)
    }
  );
  const body = await response.json().catch(() => ({}));

  if (!response.ok || typeof body.access_token !== "string") {
    throw new Error(
      body.error_description ||
      body.error ||
      "Unable to obtain a Shopify access token"
    );
  }

  accessTokenCache = {
    token: body.access_token,
    expiresAt: now + Number(body.expires_in || 86399) * 1000
  };

  return accessTokenCache.token;
}

async function shopifyGraphql(query, variables = {}, mayRefreshToken = true) {
  const response = await fetch(
    "https://" + configuredShopDomain() +
      "/admin/api/" + SHOPIFY_API_VERSION + "/graphql.json",
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
    accessTokenCache = null;
    return shopifyGraphql(query, variables, false);
  }

  const body = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error("Shopify request failed with status " + response.status);
  }

  if (Array.isArray(body.errors) && body.errors.length) {
    throw new Error(body.errors.map((error) => error.message).join("; "));
  }

  return body.data;
}

function numericShopifyId(gid) {
  const match = String(gid || "").match(/\/(\d+)$/);
  return match ? match[1] : null;
}

function formatUkPostcode(rawPostcode) {
  const compact = String(rawPostcode || "")
    .toUpperCase()
    .replace(/\s/g, "");

  if (!/^[A-Z0-9]{5,7}$/.test(compact)) {
    return { postcode: String(rawPostcode || "").trim(), outcode: null };
  }

  const inwardCode = compact.slice(-3);
  const outwardCode = compact.slice(0, -3);
  const validOutwardCode = /^(?:[A-Z]\d{1,2}|[A-Z]\d[A-Z]|[A-Z]{2}\d{1,2}|[A-Z]{2}\d[A-Z]|GIR)$/;

  if (
    !/^\d[ABD-HJLNP-UW-Z]{2}$/.test(inwardCode) ||
    !validOutwardCode.test(outwardCode)
  ) {
    return { postcode: String(rawPostcode || "").trim(), outcode: null };
  }

  return {
    postcode: outwardCode + " " + inwardCode,
    outcode: outwardCode
  };
}

function adminUrl(resource, numericId) {
  return numericId
    ? "https://" + configuredShopDomain() + "/admin/" + resource + "/" + numericId
    : null;
}

function chunks(values, size) {
  const result = [];
  for (let index = 0; index < values.length; index += size) {
    result.push(values.slice(index, index + size));
  }
  return result;
}

async function fetchAllOrdersByStatus(fulfillmentStatus) {
  const statusQueries = {
    IN_PROGRESS: "status:in_progress",
    ON_HOLD: "status:on_hold"
  };
  const statusQuery = statusQueries[fulfillmentStatus];

  if (!statusQuery) {
    throw new Error("Unsupported Shopify fulfillment status");
  }

  const ordersById = new Map();
  let after = null;
  let hasNextPage = true;

  while (hasNextPage) {
    const data = await shopifyGraphql(
      `query ManagedFulfillmentOrders($first: Int!, $after: String, $query: String!) {
        fulfillmentOrders(first: $first, after: $after, query: $query) {
          nodes {
            id
            status
            updatedAt
            order {
              id
              legacyResourceId
              name
              customer {
                id
                legacyResourceId
                displayName
              }
              shippingAddress {
                zip
              }
              metafield(namespace: "${ROUTE_METAFIELD_NAMESPACE}", key: "${ROUTE_METAFIELD_KEY}") {
                value
              }
            }
          }
          pageInfo {
            hasNextPage
            endCursor
          }
        }
      }`,
      {
        first: SHOPIFY_PAGE_SIZE,
        after,
        query: statusQuery
      }
    );
    const connection = data.fulfillmentOrders;

    for (const fulfillmentOrder of connection.nodes) {
      if (fulfillmentOrder.status !== fulfillmentStatus || !fulfillmentOrder.order) {
        continue;
      }

      const order = fulfillmentOrder.order;
      const existing = ordersById.get(order.id);

      if (existing) {
        existing.fulfillment_order_ids.push(fulfillmentOrder.id);
        if (
          new Date(fulfillmentOrder.updatedAt).getTime() <
          new Date(existing.in_progress_since).getTime()
        ) {
          existing.in_progress_since = fulfillmentOrder.updatedAt;
        }
        continue;
      }

      const postcode = formatUkPostcode(order.shippingAddress?.zip);
      const customerId = order.customer
        ? String(order.customer.legacyResourceId)
        : null;
      const orderId = String(order.legacyResourceId || numericShopifyId(order.id));

      ordersById.set(order.id, {
        order_id: order.id,
        order_numeric_id: orderId,
        order_name: order.name,
        order_url: adminUrl("orders", orderId),
        customer_id: customerId,
        customer_name: order.customer?.displayName || null,
        customer_url: adminUrl("customers", customerId),
        postcode: postcode.postcode || null,
        outcode: postcode.outcode,
        route_code: String(order.metafield?.value || "").trim().toUpperCase() || null,
        in_progress_since: fulfillmentOrder.updatedAt,
        fulfillment_order_ids: [fulfillmentOrder.id]
      });
    }

    hasNextPage = connection.pageInfo.hasNextPage;
    after = connection.pageInfo.endCursor;
  }

  const orders = Array.from(ordersById.values());
  const orderIds = orders.map((order) => order.order_id);

  if (fulfillmentStatus === "IN_PROGRESS" && orderIds.length) {
    const result = await pool.query(
      `SELECT order_id, MAX(triggered_at) AS in_progress_since
       FROM shopify_route_assignment_log
       WHERE assignment_method = 'automatic_in_progress'
         AND order_id = ANY($1::text[])
       GROUP BY order_id`,
      [orderIds]
    );
    const loggedTimes = new Map(
      result.rows.map((row) => [row.order_id, row.in_progress_since])
    );

    for (const order of orders) {
      if (loggedTimes.has(order.order_id)) {
        order.in_progress_since = loggedTimes.get(order.order_id);
      }
    }
  }

  return orders;
}

function lowIncomeThreshold() {
  const configured = Number(process.env.LOW_INCOME_THRESHOLD_GBP ?? "15");
  return Number.isFinite(configured) && configured > 0 ? configured : 15;
}

function lowIncomeExcludedProductTerm() {
  return String(process.env.LOW_INCOME_EXCLUDED_PRODUCT_TERM ?? "BOX")
    .trim()
    .toUpperCase() || "BOX";
}

async function fetchLowIncomeOrders() {
  const statuses = [
    { apiStatus: "OPEN", query: "status:open", label: "Unfulfilled" },
    { apiStatus: "IN_PROGRESS", query: "status:in_progress", label: "In Progress" },
    { apiStatus: "ON_HOLD", query: "status:on_hold", label: "On Hold" }
  ];
  const ordersById = new Map();
  const excludedProductTerm = lowIncomeExcludedProductTerm();

  for (const status of statuses) {
    let after = null;
    let hasNextPage = true;

    while (hasNextPage) {
      const data = await shopifyGraphql(
        `query LowIncomeFulfillmentOrders($first: Int!, $after: String, $query: String!) {
          fulfillmentOrders(first: $first, after: $after, query: $query) {
            nodes {
              id
              status
              order {
                id
                legacyResourceId
                name
                processedAt
                customer {
                  id
                  legacyResourceId
                  displayName
                }
                currentTotalPriceSet {
                  shopMoney {
                    amount
                    currencyCode
                  }
                }
                lineItems(first: 250) {
                  nodes {
                    title
                  }
                }
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }`,
        {
          first: SHOPIFY_PAGE_SIZE,
          after,
          query: status.query
        }
      );
      const connection = data.fulfillmentOrders;

      for (const fulfillmentOrder of connection.nodes) {
        if (
          fulfillmentOrder.status !== status.apiStatus ||
          !fulfillmentOrder.order
        ) {
          continue;
        }

        const order = fulfillmentOrder.order;
        const existing = ordersById.get(order.id);

        if (existing) {
          if (!existing.statuses.includes(status.label)) {
            existing.statuses.push(status.label);
          }
          continue;
        }

        const money = order.currentTotalPriceSet.shopMoney;
        if (money.currencyCode !== "GBP") {
          throw new Error(
            "Low Income comparisons require the Shopify shop currency to be GBP"
          );
        }

        const customerId = order.customer
          ? String(order.customer.legacyResourceId)
          : null;
        const orderId = String(order.legacyResourceId || numericShopifyId(order.id));

        ordersById.set(order.id, {
          order_id: order.id,
          order_name: order.name,
          order_url: adminUrl("orders", orderId),
          customer_id: customerId,
          customer_name: order.customer?.displayName || null,
          customer_url: adminUrl("customers", customerId),
          amount: Number(money.amount),
          currency: money.currencyCode,
          processed_at: order.processedAt,
          has_excluded_product: order.lineItems.nodes.some((lineItem) =>
            String(lineItem.title || "")
              .toUpperCase()
              .includes(excludedProductTerm)
          ),
          statuses: [status.label]
        });
      }

      hasNextPage = connection.pageInfo.hasNextPage;
      after = connection.pageInfo.endCursor;
    }
  }

  const statusOrder = new Map([
    ["Unfulfilled", 0],
    ["In Progress", 1],
    ["On Hold", 2]
  ]);

  return Array.from(ordersById.values()).map((order) => ({
    ...order,
    statuses: order.statuses.sort(
      (left, right) => statusOrder.get(left) - statusOrder.get(right)
    )
  }));
}

async function fetchOrdersForRouteUpdate(orderIds) {
  const orders = new Map();

  for (const idBatch of chunks(orderIds, 50)) {
    const data = await shopifyGraphql(
      `query OrdersForRouteUpdate($ids: [ID!]!) {
        nodes(ids: $ids) {
          ... on Order {
            id
            name
            shippingAddress {
              zip
            }
            metafield(namespace: "${ROUTE_METAFIELD_NAMESPACE}", key: "${ROUTE_METAFIELD_KEY}") {
              value
              compareDigest
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

async function setOrderRouteBatch(entries, routeCode) {
  const metafields = entries.map((entry) => ({
    ownerId: entry.order.id,
    namespace: ROUTE_METAFIELD_NAMESPACE,
    key: ROUTE_METAFIELD_KEY,
    type: "single_line_text_field",
    value: routeCode,
    compareDigest: entry.order.metafield?.compareDigest ?? null
  }));
  const data = await shopifyGraphql(
    `mutation SetInProgressOrderRoutes($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields {
          value
          owner {
            ... on Order {
              id
            }
          }
        }
        userErrors {
          field
          message
          code
        }
      }
    }`,
    { metafields }
  );
  const result = data.metafieldsSet;

  if (result.userErrors.length) {
    throw new Error(result.userErrors.map((error) => error.message).join("; "));
  }

  const confirmedOrderIds = new Set(
    (result.metafields || [])
      .filter((metafield) => metafield.value === routeCode)
      .map((metafield) => metafield.owner?.id)
      .filter(Boolean)
  );

  for (const entry of entries) {
    if (!confirmedOrderIds.has(entry.order.id)) {
      throw new Error("Shopify did not confirm every requested route change");
    }
  }
}

async function recordAssignmentLogs(records) {
  if (!records.length) return;

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    for (const record of records) {
      await client.query(
        `INSERT INTO shopify_route_assignment_log (
           batch_id,
           shop_domain,
           order_id,
           order_name,
           fulfillment_order_id,
           postcode,
           outcode,
           previous_route_code,
           route_code,
           assignment_method,
           assignment_result,
           triggered_at,
           shopify_confirmed_at,
           last_error
         ) VALUES (
           $1, $2, $3, $4, $5, $6, $7,
           $8, $9, $10, $11, $12, $13, $14
         )`,
        [
          record.batchId,
          record.shopDomain,
          record.orderId,
          record.orderName,
          record.fulfillmentOrderId,
          record.postcode,
          record.outcode,
          record.previousRouteCode,
          record.routeCode,
          record.assignmentMethod,
          record.assignmentResult,
          record.triggeredAt,
          record.shopifyConfirmedAt,
          record.lastError
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

async function requireShopifySession(request, reply) {
  const apiKey = process.env.SHOPIFY_API_KEY;
  const apiSecret = process.env.SHOPIFY_API_SECRET;
  const authorization = request.headers.authorization;

  if (!apiKey || !apiSecret) {
    return reply.code(503).send({
      error: "Shopify authentication is not configured"
    });
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

    const claims = JSON.parse(
      Buffer.from(payload, "base64url").toString("utf8")
    );

    const audiences = Array.isArray(claims.aud)
      ? claims.aud
      : [claims.aud];

    const now = Math.floor(Date.now() / 1000);

    if (
      !audiences.includes(apiKey) ||
      !claims.exp ||
      claims.exp < now ||
      (claims.nbf && claims.nbf > now)
    ) {
      throw new Error("Invalid token");
    }

    const shop = new URL(claims.dest).hostname.toLowerCase();
    const configuredShop =
      process.env.SHOPIFY_SHOP_DOMAIN?.toLowerCase();

    if (
      !shop.endsWith(".myshopify.com") ||
      (configuredShop && shop !== configuredShop)
    ) {
      throw new Error("Invalid shop");
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

app.addHook("onSend", async (_request, reply, payload) => {
  reply.header(
    "Content-Security-Policy",
    "frame-ancestors https://admin.shopify.com https://*.myshopify.com"
  );

  return payload;
});

app.get("/", async (_request, reply) => {
  const apiKey = escapeHtml(process.env.SHOPIFY_API_KEY);
  const maxOptraExportServiceUrl = JSON.stringify(
    String(process.env.MAXOPTRA_EXPORT_SERVICE_URL || "").replace(/\/+$/, "")
  ).replace(/</g, "\\u003c");

  return reply.type("text/html; charset=utf-8").send(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="shopify-api-key" content="${apiKey}">
  <script src="https://cdn.shopify.com/shopifycloud/app-bridge.js"></script>
  <title>Postcode routes</title>
  <style>
    * { box-sizing: border-box; }
    body { margin: 0; background: #f1f1f1; color: #202223; font-family: Inter, Arial, sans-serif; }
    main { max-width: 1100px; margin: 0 auto; padding: 28px 24px 48px; }
    .heading { display: flex; justify-content: space-between; align-items: end; gap: 16px; margin-bottom: 20px; }
    h1 { margin: 0 0 5px; font-size: 24px; }
    .subtitle, #count, #change-count { color: #616161; font-size: 14px; }
    [hidden] { display: none !important; }
    .tabs { display: flex; gap: 4px; margin-bottom: 16px; border-bottom: 1px solid #c9cccf; }
    .tab { min-height: 42px; padding: 8px 14px; border: 0; border-bottom: 3px solid transparent; border-radius: 0; background: transparent; color: #4a4a4a; }
    .tab:hover { background: #e8e8e8; }
    .tab[aria-selected="true"] { border-bottom-color: #303030; background: transparent; color: #202223; }
    .card { overflow: hidden; background: white; border: 1px solid #e1e3e5; border-radius: 12px; }
    .toolbar { display: flex; gap: 12px; padding: 16px; border-bottom: 1px solid #e1e3e5; }
    input, select, button { min-height: 38px; padding: 7px 11px; border: 1px solid #8c9196; border-radius: 8px; background: white; font: inherit; }
    input { flex: 1; min-width: 180px; }
    input:focus, select:focus, button:focus { outline: 2px solid #005bd3; outline-offset: 1px; }
    button { cursor: pointer; border-color: #303030; background: #303030; color: white; font-weight: 600; }
    button:hover { background: #1f1f1f; }
    button:disabled { cursor: not-allowed; opacity: .5; }
    .table-wrap { overflow-x: auto; }
    table { width: 100%; border-collapse: collapse; }
    th, td { padding: 12px 16px; border-bottom: 1px solid #e1e3e5; text-align: left; font-size: 14px; }
    th { background: #f7f7f7; color: #4a4a4a; font-weight: 600; }
    .sort-button { display: flex; width: 100%; min-height: 30px; align-items: center; justify-content: space-between; gap: 8px; padding: 0; border: 0; border-radius: 4px; background: transparent; color: inherit; font-weight: 650; }
    .sort-button:hover { background: #ededed; }
    .sort-indicator { width: 16px; color: #616161; text-align: center; }
    .filter-row th { padding-top: 8px; padding-bottom: 10px; }
    .column-filter { width: 100%; min-width: 120px; min-height: 34px; padding: 5px 8px; font-size: 13px; font-weight: 400; }
    .secondary { border-color: #8c9196; background: white; color: #303030; }
    .secondary:hover { background: #f1f1f1; }
    tbody tr:last-child td { border-bottom: 0; }
    tbody tr:hover { background: #f6f6f7; }
    .code { font-weight: 650; }
    .route-display, .route-editor, .route-actions { display: flex; align-items: center; gap: 8px; }
    .route-display { justify-content: space-between; }
    .route-input { min-width: 90px; width: 130px; min-height: 34px; padding: 5px 8px; text-transform: uppercase; }
    .small-button { min-height: 32px; padding: 5px 9px; font-size: 13px; }
    .edit-button, .cancel-button { border-color: #8c9196; background: white; color: #303030; }
    .edit-button:hover, .cancel-button:hover { background: #f1f1f1; }
    .edit-error { margin-top: 6px; color: #8e1f0b; font-size: 12px; }
    .bulk-toolbar { display: flex; align-items: center; gap: 10px; padding: 14px 16px; border-bottom: 1px solid #e1e3e5; background: #fafafa; }
    .bulk-toolbar .selection-count { min-width: 105px; color: #616161; font-size: 14px; }
    .bulk-route-input { flex: 0 1 150px; min-width: 110px; text-transform: uppercase; }
    .row-checkbox, .select-visible { width: 18px; min-width: 18px; min-height: 18px; margin: 0; padding: 0; flex: none; }
    .customer-group-row td { padding-top: 10px; padding-bottom: 10px; background: #ededed; font-weight: 650; }
    .admin-link { color: #005bd3; font-weight: 600; text-decoration: none; }
    .admin-link:hover { text-decoration: underline; }
    .route-badge { display: inline-block; min-width: 48px; padding: 3px 7px; border-radius: 6px; background: #e3f1df; text-align: center; font-weight: 700; }
    .route-badge.missing { background: #fce5cd; color: #8e1f0b; }
    .bulk-message { padding: 0 16px 12px; background: #fafafa; color: #303030; font-size: 13px; }
    .bulk-message.error { color: #8e1f0b; }
    .status-filters { display: flex; align-items: center; gap: 10px; }
    .filter-check { display: inline-flex; align-items: center; gap: 5px; white-space: nowrap; font-size: 13px; }
    .filter-check input { width: 16px; min-width: 16px; min-height: 16px; margin: 0; padding: 0; flex: none; }
    .threshold-label { display: inline-flex; align-items: center; gap: 5px; white-space: nowrap; font-size: 13px; }
    .threshold-input { width: 90px; min-width: 90px; flex: none; }
    .money { white-space: nowrap; text-align: right; font-variant-numeric: tabular-nums; }
    .message { padding: 36px 16px; text-align: center; color: #616161; }
    .error { color: #8e1f0b; }
    @media (max-width: 600px) {
      main { padding: 20px 12px; }
      .heading { align-items: start; flex-direction: column; }
      .toolbar, .bulk-toolbar { align-items: stretch; flex-direction: column; }
    }
  </style>
</head>
<body>
  <main>
    <div class="heading">
      <div>
        <h1>Postcode routes</h1>
        <div class="subtitle">Delivery routes assigned by outward postcode</div>
      </div>
      <div id="count">Loading routes…</div>
    </div>

    <nav class="tabs" role="tablist" aria-label="Postcode application sections">
      <button id="routes-tab" class="tab" type="button" role="tab" aria-selected="true" aria-controls="routes-panel">Route Assignment</button>
      <button id="changes-tab" class="tab" type="button" role="tab" aria-selected="false" aria-controls="changes-panel">Route Assignment Change Log</button>
      <button id="in-progress-tab" class="tab" type="button" role="tab" aria-selected="false" aria-controls="in-progress-panel">In Progress Orders</button>
      <button id="on-hold-tab" class="tab" type="button" role="tab" aria-selected="false" aria-controls="in-progress-panel">On Hold Orders</button>
      <button id="low-income-tab" class="tab" type="button" role="tab" aria-selected="false" aria-controls="low-income-panel">Low Income</button>
      <button id="maxoptra-tab" class="tab" type="button" role="tab" aria-selected="false" aria-controls="in-progress-panel">Export to MaxOptra</button>
    </nav>

    <section id="routes-panel" class="card" role="tabpanel" aria-labelledby="routes-tab">
      <div class="toolbar">
        <input id="search" type="search" placeholder="Search all columns" aria-label="Search all columns">
        <button id="clear" class="secondary" type="button">Clear filters</button>
        <button id="refresh" class="secondary" type="button">Refresh data</button>
        <button id="export" type="button">Export CSV</button>
      </div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th scope="col"><button class="sort-button" type="button" data-route-sort="outcode">Outcode <span class="sort-indicator" aria-hidden="true"></span></button></th>
              <th scope="col"><button class="sort-button" type="button" data-route-sort="route_code">Route <span class="sort-indicator" aria-hidden="true"></span></button></th>
              <th scope="col"><button class="sort-button" type="button" data-route-sort="delivery_day">Delivery day <span class="sort-indicator" aria-hidden="true"></span></button></th>
            </tr>
            <tr class="filter-row">
              <th><input id="outcode-filter" class="column-filter" type="search" placeholder="Filter outcode" aria-label="Filter outcode"></th>
              <th><input id="route-filter" class="column-filter" type="search" placeholder="Filter route" aria-label="Filter route"></th>
              <th>
                <select id="day-filter" class="column-filter" aria-label="Filter delivery day">
                  <option value="">All days</option>
                  <option value="Wednesday">Wednesday</option>
                  <option value="Thursday">Thursday</option>
                  <option value="Friday">Friday</option>
                </select>
              </th>
            </tr>
          </thead>
          <tbody id="rows">
            <tr><td colspan="3" class="message">Loading…</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section id="changes-panel" class="card" role="tabpanel" aria-labelledby="changes-tab" hidden>
      <div class="toolbar">
        <input id="change-search" type="search" placeholder="Search the change log" aria-label="Search the change log">
        <button id="change-clear" class="secondary" type="button">Clear search</button>
        <button id="change-refresh" class="secondary" type="button">Refresh log</button>
      </div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th scope="col"><button class="sort-button" type="button" data-change-sort="id">ID <span class="sort-indicator" aria-hidden="true"></span></button></th>
              <th scope="col"><button class="sort-button" type="button" data-change-sort="changed_at">Changed at <span class="sort-indicator" aria-hidden="true"></span></button></th>
              <th scope="col"><button class="sort-button" type="button" data-change-sort="outcode">Outcode <span class="sort-indicator" aria-hidden="true"></span></button></th>
              <th scope="col"><button class="sort-button" type="button" data-change-sort="old_route_code">Old route <span class="sort-indicator" aria-hidden="true"></span></button></th>
              <th scope="col"><button class="sort-button" type="button" data-change-sort="new_route_code">New route <span class="sort-indicator" aria-hidden="true"></span></button></th>
              <th scope="col"><button class="sort-button" type="button" data-change-sort="old_delivery_day">Old day <span class="sort-indicator" aria-hidden="true"></span></button></th>
              <th scope="col"><button class="sort-button" type="button" data-change-sort="new_delivery_day">New day <span class="sort-indicator" aria-hidden="true"></span></button></th>
              <th scope="col"><button class="sort-button" type="button" data-change-sort="shopify_user_id">Shopify user ID <span class="sort-indicator" aria-hidden="true"></span></button></th>
              <th scope="col"><button class="sort-button" type="button" data-change-sort="shop_domain">Shop <span class="sort-indicator" aria-hidden="true"></span></button></th>
            </tr>
          </thead>
          <tbody id="change-rows">
            <tr><td colspan="9" class="message">Open this tab to load the change log</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section id="in-progress-panel" class="card" role="tabpanel" aria-labelledby="in-progress-tab" hidden>
      <div class="toolbar">
        <input id="in-progress-search" type="search" placeholder="Search customers, orders or postcodes" aria-label="Search In Progress orders">
        <select id="in-progress-route-filter" aria-label="Filter by current route">
          <option value="">All routes</option>
        </select>
        <button id="in-progress-clear" class="secondary" type="button">Clear filters</button>
        <button id="in-progress-refresh" class="secondary" type="button">Refresh data</button>
        <button id="in-progress-export-all" class="secondary" type="button">Export all</button>
        <button id="in-progress-export-filter" type="button">Export filter</button>
      </div>

      <div class="bulk-toolbar">
        <span id="in-progress-selection-count" class="selection-count">0 selected</span>
        <button id="in-progress-select-filtered" class="secondary small-button" type="button">Select all filtered</button>
        <button id="in-progress-clear-selection" class="secondary small-button" type="button">Clear selection</button>
        <input id="in-progress-new-route" class="bulk-route-input" type="text" maxlength="50" placeholder="New route" aria-label="New route code">
        <button id="in-progress-update-selected" type="button">Update selected</button>
        <button id="maxoptra-preview-selected" type="button" hidden>Preview MaxOptra CSV</button>
      </div>
      <div id="in-progress-bulk-message" class="bulk-message" role="status" aria-live="polite"></div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th scope="col"><input id="in-progress-select-visible" class="select-visible" type="checkbox" aria-label="Select all currently filtered orders"></th>
              <th scope="col">Customer ID</th>
              <th scope="col">Customer name</th>
              <th scope="col">V order number</th>
              <th scope="col">Postcode</th>
              <th scope="col">Outcode</th>
              <th scope="col">Current route</th>
              <th id="managed-status-since-heading" scope="col">In Progress since</th>
            </tr>
          </thead>
          <tbody id="in-progress-rows">
            <tr><td colspan="8" class="message">Open this tab to load In Progress orders</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section id="low-income-panel" class="card" role="tabpanel" aria-labelledby="low-income-tab" hidden>
      <div class="toolbar">
        <input id="low-income-search" type="search" placeholder="Search customers or orders" aria-label="Search Low Income orders">
        <label class="threshold-label" for="low-income-threshold">Below £</label>
        <input id="low-income-threshold" class="threshold-input" type="number" min="0.01" step="0.01" value="15.00" aria-label="Low Income threshold in pounds">
        <div class="status-filters" aria-label="Fulfilment statuses">
          <label class="filter-check"><input id="low-income-unfulfilled" type="checkbox" checked>Unfulfilled</label>
          <label class="filter-check"><input id="low-income-in-progress" type="checkbox" checked>In Progress</label>
          <label class="filter-check"><input id="low-income-on-hold" type="checkbox" checked>On Hold</label>
        </div>
        <button id="low-income-clear" class="secondary" type="button">Reset filters</button>
        <button id="low-income-refresh" class="secondary" type="button">Refresh data</button>
        <button id="low-income-export" type="button" disabled>Export CSV</button>
      </div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th scope="col">Customer ID</th>
              <th scope="col">Customer name</th>
              <th scope="col">V order number</th>
              <th scope="col">Status</th>
              <th scope="col" class="money">Order value</th>
              <th scope="col" class="money">Customer combined value</th>
            </tr>
          </thead>
          <tbody id="low-income-rows">
            <tr><td colspan="6" class="message">Open this tab to load Low Income orders</td></tr>
          </tbody>
        </table>
      </div>
    </section>
  </main>

  <script>
    const maxOptraExportServiceUrl = ${maxOptraExportServiceUrl};
    const rowsElement = document.getElementById("rows");
    const countElement = document.getElementById("count");
    const searchElement = document.getElementById("search");
    const outcodeFilterElement = document.getElementById("outcode-filter");
    const routeFilterElement = document.getElementById("route-filter");
    const dayFilterElement = document.getElementById("day-filter");
    const clearElement = document.getElementById("clear");
    const refreshElement = document.getElementById("refresh");
    const exportElement = document.getElementById("export");
    const routesTabElement = document.getElementById("routes-tab");
    const changesTabElement = document.getElementById("changes-tab");
    const routesPanelElement = document.getElementById("routes-panel");
    const changesPanelElement = document.getElementById("changes-panel");
    const inProgressTabElement = document.getElementById("in-progress-tab");
    const onHoldTabElement = document.getElementById("on-hold-tab");
    const maxOptraTabElement = document.getElementById("maxoptra-tab");
    const inProgressPanelElement = document.getElementById("in-progress-panel");
    const managedStatusSinceHeadingElement = document.getElementById("managed-status-since-heading");
    const inProgressRowsElement = document.getElementById("in-progress-rows");
    const inProgressSearchElement = document.getElementById("in-progress-search");
    const inProgressRouteFilterElement = document.getElementById("in-progress-route-filter");
    const inProgressClearElement = document.getElementById("in-progress-clear");
    const inProgressRefreshElement = document.getElementById("in-progress-refresh");
    const inProgressExportAllElement = document.getElementById("in-progress-export-all");
    const inProgressExportFilterElement = document.getElementById("in-progress-export-filter");
    const inProgressSelectFilteredElement = document.getElementById("in-progress-select-filtered");
    const inProgressClearSelectionElement = document.getElementById("in-progress-clear-selection");
    const inProgressSelectVisibleElement = document.getElementById("in-progress-select-visible");
    const inProgressSelectionCountElement = document.getElementById("in-progress-selection-count");
    const inProgressNewRouteElement = document.getElementById("in-progress-new-route");
    const inProgressUpdateSelectedElement = document.getElementById("in-progress-update-selected");
    const maxOptraPreviewSelectedElement = document.getElementById("maxoptra-preview-selected");
    const inProgressBulkMessageElement = document.getElementById("in-progress-bulk-message");
    const lowIncomeTabElement = document.getElementById("low-income-tab");
    const lowIncomePanelElement = document.getElementById("low-income-panel");
    const lowIncomeRowsElement = document.getElementById("low-income-rows");
    const lowIncomeSearchElement = document.getElementById("low-income-search");
    const lowIncomeThresholdElement = document.getElementById("low-income-threshold");
    const lowIncomeUnfulfilledElement = document.getElementById("low-income-unfulfilled");
    const lowIncomeInProgressElement = document.getElementById("low-income-in-progress");
    const lowIncomeOnHoldElement = document.getElementById("low-income-on-hold");
    const lowIncomeClearElement = document.getElementById("low-income-clear");
    const lowIncomeRefreshElement = document.getElementById("low-income-refresh");
    const lowIncomeExportElement = document.getElementById("low-income-export");
    const changeRowsElement = document.getElementById("change-rows");
    const changeSearchElement = document.getElementById("change-search");
    const changeClearElement = document.getElementById("change-clear");
    const changeRefreshElement = document.getElementById("change-refresh");
    const sortButtons = Array.from(document.querySelectorAll("[data-route-sort]"));
    const changeSortButtons = Array.from(document.querySelectorAll("[data-change-sort]"));
    let routes = [];
    let sortColumn = "outcode";
    let sortDirection = "asc";
    let changes = [];
    let changesLoaded = false;
    let changeSortColumn = "changed_at";
    let changeSortDirection = "desc";
    let activeTab = "routes";
    let activeManagedStatus = "in-progress";
    let inProgressOrders = [];
    let inProgressLoaded = false;
    let managedLoadRequestId = 0;
    const selectedInProgressOrderIds = new Set();
    let lowIncomeOrders = [];
    let lowIncomeLoaded = false;
    let lowIncomeDefaultThreshold = 15;

    function visibleRoutes() {
      const query = searchElement.value.trim().toUpperCase();
      const outcodeFilter = outcodeFilterElement.value.trim().toUpperCase();
      const routeFilter = routeFilterElement.value.trim().toUpperCase();
      const dayFilter = dayFilterElement.value;

      return routes
        .filter((route) => {
          const outcode = String(route.outcode || "").toUpperCase();
          const routeCode = String(route.route_code || "").toUpperCase();
          const deliveryDay = String(route.delivery_day || "");
          const matchesSearch = !query || [outcode, routeCode, deliveryDay.toUpperCase()].some((value) => value.includes(query));

          return matchesSearch
            && (!outcodeFilter || outcode.includes(outcodeFilter))
            && (!routeFilter || routeCode.includes(routeFilter))
            && (!dayFilter || deliveryDay === dayFilter);
        })
        .sort((left, right) => {
          const leftValue = String(left[sortColumn] || "");
          const rightValue = String(right[sortColumn] || "");
          const comparison = leftValue.localeCompare(rightValue, undefined, {
            numeric: true,
            sensitivity: "base"
          });
          return sortDirection === "asc" ? comparison : -comparison;
        });
    }

    function updateSortButtons() {
      for (const button of sortButtons) {
        const active = button.dataset.routeSort === sortColumn;
        const indicator = button.querySelector(".sort-indicator");
        indicator.textContent = active ? (sortDirection === "asc" ? "▲" : "▼") : "";
        button.title = active
          ? "Sorted " + sortDirection + ". Click to reverse."
          : "Sort by " + button.textContent.trim();
      }
    }

    function showRouteEditor(route, cell) {
      const editor = document.createElement("div");
      editor.className = "route-editor";

      const input = document.createElement("input");
      input.className = "route-input";
      input.value = route.route_code;
      input.maxLength = 50;
      input.setAttribute("aria-label", "Route for " + route.outcode);

      const actions = document.createElement("div");
      actions.className = "route-actions";

      const saveButton = document.createElement("button");
      saveButton.type = "button";
      saveButton.className = "small-button";
      saveButton.textContent = "Save";

      const cancelButton = document.createElement("button");
      cancelButton.type = "button";
      cancelButton.className = "small-button cancel-button";
      cancelButton.textContent = "Cancel";

      const errorElement = document.createElement("div");
      errorElement.className = "edit-error";
      errorElement.setAttribute("role", "alert");

      actions.append(saveButton, cancelButton);
      editor.append(input, actions);
      cell.replaceChildren(editor, errorElement);

      async function saveRoute() {
        const newRouteCode = input.value.trim().toUpperCase();

        if (!newRouteCode) {
          errorElement.textContent = "Route cannot be blank";
          input.focus();
          return;
        }

        input.disabled = true;
        saveButton.disabled = true;
        cancelButton.disabled = true;
        saveButton.textContent = "Saving...";
        errorElement.textContent = "";

        try {
          const response = await fetch("/api/postcode-routes/route", {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              outcode: route.outcode,
              delivery_day: route.delivery_day,
              original_route_code: route.route_code,
              route_code: newRouteCode
            })
          });
          const body = await response.json().catch(() => ({}));

          if (!response.ok) {
            throw new Error(body.error || "Unable to update route");
          }

          route.route_code = body.route_code;
          render();
        } catch (error) {
          input.disabled = false;
          saveButton.disabled = false;
          cancelButton.disabled = false;
          saveButton.textContent = "Save";
          errorElement.textContent = error.message;
          input.focus();
        }
      }

      saveButton.addEventListener("click", saveRoute);
      cancelButton.addEventListener("click", render);
      input.addEventListener("keydown", (event) => {
        if (event.key === "Enter") saveRoute();
        if (event.key === "Escape") render();
      });
      input.focus();
      input.select();
    }

    function render() {
      const visible = visibleRoutes();

      rowsElement.replaceChildren();

      if (!visible.length) {
        const row = document.createElement("tr");
        const cell = document.createElement("td");
        cell.colSpan = 3;
        cell.className = "message";
        cell.textContent = "No postcode routes found";
        row.appendChild(cell);
        rowsElement.appendChild(row);
      }

      for (const route of visible) {
        const row = document.createElement("tr");

        const outcodeCell = document.createElement("td");
        outcodeCell.textContent = route.outcode;
        outcodeCell.className = "code";

        const routeCell = document.createElement("td");
        routeCell.className = "code";
        const routeDisplay = document.createElement("div");
        routeDisplay.className = "route-display";
        const routeValue = document.createElement("span");
        routeValue.textContent = route.route_code;
        const editButton = document.createElement("button");
        editButton.type = "button";
        editButton.className = "small-button edit-button";
        editButton.textContent = "Edit";
        editButton.setAttribute("aria-label", "Edit route for " + route.outcode);
        editButton.addEventListener("click", () => showRouteEditor(route, routeCell));
        routeDisplay.append(routeValue, editButton);
        routeCell.appendChild(routeDisplay);

        const dayCell = document.createElement("td");
        dayCell.textContent = route.delivery_day;

        row.append(outcodeCell, routeCell, dayCell);
        rowsElement.appendChild(row);
      }

      if (activeTab === "routes") {
        countElement.textContent = visible.length === routes.length
          ? routes.length + " routes"
          : visible.length + " of " + routes.length + " routes";
      }
      exportElement.disabled = visible.length === 0;
      updateSortButtons();
    }

    function csvCell(value) {
      return '"' + String(value ?? "").replaceAll('"', '""') + '"';
    }

    function exportCsv() {
      const lines = [
        ["outcode", "route_code", "delivery_day"],
        ...visibleRoutes().map((route) => [route.outcode, route.route_code, route.delivery_day])
      ];
      const csv = lines
        .map((line) => line.map(csvCell).join(","))
        .join(String.fromCharCode(13, 10));
      const blob = new Blob(
        [String.fromCharCode(0xFEFF) + csv],
        { type: "text/csv;charset=utf-8" }
      );
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = "postcode-routes-" + new Date().toISOString().slice(0, 10) + ".csv";
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    }

    async function loadRoutes(initialLoad = false) {
      refreshElement.disabled = true;
      refreshElement.textContent = initialLoad ? "Loading..." : "Refreshing...";

      try {
        const response = await fetch("/api/postcode-routes");
        if (!response.ok) {
          const body = await response.json().catch(() => ({}));
          throw new Error(body.error || "Unable to load postcode routes");
        }

        routes = await response.json();
        render();
      } catch (error) {
        if (initialLoad) {
          rowsElement.innerHTML = '<tr><td colspan="3" class="message error"></td></tr>';
          rowsElement.querySelector("td").textContent = error.message;
          countElement.textContent = "Unable to load routes";
        } else {
          countElement.textContent = "Refresh failed - current data still shown";
        }
      } finally {
        refreshElement.disabled = false;
        refreshElement.textContent = "Refresh data";
      }
    }

    function visibleChanges() {
      const query = changeSearchElement.value.trim().toUpperCase();

      return changes
        .filter((change) => {
          if (!query) return true;
          return Object.values(change).some((value) =>
            String(value == null ? "" : value).toUpperCase().includes(query)
          );
        })
        .sort((left, right) => {
          let comparison;

          if (changeSortColumn === "id") {
            comparison = Number(left.id) - Number(right.id);
          } else if (changeSortColumn === "changed_at") {
            comparison = new Date(left.changed_at).getTime() - new Date(right.changed_at).getTime();
          } else {
            comparison = String(left[changeSortColumn] || "").localeCompare(
              String(right[changeSortColumn] || ""),
              undefined,
              { numeric: true, sensitivity: "base" }
            );
          }

          return changeSortDirection === "asc" ? comparison : -comparison;
        });
    }

    function updateChangeSortButtons() {
      for (const button of changeSortButtons) {
        const active = button.dataset.changeSort === changeSortColumn;
        const indicator = button.querySelector(".sort-indicator");
        indicator.textContent = active ? (changeSortDirection === "asc" ? "▲" : "▼") : "";
        button.title = active
          ? "Sorted " + changeSortDirection + ". Click to reverse."
          : "Sort by " + button.textContent.trim();
      }
    }

    function formatChangedAt(value) {
      if (!value) return "-";
      return new Intl.DateTimeFormat("en-GB", {
        dateStyle: "medium",
        timeStyle: "short",
        timeZone: "Europe/London"
      }).format(new Date(value));
    }

    function renderChanges() {
      const visible = visibleChanges();
      changeRowsElement.replaceChildren();

      if (!visible.length) {
        const row = document.createElement("tr");
        const cell = document.createElement("td");
        cell.colSpan = 9;
        cell.className = "message";
        cell.textContent = changes.length ? "No matching changes found" : "No route changes have been recorded yet";
        row.appendChild(cell);
        changeRowsElement.appendChild(row);
      }

      for (const change of visible) {
        const row = document.createElement("tr");
        const values = [
          change.id,
          formatChangedAt(change.changed_at),
          change.outcode,
          change.old_route_code,
          change.new_route_code,
          change.old_delivery_day,
          change.new_delivery_day,
          change.shopify_user_id,
          change.shop_domain
        ];

        values.forEach((value, index) => {
          const cell = document.createElement("td");
          cell.textContent = value == null || value === "" ? "-" : value;
          if ([2, 3, 4].includes(index)) cell.className = "code";
          row.appendChild(cell);
        });

        changeRowsElement.appendChild(row);
      }

      if (activeTab === "changes") {
        countElement.textContent = visible.length === changes.length
          ? changes.length + " changes"
          : visible.length + " of " + changes.length + " changes";
      }
      updateChangeSortButtons();
    }

    async function loadChanges() {
      changeRefreshElement.disabled = true;
      changeRefreshElement.textContent = changesLoaded ? "Refreshing..." : "Loading...";

      try {
        const response = await fetch("/api/postcode-route-changes");
        if (!response.ok) {
          const body = await response.json().catch(() => ({}));
          throw new Error(body.error || "Unable to load the change log");
        }

        changes = await response.json();
        changesLoaded = true;
        renderChanges();
      } catch (error) {
        if (!changesLoaded) {
          changeRowsElement.innerHTML = '<tr><td colspan="9" class="message error"></td></tr>';
          changeRowsElement.querySelector("td").textContent = error.message;
          countElement.textContent = "Unable to load change log";
        } else {
          countElement.textContent = "Refresh failed - current log still shown";
        }
      } finally {
        changeRefreshElement.disabled = false;
        changeRefreshElement.textContent = "Refresh log";
      }
    }

    function managedStatusLabel() {
      return activeManagedStatus === "on-hold" ? "On Hold" : "In Progress";
    }

    function managedOrdersEndpoint() {
      return activeManagedStatus === "on-hold"
        ? "/api/on-hold-orders"
        : "/api/in-progress-orders";
    }

    function visibleInProgressOrders() {
      const query = inProgressSearchElement.value.trim().toUpperCase();
      const routeFilter = inProgressRouteFilterElement.value;

      return inProgressOrders
        .filter((order) => {
          const routeCode = String(order.route_code || "").toUpperCase();
          const matchesRoute = !routeFilter
            || (routeFilter === "__MISSING__" ? !routeCode : routeCode === routeFilter);
          const matchesSearch = !query || [
            order.customer_id,
            order.customer_name,
            order.order_name,
            order.postcode,
            order.outcode,
            order.route_code
          ].some((value) =>
            String(value == null ? "" : value).toUpperCase().includes(query)
          );

          return matchesRoute && matchesSearch;
        })
        .sort((left, right) => {
          const leftCustomer = String(left.customer_id || "99999999999999999999");
          const rightCustomer = String(right.customer_id || "99999999999999999999");
          const customerComparison = leftCustomer.localeCompare(
            rightCustomer,
            undefined,
            { numeric: true }
          );

          if (customerComparison !== 0) return customerComparison;

          return String(left.order_name || "").localeCompare(
            String(right.order_name || ""),
            undefined,
            { numeric: true, sensitivity: "base" }
          );
        });
    }

    function updateInProgressRouteOptions() {
      const previousValue = inProgressRouteFilterElement.value;
      const routeCodes = Array.from(new Set(
        inProgressOrders
          .map((order) => String(order.route_code || "").trim().toUpperCase())
          .filter(Boolean)
      )).sort((left, right) => left.localeCompare(right, undefined, { numeric: true }));
      const hasMissing = inProgressOrders.some((order) => !order.route_code);

      inProgressRouteFilterElement.replaceChildren();

      const allOption = document.createElement("option");
      allOption.value = "";
      allOption.textContent = "All routes";
      inProgressRouteFilterElement.appendChild(allOption);

      if (hasMissing) {
        const missingOption = document.createElement("option");
        missingOption.value = "__MISSING__";
        missingOption.textContent = "Missing route";
        inProgressRouteFilterElement.appendChild(missingOption);
      }

      for (const routeCode of routeCodes) {
        const option = document.createElement("option");
        option.value = routeCode;
        option.textContent = routeCode;
        inProgressRouteFilterElement.appendChild(option);
      }

      const availableValues = Array.from(inProgressRouteFilterElement.options)
        .map((option) => option.value);
      inProgressRouteFilterElement.value = availableValues.includes(previousValue)
        ? previousValue
        : "";
    }

    function formatInProgressDate(value) {
      if (!value) return "-";
      return new Intl.DateTimeFormat("en-GB", {
        dateStyle: "medium",
        timeStyle: "short",
        timeZone: "Europe/London"
      }).format(new Date(value));
    }

    function createAdminLink(url, label) {
      if (!url) return document.createTextNode(label || "-");

      const link = document.createElement("a");
      link.className = "admin-link";
      link.href = url;
      link.target = "_blank";
      link.rel = "noopener";
      link.textContent = label;
      return link;
    }

    function appendInProgressCell(row, value, className = "") {
      const cell = document.createElement("td");
      cell.textContent = value == null || value === "" ? "-" : value;
      if (className) cell.className = className;
      row.appendChild(cell);
      return cell;
    }

    function renderInProgressOrders() {
      const visible = visibleInProgressOrders();
      const groups = new Map();

      for (const order of visible) {
        const groupKey = order.customer_id
          ? "customer:" + order.customer_id
          : "order:" + order.order_id;

        if (!groups.has(groupKey)) groups.set(groupKey, []);
        groups.get(groupKey).push(order);
      }

      inProgressRowsElement.replaceChildren();

      if (!visible.length) {
        const row = document.createElement("tr");
        const cell = document.createElement("td");
        cell.colSpan = 8;
        cell.className = "message";
        cell.textContent = inProgressOrders.length
          ? "No " + managedStatusLabel() + " orders match the filters"
          : "No " + managedStatusLabel() + " orders found";
        row.appendChild(cell);
        inProgressRowsElement.appendChild(row);
      }

      for (const groupOrders of groups.values()) {
        const firstOrder = groupOrders[0];
        const groupRow = document.createElement("tr");
        groupRow.className = "customer-group-row";
        const groupCell = document.createElement("td");
        groupCell.colSpan = 8;

        if (firstOrder.customer_id) {
          groupCell.appendChild(createAdminLink(
            firstOrder.customer_url,
            "Customer " + firstOrder.customer_id
          ));
          groupCell.appendChild(document.createTextNode(
            " — " + (firstOrder.customer_name || "Unnamed customer") +
            " — " + groupOrders.length +
            (groupOrders.length === 1 ? " order" : " orders")
          ));
        } else {
          groupCell.textContent = "No customer account — 1 order";
        }

        groupRow.appendChild(groupCell);
        inProgressRowsElement.appendChild(groupRow);

        for (const order of groupOrders) {
          const row = document.createElement("tr");

          const selectionCell = document.createElement("td");
          const checkbox = document.createElement("input");
          checkbox.type = "checkbox";
          checkbox.className = "row-checkbox";
          checkbox.checked = selectedInProgressOrderIds.has(order.order_id);
          checkbox.setAttribute("aria-label", "Select " + order.order_name);
          checkbox.addEventListener("change", () => {
            if (checkbox.checked) {
              selectedInProgressOrderIds.add(order.order_id);
            } else {
              selectedInProgressOrderIds.delete(order.order_id);
            }
            renderInProgressOrders();
          });
          selectionCell.appendChild(checkbox);
          row.appendChild(selectionCell);

          const customerCell = document.createElement("td");
          customerCell.appendChild(createAdminLink(
            order.customer_url,
            order.customer_id || "-"
          ));
          row.appendChild(customerCell);

          appendInProgressCell(row, order.customer_name);

          const orderCell = document.createElement("td");
          orderCell.appendChild(createAdminLink(order.order_url, order.order_name));
          row.appendChild(orderCell);

          appendInProgressCell(row, order.postcode, "code");
          appendInProgressCell(row, order.outcode, "code");

          const routeCell = document.createElement("td");
          const routeBadge = document.createElement("span");
          routeBadge.className = "route-badge" + (order.route_code ? "" : " missing");
          routeBadge.textContent = order.route_code || "Missing";
          routeCell.appendChild(routeBadge);
          row.appendChild(routeCell);

          appendInProgressCell(row, formatInProgressDate(order.in_progress_since));
          inProgressRowsElement.appendChild(row);
        }
      }

      const selectedVisibleCount = visible.filter((order) =>
        selectedInProgressOrderIds.has(order.order_id)
      ).length;
      inProgressSelectVisibleElement.checked = visible.length > 0
        && selectedVisibleCount === visible.length;
      inProgressSelectVisibleElement.indeterminate = selectedVisibleCount > 0
        && selectedVisibleCount < visible.length;
      inProgressSelectVisibleElement.disabled = visible.length === 0;
      inProgressSelectionCountElement.textContent =
        selectedInProgressOrderIds.size + " selected";
      inProgressUpdateSelectedElement.disabled =
        selectedInProgressOrderIds.size === 0 ||
        !inProgressNewRouteElement.value.trim();
      maxOptraPreviewSelectedElement.disabled =
        selectedInProgressOrderIds.size === 0 || !maxOptraExportServiceUrl;
      inProgressSelectFilteredElement.disabled = visible.length === 0;
      inProgressClearSelectionElement.disabled = selectedInProgressOrderIds.size === 0;
      inProgressExportAllElement.disabled = inProgressOrders.length === 0;
      inProgressExportFilterElement.disabled = visible.length === 0;

      if (
        activeTab === "in-progress" ||
        activeTab === "on-hold" ||
        activeTab === "maxoptra"
      ) {
        countElement.textContent = visible.length === inProgressOrders.length
          ? inProgressOrders.length + " " + managedStatusLabel() + " orders"
          : visible.length + " of " + inProgressOrders.length + " " +
            managedStatusLabel() + " orders";
      }
    }

    function downloadInProgressCsv(orders, filename) {
      const lines = [
        [
          "customer_id",
          "customer_name",
          "order_number",
          "postcode",
          "outcode",
          "route_code",
          activeManagedStatus === "on-hold" ? "on_hold_since" : "in_progress_since"
        ],
        ...orders.map((order) => [
          order.customer_id,
          order.customer_name,
          order.order_name,
          order.postcode,
          order.outcode,
          order.route_code,
          order.in_progress_since
        ])
      ];
      const csv = lines
        .map((line) => line.map(csvCell).join(","))
        .join(String.fromCharCode(13, 10));
      const blob = new Blob(
        [String.fromCharCode(0xFEFF) + csv],
        { type: "text/csv;charset=utf-8" }
      );
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    }

    function filteredInProgressFilename() {
      const routeFilter = inProgressRouteFilterElement.value;

      if (!routeFilter) return "ALL ROUTES.csv";
      if (routeFilter === "__MISSING__") return "MISSING.csv";

      return routeFilter.replace(/[^A-Z0-9_-]/g, "_") + ".csv";
    }

    function downloadMaxOptraPreview(body) {
      const rows = body.orders.map((order) => order.csv);
      const lines = [
        body.columns,
        ...rows.map((row) => body.columns.map((column) => row[column]))
      ];
      const csv = lines
        .map((line) => line.map(csvCell).join(","))
        .join(String.fromCharCode(13, 10));
      const routes = Array.from(new Set(
        rows.map((row) => String(row.Territory || "").trim().toUpperCase())
          .filter(Boolean)
      ));
      const filename = routes.length === 1
        ? routes[0].replace(/[^A-Z0-9_-]/g, "_") + ".csv"
        : "ALL ROUTES.csv";
      const blob = new Blob(
        [String.fromCharCode(0xFEFF) + csv],
        { type: "text/csv;charset=utf-8" }
      );
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    }

    async function previewSelectedMaxOptraOrders() {
      const orderIds = Array.from(selectedInProgressOrderIds);
      if (!orderIds.length) return;

      if (!maxOptraExportServiceUrl) {
        inProgressBulkMessageElement.className = "bulk-message error";
        inProgressBulkMessageElement.textContent =
          "MAXOPTRA_EXPORT_SERVICE_URL is not configured.";
        return;
      }

      maxOptraPreviewSelectedElement.disabled = true;
      maxOptraPreviewSelectedElement.textContent = "Preparing preview...";
      inProgressBulkMessageElement.className = "bulk-message";
      inProgressBulkMessageElement.textContent =
        "Preparing the MaxOptra CSV preview. Nothing will be sent.";

      try {
        const token = await window.shopify.idToken();
        const response = await fetch(
          maxOptraExportServiceUrl + "/api/exports/preview",
          {
            method: "POST",
            headers: {
              Authorization: "Bearer " + token,
              "Content-Type": "application/json"
            },
            body: JSON.stringify({ order_ids: orderIds })
          }
        );
        const body = await response.json().catch(() => ({}));

        if (!response.ok) {
          throw new Error(body.error || "Unable to prepare the MaxOptra preview");
        }

        if (body.ready) downloadMaxOptraPreview(body);

        inProgressBulkMessageElement.className =
          "bulk-message" + (body.skipped ? " error" : "");
        inProgressBulkMessageElement.textContent =
          body.ready + " ready, " + body.skipped +
          " skipped. Nothing was sent to MaxOptra." +
          (body.errors?.length ? " First issue: " + body.errors[0].error : "");
      } catch (error) {
        inProgressBulkMessageElement.className = "bulk-message error";
        inProgressBulkMessageElement.textContent = error.message;
      } finally {
        maxOptraPreviewSelectedElement.textContent = "Preview MaxOptra CSV";
        renderInProgressOrders();
      }
    }

    async function loadInProgressOrders(initialLoad = false) {
      const requestId = ++managedLoadRequestId;
      const requestedStatus = activeManagedStatus;
      const requestedLabel = managedStatusLabel();
      const requestedEndpoint = managedOrdersEndpoint();

      inProgressRefreshElement.disabled = true;
      inProgressRefreshElement.textContent = initialLoad ? "Loading..." : "Refreshing...";

      try {
        const response = await fetch(requestedEndpoint);
        const body = await response.json().catch(() => ({}));

        if (!response.ok) {
          throw new Error(body.error || "Unable to load " + requestedLabel + " orders");
        }

        if (requestId !== managedLoadRequestId || requestedStatus !== activeManagedStatus) {
          return;
        }

        inProgressOrders = body;
        inProgressLoaded = true;

        const currentIds = new Set(inProgressOrders.map((order) => order.order_id));
        for (const selectedId of selectedInProgressOrderIds) {
          if (!currentIds.has(selectedId)) selectedInProgressOrderIds.delete(selectedId);
        }

        updateInProgressRouteOptions();
        renderInProgressOrders();
      } catch (error) {
        if (requestId !== managedLoadRequestId || requestedStatus !== activeManagedStatus) {
          return;
        }

        if (!inProgressLoaded) {
          inProgressRowsElement.innerHTML = '<tr><td colspan="8" class="message error"></td></tr>';
          inProgressRowsElement.querySelector("td").textContent = error.message;
          countElement.textContent = "Unable to load " + requestedLabel + " orders";
        } else {
          inProgressBulkMessageElement.className = "bulk-message error";
          inProgressBulkMessageElement.textContent =
            "Refresh failed — current data is still shown: " + error.message;
        }
      } finally {
        if (requestId === managedLoadRequestId) {
          inProgressRefreshElement.disabled = false;
          inProgressRefreshElement.textContent = "Refresh data";
        }
      }
    }

    async function updateSelectedInProgressOrders() {
      const orderIds = Array.from(selectedInProgressOrderIds);
      const routeCode = inProgressNewRouteElement.value.trim().toUpperCase();

      if (!orderIds.length || !routeCode) return;

      if (!window.confirm(
        "Change " + orderIds.length + " selected " +
        (orderIds.length === 1 ? "order" : "orders") +
        " to route " + routeCode + "?"
      )) {
        return;
      }

      inProgressUpdateSelectedElement.disabled = true;
      inProgressUpdateSelectedElement.textContent = "Updating...";
      inProgressBulkMessageElement.className = "bulk-message";
      inProgressBulkMessageElement.textContent = "Updating Shopify and recording the changes...";

      try {
        const response = await fetch(managedOrdersEndpoint() + "/routes", {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            order_ids: orderIds,
            route_code: routeCode
          })
        });
        const body = await response.json().catch(() => ({}));

        if (!response.ok) {
          throw new Error(body.error || "Unable to update the selected routes");
        }

        selectedInProgressOrderIds.clear();
        for (const failure of body.errors || []) {
          selectedInProgressOrderIds.add(failure.order_id);
        }

        if (!body.failed) inProgressNewRouteElement.value = "";
        await loadInProgressOrders(false);

        inProgressBulkMessageElement.className =
          "bulk-message" + (body.failed ? " error" : "");
        inProgressBulkMessageElement.textContent =
          body.requested + " requested: " +
          body.written + " written, " +
          body.overwritten + " overwritten, " +
          body.already_present + " already present, " +
          body.failed + " failed.";
      } catch (error) {
        inProgressBulkMessageElement.className = "bulk-message error";
        inProgressBulkMessageElement.textContent = error.message;
      } finally {
        inProgressUpdateSelectedElement.textContent = "Update selected";
        renderInProgressOrders();
      }
    }

    function selectedLowIncomeStatuses() {
      const statuses = new Set();
      if (lowIncomeUnfulfilledElement.checked) statuses.add("Unfulfilled");
      if (lowIncomeInProgressElement.checked) statuses.add("In Progress");
      if (lowIncomeOnHoldElement.checked) statuses.add("On Hold");
      return statuses;
    }

    function visibleLowIncomeGroups() {
      const selectedStatuses = selectedLowIncomeStatuses();
      const threshold = Number(lowIncomeThresholdElement.value);
      const thresholdPence = Math.round(threshold * 100);
      const query = lowIncomeSearchElement.value.trim().toUpperCase();
      const groups = new Map();

      if (!Number.isFinite(threshold) || threshold <= 0 || !selectedStatuses.size) {
        return [];
      }

      for (const order of lowIncomeOrders) {
        if (!order.statuses.some((status) => selectedStatuses.has(status))) {
          continue;
        }

        const groupKey = order.customer_id
          ? "customer:" + order.customer_id
          : "order:" + order.order_id;

        if (!groups.has(groupKey)) {
          groups.set(groupKey, {
            customer_id: order.customer_id,
            customer_name: order.customer_name,
            customer_url: order.customer_url,
            total_pence: 0,
            has_excluded_product: false,
            orders: []
          });
        }

        const group = groups.get(groupKey);
        group.total_pence += Math.round(Number(order.amount) * 100);
        group.has_excluded_product =
          group.has_excluded_product || Boolean(order.has_excluded_product);
        group.orders.push(order);
      }

      return Array.from(groups.values())
        .filter((group) =>
          !group.has_excluded_product &&
          group.total_pence < thresholdPence
        )
        .filter((group) => {
          if (!query) return true;

          return [
            group.customer_id,
            group.customer_name,
            ...group.orders.flatMap((order) => [
              order.order_name,
              order.amount,
              ...order.statuses
            ])
          ].some((value) =>
            String(value == null ? "" : value).toUpperCase().includes(query)
          );
        })
        .sort((left, right) => {
          const leftCustomer = String(left.customer_id || "99999999999999999999");
          const rightCustomer = String(right.customer_id || "99999999999999999999");
          return leftCustomer.localeCompare(
            rightCustomer,
            undefined,
            { numeric: true }
          );
        });
    }

    function formatGbpFromPence(value) {
      return new Intl.NumberFormat("en-GB", {
        style: "currency",
        currency: "GBP"
      }).format(value / 100);
    }

    function exportLowIncomeCsv() {
      const groups = visibleLowIncomeGroups();
      const lines = [
        [
          "customer_id",
          "customer_name",
          "order_number",
          "status",
          "order_value_gbp",
          "customer_combined_value_gbp"
        ]
      ];

      for (const group of groups) {
        for (const order of group.orders) {
          lines.push([
            group.customer_id,
            group.customer_name,
            order.order_name,
            order.statuses.join(", "),
            Number(order.amount).toFixed(2),
            (group.total_pence / 100).toFixed(2)
          ]);
        }
      }

      const csv = lines
        .map((line) => line.map(csvCell).join(","))
        .join(String.fromCharCode(13, 10));
      const blob = new Blob(
        [String.fromCharCode(0xFEFF) + csv],
        { type: "text/csv;charset=utf-8" }
      );
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = "LOW INCOME.csv";
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    }

    function renderLowIncomeOrders() {
      const groups = visibleLowIncomeGroups();
      const threshold = Number(lowIncomeThresholdElement.value);
      const orderCount = groups.reduce(
        (total, group) => total + group.orders.length,
        0
      );

      lowIncomeRowsElement.replaceChildren();

      if (!groups.length) {
        const row = document.createElement("tr");
        const cell = document.createElement("td");
        cell.colSpan = 6;
        cell.className = "message";
        cell.textContent = lowIncomeOrders.length
          ? "No customer totals are below the selected threshold"
          : "No qualifying orders found";
        row.appendChild(cell);
        lowIncomeRowsElement.appendChild(row);
      }

      for (const group of groups) {
        const groupRow = document.createElement("tr");
        groupRow.className = "customer-group-row";
        const groupCell = document.createElement("td");
        groupCell.colSpan = 6;

        if (group.customer_id) {
          groupCell.appendChild(createAdminLink(
            group.customer_url,
            "Customer " + group.customer_id
          ));
          groupCell.appendChild(document.createTextNode(
            " — " + (group.customer_name || "Unnamed customer") +
            " — " + group.orders.length +
            (group.orders.length === 1 ? " order" : " orders") +
            " — combined " + formatGbpFromPence(group.total_pence)
          ));
        } else {
          groupCell.textContent =
            "No customer account — combined " +
            formatGbpFromPence(group.total_pence);
        }

        groupRow.appendChild(groupCell);
        lowIncomeRowsElement.appendChild(groupRow);

        for (const order of group.orders) {
          const row = document.createElement("tr");
          const customerCell = document.createElement("td");
          customerCell.appendChild(createAdminLink(
            order.customer_url,
            order.customer_id || "-"
          ));
          row.appendChild(customerCell);

          appendInProgressCell(row, order.customer_name);

          const orderCell = document.createElement("td");
          orderCell.appendChild(createAdminLink(order.order_url, order.order_name));
          row.appendChild(orderCell);

          appendInProgressCell(row, order.statuses.join(", "));

          const orderValueCell = appendInProgressCell(
            row,
            formatGbpFromPence(Math.round(Number(order.amount) * 100)),
            "money"
          );
          orderValueCell.setAttribute("data-value", String(order.amount));
          appendInProgressCell(
            row,
            formatGbpFromPence(group.total_pence),
            "money"
          );

          lowIncomeRowsElement.appendChild(row);
        }
      }

      if (activeTab === "low-income") {
        const thresholdLabel = Number.isFinite(threshold) && threshold > 0
          ? "£" + threshold.toFixed(2)
          : "the threshold";
        countElement.textContent =
          groups.length + (groups.length === 1 ? " customer" : " customers") +
          ", " + orderCount + (orderCount === 1 ? " order" : " orders") +
          " below " + thresholdLabel;
      }

      lowIncomeExportElement.disabled = groups.length === 0;
    }

    async function loadLowIncomeOrders(initialLoad = false) {
      lowIncomeRefreshElement.disabled = true;
      lowIncomeRefreshElement.textContent = initialLoad ? "Loading..." : "Refreshing...";

      try {
        const response = await fetch("/api/low-income-orders");
        const body = await response.json().catch(() => ({}));

        if (!response.ok) {
          throw new Error(body.error || "Unable to load Low Income orders");
        }

        const firstSuccessfulLoad = !lowIncomeLoaded;
        lowIncomeOrders = body.orders;
        lowIncomeDefaultThreshold = Number(body.default_threshold) || 15;
        lowIncomeLoaded = true;

        if (firstSuccessfulLoad) {
          lowIncomeThresholdElement.value = lowIncomeDefaultThreshold.toFixed(2);
        }

        renderLowIncomeOrders();
      } catch (error) {
        if (!lowIncomeLoaded) {
          lowIncomeRowsElement.innerHTML = '<tr><td colspan="6" class="message error"></td></tr>';
          lowIncomeRowsElement.querySelector("td").textContent = error.message;
          countElement.textContent = "Unable to load Low Income orders";
        }
      } finally {
        lowIncomeRefreshElement.disabled = false;
        lowIncomeRefreshElement.textContent = "Refresh data";
      }
    }

    function showTab(tabName) {
      const showRoutes = tabName === "routes";
      const showChanges = tabName === "changes";
      const showInProgress = tabName === "in-progress";
      const showOnHold = tabName === "on-hold";
      const showLowIncome = tabName === "low-income";
      const showMaxOptra = tabName === "maxoptra";
      const showManagedOrders = showInProgress || showOnHold || showMaxOptra;

      if (showManagedOrders) {
        const nextStatus = showOnHold ? "on-hold" : "in-progress";

        if (activeManagedStatus !== nextStatus) {
          activeManagedStatus = nextStatus;
          inProgressOrders = [];
          inProgressLoaded = false;
          selectedInProgressOrderIds.clear();
          inProgressSearchElement.value = "";
          inProgressRouteFilterElement.value = "";
          inProgressNewRouteElement.value = "";
          inProgressBulkMessageElement.textContent = "";
        }

        managedStatusSinceHeadingElement.textContent =
          managedStatusLabel() + " since";
        inProgressSearchElement.placeholder =
          "Search " + managedStatusLabel() + " customers, orders or postcodes";
        inProgressSearchElement.setAttribute(
          "aria-label",
          "Search " + managedStatusLabel() + " orders"
        );
        inProgressPanelElement.setAttribute(
          "aria-labelledby",
          showOnHold
            ? "on-hold-tab"
            : (showMaxOptra ? "maxoptra-tab" : "in-progress-tab")
        );
        maxOptraPreviewSelectedElement.hidden = !showMaxOptra;
      }

      activeTab = tabName;
      routesPanelElement.hidden = !showRoutes;
      changesPanelElement.hidden = !showChanges;
      inProgressPanelElement.hidden = !showManagedOrders;
      lowIncomePanelElement.hidden = !showLowIncome;
      routesTabElement.setAttribute("aria-selected", String(showRoutes));
      changesTabElement.setAttribute("aria-selected", String(showChanges));
      inProgressTabElement.setAttribute("aria-selected", String(showInProgress));
      onHoldTabElement.setAttribute("aria-selected", String(showOnHold));
      lowIncomeTabElement.setAttribute("aria-selected", String(showLowIncome));
      maxOptraTabElement.setAttribute("aria-selected", String(showMaxOptra));

      if (showRoutes) {
        render();
      } else if (showChanges) {
        if (changesLoaded) {
          renderChanges();
        } else {
          loadChanges();
        }
      } else if (showManagedOrders) {
        if (inProgressLoaded) {
          renderInProgressOrders();
        } else {
          loadInProgressOrders(true);
        }
      } else if (showLowIncome) {
        if (lowIncomeLoaded) {
          renderLowIncomeOrders();
        } else {
          loadLowIncomeOrders(true);
        }
      }
    }

    for (const element of [searchElement, outcodeFilterElement, routeFilterElement]) {
      element.addEventListener("input", render);
    }
    dayFilterElement.addEventListener("change", render);
    clearElement.addEventListener("click", () => {
      searchElement.value = "";
      outcodeFilterElement.value = "";
      routeFilterElement.value = "";
      dayFilterElement.value = "";
      render();
      searchElement.focus();
    });
    for (const button of sortButtons) {
      button.addEventListener("click", () => {
        const nextColumn = button.dataset.routeSort;
        if (sortColumn === nextColumn) {
          sortDirection = sortDirection === "asc" ? "desc" : "asc";
        } else {
          sortColumn = nextColumn;
          sortDirection = "asc";
        }
        render();
      });
    }
    for (const button of changeSortButtons) {
      button.addEventListener("click", () => {
        const nextColumn = button.dataset.changeSort;
        if (changeSortColumn === nextColumn) {
          changeSortDirection = changeSortDirection === "asc" ? "desc" : "asc";
        } else {
          changeSortColumn = nextColumn;
          changeSortDirection = "asc";
        }
        renderChanges();
      });
    }
    routesTabElement.addEventListener("click", () => showTab("routes"));
    changesTabElement.addEventListener("click", () => showTab("changes"));
    inProgressTabElement.addEventListener("click", () => showTab("in-progress"));
    onHoldTabElement.addEventListener("click", () => showTab("on-hold"));
    lowIncomeTabElement.addEventListener("click", () => showTab("low-income"));
    maxOptraTabElement.addEventListener("click", () => showTab("maxoptra"));
    changeSearchElement.addEventListener("input", renderChanges);
    changeClearElement.addEventListener("click", () => {
      changeSearchElement.value = "";
      renderChanges();
      changeSearchElement.focus();
    });
    changeRefreshElement.addEventListener("click", loadChanges);
    refreshElement.addEventListener("click", () => loadRoutes(false));
    exportElement.addEventListener("click", exportCsv);
    inProgressSearchElement.addEventListener("input", renderInProgressOrders);
    inProgressRouteFilterElement.addEventListener("change", renderInProgressOrders);
    inProgressNewRouteElement.addEventListener("input", renderInProgressOrders);
    inProgressClearElement.addEventListener("click", () => {
      inProgressSearchElement.value = "";
      inProgressRouteFilterElement.value = "";
      renderInProgressOrders();
      inProgressSearchElement.focus();
    });
    inProgressRefreshElement.addEventListener(
      "click",
      () => loadInProgressOrders(false)
    );
    inProgressExportAllElement.addEventListener("click", () => {
      downloadInProgressCsv(inProgressOrders, "ALL ROUTES.csv");
    });
    inProgressExportFilterElement.addEventListener("click", () => {
      downloadInProgressCsv(
        visibleInProgressOrders(),
        filteredInProgressFilename()
      );
    });
    inProgressSelectFilteredElement.addEventListener("click", () => {
      for (const order of visibleInProgressOrders()) {
        selectedInProgressOrderIds.add(order.order_id);
      }
      renderInProgressOrders();
    });
    inProgressClearSelectionElement.addEventListener("click", () => {
      selectedInProgressOrderIds.clear();
      renderInProgressOrders();
    });
    inProgressSelectVisibleElement.addEventListener("change", () => {
      for (const order of visibleInProgressOrders()) {
        if (inProgressSelectVisibleElement.checked) {
          selectedInProgressOrderIds.add(order.order_id);
        } else {
          selectedInProgressOrderIds.delete(order.order_id);
        }
      }
      renderInProgressOrders();
    });
    inProgressUpdateSelectedElement.addEventListener(
      "click",
      updateSelectedInProgressOrders
    );
    maxOptraPreviewSelectedElement.addEventListener(
      "click",
      previewSelectedMaxOptraOrders
    );
    lowIncomeSearchElement.addEventListener("input", renderLowIncomeOrders);
    lowIncomeThresholdElement.addEventListener("input", renderLowIncomeOrders);
    for (const statusElement of [
      lowIncomeUnfulfilledElement,
      lowIncomeInProgressElement,
      lowIncomeOnHoldElement
    ]) {
      statusElement.addEventListener("change", renderLowIncomeOrders);
    }
    lowIncomeClearElement.addEventListener("click", () => {
      lowIncomeSearchElement.value = "";
      lowIncomeThresholdElement.value = lowIncomeDefaultThreshold.toFixed(2);
      lowIncomeUnfulfilledElement.checked = true;
      lowIncomeInProgressElement.checked = true;
      lowIncomeOnHoldElement.checked = true;
      renderLowIncomeOrders();
      lowIncomeSearchElement.focus();
    });
    lowIncomeRefreshElement.addEventListener(
      "click",
      () => loadLowIncomeOrders(false)
    );
    lowIncomeExportElement.addEventListener("click", exportLowIncomeCsv);

    loadRoutes(true);
  </script>
</body>
</html>`);
});

app.get("/health", async () => {
  const result = await pool.query(`
    SELECT COUNT(*)::integer AS count
    FROM postcode_routes
  `);

  return {
    ok: true,
    database: "connected",
    postcodeRoutes: result.rows[0].count
  };
});

app.get(
  "/api/low-income-orders",
  { preHandler: requireShopifySession },
  async (request, reply) => {
    try {
      return {
        default_threshold: lowIncomeThreshold(),
        excluded_product_term: lowIncomeExcludedProductTerm(),
        currency: "GBP",
        orders: await fetchLowIncomeOrders()
      };
    } catch (error) {
      request.log.error({ error }, "Unable to load Low Income orders");
      return reply.code(502).send({
        error: error.message || "Unable to load Low Income orders"
      });
    }
  }
);

app.get(
  "/api/in-progress-orders",
  { preHandler: requireShopifySession },
  async (request, reply) => {
    try {
      const orders = await fetchAllOrdersByStatus("IN_PROGRESS");
      return orders;
    } catch (error) {
      request.log.error({ error }, "Unable to load In Progress orders");
      return reply.code(502).send({
        error: error.message || "Unable to load In Progress orders"
      });
    }
  }
);

app.get(
  "/api/on-hold-orders",
  { preHandler: requireShopifySession },
  async (request, reply) => {
    try {
      const orders = await fetchAllOrdersByStatus("ON_HOLD");
      return orders;
    } catch (error) {
      request.log.error({ error }, "Unable to load On Hold orders");
      return reply.code(502).send({
        error: error.message || "Unable to load On Hold orders"
      });
    }
  }
);

async function updateManagedOrderRoutes(
  request,
  reply,
  requiredStatus,
  statusLabel
) {
    const requestedIds = request.body?.order_ids;
    const requestedRouteCode = request.body?.route_code;

    if (!Array.isArray(requestedIds) || typeof requestedRouteCode !== "string") {
      return reply.code(400).send({ error: "Invalid route update request" });
    }

    const orderIds = Array.from(new Set(requestedIds));
    const routeCode = requestedRouteCode.trim().toUpperCase();

    if (
      orderIds.length === 0 ||
      orderIds.length > 1000 ||
      orderIds.some(
        (orderId) =>
          typeof orderId !== "string" ||
          !/^gid:\/\/shopify\/Order\/\d+$/.test(orderId)
      )
    ) {
      return reply.code(400).send({
        error: "Select between 1 and 1,000 valid Shopify orders"
      });
    }

    if (!routeCode || routeCode.length > 50 || /[\r\n]/.test(routeCode)) {
      return reply.code(400).send({
        error: "Route must contain between 1 and 50 characters"
      });
    }

    await pool.query("SELECT 1");

    const batchId = randomUUID();
    const triggeredAt = new Date().toISOString();
    const assignmentMethod = orderIds.length === 1
      ? "individual_override"
      : "bulk_override";
    const shopDomain = request.shopifySession.shop;
    const currentOrders = await fetchOrdersForRouteUpdate(orderIds);
    const logs = [];
    const pendingWrites = [];

    for (const orderId of orderIds) {
      const order = currentOrders.get(orderId);

      if (!order) {
        logs.push({
          batchId,
          shopDomain,
          orderId,
          orderName: null,
          fulfillmentOrderId: null,
          postcode: null,
          outcode: null,
          previousRouteCode: null,
          routeCode,
          assignmentMethod,
          assignmentResult: "failed",
          triggeredAt,
          shopifyConfirmedAt: null,
          lastError: "Shopify order could not be found"
        });
        continue;
      }

      const fulfillmentOrder = order.fulfillmentOrders.nodes.find(
        (candidate) => candidate.status === requiredStatus
      );
      const postcode = formatUkPostcode(order.shippingAddress?.zip);
      const previousRouteCode = String(order.metafield?.value || "")
        .trim()
        .toUpperCase() || null;
      const baseLog = {
        batchId,
        shopDomain,
        orderId: order.id,
        orderName: order.name,
        fulfillmentOrderId: fulfillmentOrder?.id || null,
        postcode: postcode.postcode || null,
        outcode: postcode.outcode,
        previousRouteCode,
        routeCode,
        assignmentMethod,
        triggeredAt
      };

      if (!fulfillmentOrder) {
        logs.push({
          ...baseLog,
          assignmentResult: "failed",
          shopifyConfirmedAt: null,
          lastError: "Order is no longer " + statusLabel
        });
      } else if (previousRouteCode === routeCode) {
        logs.push({
          ...baseLog,
          assignmentResult: "already_present",
          shopifyConfirmedAt: triggeredAt,
          lastError: null
        });
      } else {
        pendingWrites.push({ order, baseLog });
      }
    }

    for (const writeBatch of chunks(pendingWrites, SHOPIFY_METAFIELD_BATCH_SIZE)) {
      try {
        await setOrderRouteBatch(writeBatch, routeCode);
        const confirmedAt = new Date().toISOString();

        for (const entry of writeBatch) {
          logs.push({
            ...entry.baseLog,
            assignmentResult: entry.baseLog.previousRouteCode
              ? "overwritten"
              : "written",
            shopifyConfirmedAt: confirmedAt,
            lastError: null
          });
        }
      } catch (error) {
        const message = String(
          error?.message || error || "Shopify route update failed"
        ).slice(0, 2000);

        for (const entry of writeBatch) {
          logs.push({
            ...entry.baseLog,
            assignmentResult: "failed",
            shopifyConfirmedAt: null,
            lastError: message
          });
        }
      }
    }

    await recordAssignmentLogs(logs);

    const counts = {
      requested: orderIds.length,
      written: 0,
      overwritten: 0,
      already_present: 0,
      failed: 0
    };

    for (const log of logs) {
      counts[log.assignmentResult] += 1;
    }

    return {
      batch_id: batchId,
      assignment_method: assignmentMethod,
      ...counts,
      errors: logs
        .filter((log) => log.assignmentResult === "failed")
        .slice(0, 20)
        .map((log) => ({
          order_name: log.orderName,
          order_id: log.orderId,
          error: log.lastError
        }))
    };
}

app.patch(
  "/api/in-progress-orders/routes",
  { preHandler: requireShopifySession },
  async (request, reply) =>
    updateManagedOrderRoutes(request, reply, "IN_PROGRESS", "In Progress")
);

app.patch(
  "/api/on-hold-orders/routes",
  { preHandler: requireShopifySession },
  async (request, reply) =>
    updateManagedOrderRoutes(request, reply, "ON_HOLD", "On Hold")
);

app.get(
  "/api/postcode-routes",
  { preHandler: requireShopifySession },
  async () => {
    const result = await pool.query(`
      SELECT outcode, route_code, delivery_day
      FROM postcode_routes
      ORDER BY outcode
    `);

    return result.rows;
  }
);

app.get(
  "/api/postcode-route-changes",
  { preHandler: requireShopifySession },
  async () => {
    const result = await pool.query(`
      SELECT
        id,
        outcode,
        old_route_code,
        new_route_code,
        old_delivery_day,
        new_delivery_day,
        changed_at,
        shop_domain,
        shopify_user_id
      FROM postcode_route_changes
      ORDER BY changed_at DESC, id DESC
    `);

    return result.rows;
  }
);

app.patch(
  "/api/postcode-routes/route",
  { preHandler: requireShopifySession },
  async (request, reply) => {
    const {
      outcode,
      delivery_day: deliveryDay,
      original_route_code: originalRouteCode,
      route_code: requestedRouteCode
    } = request.body || {};

    if (
      typeof outcode !== "string" ||
      typeof deliveryDay !== "string" ||
      typeof originalRouteCode !== "string" ||
      typeof requestedRouteCode !== "string"
    ) {
      return reply.code(400).send({ error: "Invalid route update" });
    }

    const routeCode = requestedRouteCode.trim().toUpperCase();

    if (!routeCode || routeCode.length > 50) {
      return reply.code(400).send({
        error: "Route must contain between 1 and 50 characters"
      });
    }

    const client = await pool.connect();

    try {
      await client.query("BEGIN");

      const matches = await client.query(
        `SELECT ctid::text AS row_id
         FROM postcode_routes
         WHERE outcode = $1
           AND delivery_day = $2
           AND route_code = $3
         FOR UPDATE`,
        [outcode, deliveryDay, originalRouteCode]
      );

      if (matches.rowCount === 0) {
        await client.query("ROLLBACK");
        return reply.code(409).send({
          error: "This route changed since the table loaded. Refresh and try again."
        });
      }

      if (matches.rowCount > 1) {
        await client.query("ROLLBACK");
        return reply.code(409).send({
          error: "This row is duplicated in the database and cannot be safely updated."
        });
      }

      const result = await client.query(
        `UPDATE postcode_routes
         SET route_code = $1
         WHERE ctid = $2::tid
         RETURNING outcode, route_code, delivery_day`,
        [routeCode, matches.rows[0].row_id]
      );

      const updatedRoute = result.rows[0];

      if (
        originalRouteCode !== updatedRoute.route_code ||
        deliveryDay !== updatedRoute.delivery_day
      ) {
        await client.query(
          `INSERT INTO postcode_route_changes (
             outcode,
             old_route_code,
             new_route_code,
             old_delivery_day,
             new_delivery_day,
             shop_domain,
             shopify_user_id
           )
           VALUES ($1, $2, $3, $4, $5, $6, $7)`,
          [
            outcode,
            originalRouteCode,
            updatedRoute.route_code,
            deliveryDay,
            updatedRoute.delivery_day,
            request.shopifySession?.shop || null,
            request.shopifySession?.userId || null
          ]
        );
      }

      await client.query("COMMIT");
      return updatedRoute;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }
);

const port = Number(process.env.PORT || 3000);

await app.listen({
  port,
  host: "0.0.0.0"
});
