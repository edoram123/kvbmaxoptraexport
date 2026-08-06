import Fastify from "fastify";
import pg from "pg";

const { Pool } = pg;

const app = Fastify({
  logger: true
});

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

function maxOptraSettings() {
  return {
    configured: Boolean(
      process.env.MAXOPTRA_BASE_URL && process.env.MAXOPTRA_API_KEY
    ),
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

app.addHook("onClose", async () => {
  await pool.end();
});

const port = Number(process.env.PORT || 3000);

await app.listen({
  port,
  host: "0.0.0.0"
});
