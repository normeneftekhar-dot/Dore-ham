import Redis from "ioredis";

export default async function handler(_req, res) {
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("X-Content-Type-Options", "nosniff");
  if (!process.env.REDIS_URL) {
    return res.status(503).json({ ok: false, redis: "not_configured" });
  }
  const redis = new Redis(process.env.REDIS_URL, {
    lazyConnect: true,
    connectTimeout: 4000,
    maxRetriesPerRequest: 1,
    retryStrategy: () => null,
  });
  try {
    await redis.connect();
    await redis.ping();
    return res.status(200).json({ ok: true, redis: "connected" });
  } catch (_error) {
    return res.status(503).json({ ok: false, redis: "unavailable" });
  } finally {
    try { redis.disconnect(); } catch (_) {}
  }
}
