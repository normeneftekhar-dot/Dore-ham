const buckets = new Map();

function send(res, status, data) {
  res.status(status).setHeader("Cache-Control", "no-store");
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");
  res.json(data);
}

function originAllowed(req) {
  const origin = req.headers.origin;
  if (!origin) return true;
  try { return new URL(origin).host === req.headers.host; }
  catch (_) { return false; }
}

export default async function handler(req, res) {
  if (req.method !== "POST") return send(res, 405, { error: "method_not_allowed" });
  if (!originAllowed(req)) return send(res, 403, { error: "origin_denied" });
  if (!process.env.OPENAI_API_KEY) return send(res, 503, { error: "ai_not_configured" });

  const ip = String(req.headers["x-forwarded-for"] || req.socket?.remoteAddress || "unknown").split(",")[0].trim();
  const now = Date.now();
  const bucket = buckets.get(ip) || { start: now, count: 0 };
  if (now - bucket.start >= 60000) { bucket.start = now; bucket.count = 0; }
  bucket.count += 1;
  buckets.set(ip, bucket);
  if (bucket.count > 12) return send(res, 429, { error: "rate_limited" });

  try {
    let body = req.body;
    if (typeof body === "string") body = JSON.parse(body);
    if (!body || typeof body.prompt !== "string" || body.prompt.length > 6000)
      return send(res, 400, { error: "invalid_prompt" });

    const endpoint = process.env.OPENAI_BASE_URL || "https://api.openai.com/v1/chat/completions";
    const upstream = await fetch(endpoint, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: process.env.OPENAI_MODEL || "gpt-4o-mini",
        messages: [{ role: "user", content: body.prompt }],
        max_tokens: 220,
        temperature: 0.8,
      }),
      signal: AbortSignal.timeout(25000),
    });
    const result = await upstream.json().catch(() => ({}));
    if (!upstream.ok) return send(res, 502, { error: "ai_provider_error" });
    const text = result.choices?.[0]?.message?.content;
    if (typeof text !== "string") return send(res, 502, { error: "empty_ai_response" });
    return send(res, 200, { text: text.slice(0, 1200) });
  } catch (err) {
    const timeout = err?.name === "TimeoutError" || err?.name === "AbortError";
    return send(res, timeout ? 504 : 400, { error: timeout ? "ai_timeout" : "bad_request" });
  }
}
