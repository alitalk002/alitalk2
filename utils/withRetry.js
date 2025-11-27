function calcDelay({ base, factor, attempt, jitter, max }) {
  const backoff = Math.min(base * Math.pow(factor, attempt), max);
  const rand = 1 + (Math.random() * 2 - 1) * jitter; // 1±jitter
  return Math.round(backoff * rand);
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export async function withRetry(fn, opts = {}) {
  const {
    retries = 3,
    base = 800,
    factor = 2,
    jitter = 0.3,
    max = 10000,
  } = opts;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      const code = err?.cause?.code || err?.code;
      const transient =
        code === "ECONNRESET" || code === "ETIMEDOUT" || code === "EAI_AGAIN";
      if (!transient && attempt === 0) throw err; // 비일시적이면 즉시
      if (attempt === retries) throw err;
      const delay = calcDelay({ base, factor, attempt, jitter, max });
      await sleep(delay);
    }
  }
}
