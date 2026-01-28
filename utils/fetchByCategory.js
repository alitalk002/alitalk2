import pLimit from "p-limit";
import crypto from "crypto";
import "dotenv/config";

const METHOD = "aliexpress.affiliate.product.query";
const APP_KEY = process.env.AE_APP_KEY;
const TRACKING_ID = process.env.AE_TRACKING_ID;
const APP_SECRET = process.env.AE_APP_SECRET;
const API = "https://api-sg.aliexpress.com/sync";

const FIELDS = [
  "product_id",
  "product_title",
  "product_detail_url",
  "product_main_image_url",
  "target_app_sale_price",
  "target_app_sale_price_currency",
  "promotion_link",
  "lastest_volume",
  "review_count",
  "first_level_category_id",
  "first_level_category_name",
  "second_level_category_id",
  "second_level_category_name",
].join(",");

async function fetchJsonWithRetry(
  url,
  {
    retries = 1, // 총 2회(0..4)
    base = 600, // 시작 지연(ms)
    factor = 2,
    jitter = 0.35,
    max = 10000,
    timeoutMs = 18000,
    fetchInit = {},
  } = {},
) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    const ctrl = new AbortController();
    const to = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const res = await fetch(url, { signal: ctrl.signal, ...fetchInit });
      clearTimeout(to);

      if (res.ok) {
        const txt = await res.text();
        try {
          return JSON.parse(txt);
        } catch {
          return {};
        }
      }

      // 429/5xx → 재시도
      if (res.status === 429 || (res.status >= 500 && res.status <= 599)) {
        if (attempt === retries)
          throw new Error(`HTTP ${res.status} (max retry)`);
        const ra = res.headers.get("retry-after");
        const delay = ra
          ? Number(ra) * 1000
          : calcDelay({ base, factor, attempt, jitter, max });
        await sleep(delay);
        continue;
      }

      // 그 외 4xx → 즉시 실패
      const body = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status}: ${body.slice(0, 300)}`);
    } catch (err) {
      clearTimeout(to);
      const code = err?.cause?.code || err?.code;
      const isAbort = err?.name === "AbortError";
      const transient =
        isAbort ||
        code === "ECONNRESET" ||
        code === "ETIMEDOUT" ||
        code === "EAI_AGAIN";
      if (!transient || attempt === retries) throw err;
      const delay = calcDelay({ base, factor, attempt, jitter, max });
      await sleep(delay);
    }
  }
}
function signSha256(params, secret) {
  const base = Object.keys(params)
    .filter(
      (k) => params[k] !== undefined && params[k] !== null && k !== "sign",
    )
    .sort()
    .map((k) => k + params[k])
    .join("");
  return crypto
    .createHmac("sha256", secret)
    .update(base, "utf8")
    .digest("hex")
    .toUpperCase();
}
function parseProducts(raw) {
  const arr =
    raw?.aliexpress_affiliate_product_query_response?.resp_result?.result
      ?.products?.product ??
    raw?.resp_result?.result?.products?.product ??
    raw?.result?.products?.product ??
    [];
  return Array.isArray(arr) ? arr : [];
}

function normalize(p) {
  // if (p.lastest_volume > 0) console.log("p:", p);

  return {
    _id: p.product_id,
    // title: p.product_title,
    // price: p.target_app_sale_price,
    // currency: p.target_app_sale_price_currency,
    // image: p.product_main_image_url,
    promotion_link: p.promotion_link,
    c1_id: p.first_level_category_id,
    c1_name: p.first_level_category_name,
    c2_id: p.second_level_category_id,
    c2_name: p.second_level_category_name,
    volume: p.lastest_volume,
  };
}

export async function fetchByCategory({ categoryId }) {
  try {
    const pageSize = 50;
    const allItems = [];
    let pageNo = 1;
    let breakNo = 0;
    let lastRaw = null;
    let totalServerCount = 0;
    let totalFilteredCount = 0;

    while (true) {
      const params = {
        app_key: APP_KEY,
        method: METHOD,
        sign_method: "sha256",
        timestamp: Date.now(), // epoch(ms)
        v: "1.0",
        // biz
        tracking_id: TRACKING_ID,
        page_no: pageNo,
        page_size: pageSize,
        target_language: "KO",
        target_currency: "KRW",
        ship_to_country: "KR",
        // country: "KR", // 필요 시만 사용

        //  SALE_PRICE_ASC → 가격 낮은 순
        // SALE_PRICE_DESC → 가격 높은 순
        // LAST_VOLUME_ASC → 최근 판매량 낮은 순
        // LAST_VOLUME_DESC → 최근 판매량 높은 순
        sort: "LAST_VOLUME_DESC",

        fields: FIELDS,
        // 카테고리: 서버가 먹는 키를 모두 전달
        category_ids: String(categoryId),
        // category_id: String(categoryId),
        // keywords: "", // 섞임 방지로 비움
      };
      params.sign = signSha256(params, APP_SECRET);

      const url = API + "?" + new URLSearchParams(params).toString();
      // const res = await fetch(url);
      // const raw = await res.json().catch(() => ({}));
      const raw = await fetchJsonWithRetry(url);

      lastRaw = raw;

      // 에러 그대로 전달하되, 형태는 아래 호출부와 호환되게 유지
      if (raw?.error_response) {
        return {
          items: [],
          raw,
          serverCount: 0,
          filteredCount: 0,
          note: "error_response",
        };
      }

      // 서버 반환
      const products = parseProducts(raw);
      const filtered = products.filter(
        (p) =>
          Number(p.first_level_category_id) === Number(categoryId) ||
          Number(p.second_level_category_id) === Number(categoryId),
      );

      const final = (filtered.length ? filtered : products).map(normalize);

      totalServerCount += products.length;
      totalFilteredCount += filtered.length;

      // 현 페이지 결과 누적
      if (final.length > 0) {
        allItems.push(...final);
      }

      // 종료 조건:
      // - 서버가 더 이상 주지 않음 (0개)
      // - 페이지 크기 미만(마지막 페이지로 추정)
      if (products.length === 0 && products.length < pageSize) {
        if (breakNo === 2) {
          break;
        }
        breakNo++;
      } else {
        breakNo = 0;
        pageNo++;
      }
    }

    return {
      items: allItems,
      raw: lastRaw, // 마지막 페이지 raw
      serverCount: totalServerCount,
      filteredCount: totalFilteredCount,
    };
  } catch (e) {
    console.log("e.message:", e);
  }
}
