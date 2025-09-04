// filename: getHotItemsKR.js
// Node 18+ (fetch 내장), package.json: { "type": "module" }
// 준비: npm i dotenv
// .env: AE_APP_KEY=..., AE_APP_SECRET=..., AE_TRACKING_ID=...

import crypto from "crypto";
import "dotenv/config";
import { pathToFileURL } from "url";

// ===== 환경 =====
const APP_KEY = process.env.AE_APP_KEY;
const APP_SECRET = process.env.AE_APP_SECRET;
const TRACKING_ID = process.env.AE_TRACKING_ID;
const DEBUG = process.env.DEBUG === "1" || process.env.DEBUG === "true";

if (!APP_KEY || !APP_SECRET || !TRACKING_ID) {
  console.error("[ENV] AE_APP_KEY / AE_APP_SECRET / AE_TRACKING_ID 필수");
  process.exit(1);
}

// ===== 상수 =====
const METHOD = "aliexpress.affiliate.product.query";
const API_REST = "https://api-sg.aliexpress.com/rest";
const API_SYNC = "https://api-sg.aliexpress.com/sync";

// “핫한” 정렬 후보(게이트웨이별 네이밍 편차 대응)
const SORT_PREFERENCES = ["LAST_VOLUME_DESC", "VOLUME_DESC", "SALE_NUM_DESC"];

// 필요한 필드(최근 판매량: lastest_volume)
const FIELDS = [
  "product_id",
  "product_title",
  "product_detail_url",
  "product_main_image_url",
  "target_app_sale_price",
  "target_app_sale_price_currency",
  "app_sale_price",
  "app_sale_price_currency",
  "sale_price",
  "sale_price_currency",
  "evaluate_rate",
  "promotion_link",
  "lastest_volume",
].join(",");

// ===== 서명 유틸 =====
function buildBase(params) {
  return Object.keys(params)
    .filter(
      (k) => k !== "sign" && params[k] !== undefined && params[k] !== null
    )
    .sort()
    .map((k) => `${k}${params[k]}`)
    .join("");
}
function signHMAC256(params, secret) {
  const base = buildBase(params);
  return crypto
    .createHmac("sha256", secret)
    .update(base, "utf8")
    .digest("hex")
    .toUpperCase();
}
function signMD5(params, secret) {
  const base = buildBase(params);
  return crypto
    .createHash("md5")
    .update(secret + base + secret, "utf8")
    .digest("hex")
    .toUpperCase();
}
const tsEpochMs = () => Date.now();
function tsYYYYMMDD_HHMMSS_UTC() {
  const d = new Date();
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(
    d.getUTCDate()
  )} ${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:${p(d.getUTCSeconds())}`;
}
function buildSignedURL(endpoint, params, signMethod) {
  const sign =
    signMethod === "md5"
      ? signMD5(params, APP_SECRET)
      : signHMAC256(params, APP_SECRET);
  const url = new URL(endpoint);
  Object.entries({ ...params, sign }).forEach(([k, v]) =>
    url.searchParams.append(k, String(v))
  );
  return url.toString();
}

// ===== 파싱 유틸 =====
const toInt = (v) => {
  if (v === undefined || v === null) return 0;
  const n = Number(String(v).replace(/[^\d]/g, ""));
  return Number.isFinite(n) ? n : 0;
};

// 객체/래퍼 형태를 배열로 정규화
function toArrayCandidates(c) {
  if (Array.isArray(c)) return c;
  if (!c || typeof c !== "object") return [];

  // 흔한 키들
  for (const k of [
    "products",
    "items",
    "list",
    "resultList",
    "result_list",
    "aeop_ae_product",
  ]) {
    if (Array.isArray(c[k])) return c[k];
  }
  // 값들 중 첫 배열
  const firstArray = Object.values(c).find(Array.isArray);
  if (firstArray) return firstArray;

  // 숫자 키만 가진 배열스러운 객체 { "0": {...}, "1": {...} }
  const keys = Object.keys(c);
  if (keys.length && keys.every((k) => /^\d+$/.test(k)))
    return Object.values(c);

  return [];
}

function parseItems(data) {
  const paths = [
    data?.resp_result?.result?.products,
    data?.resp_result?.result?.items,
    data?.data?.products,
    data?.result?.products,
    data?.result?.items,
    data?.aliexpress_affiliate_product_query_response?.result?.products,
    data?.aliexpress_affiliate_product_query_response?.result?.items,
    data?.aliexpress_affiliate_product_query_response?.resp_result?.result
      ?.products,
    data?.aliexpress_affiliate_product_query_response?.resp_result?.result
      ?.items,
  ];
  const raw = paths.find((v) => v != null);
  const candidates = toArrayCandidates(raw);

  if (DEBUG) {
    const type = Array.isArray(raw) ? "array" : typeof raw;
    console.log(
      "[DEBUG] parseItems rawType:",
      type,
      "len:",
      Array.isArray(raw) ? raw.length : Object.keys(raw || {}).length
    );
    if (!candidates.length && raw && typeof raw === "object") {
      console.log("[DEBUG] raw keys:", Object.keys(raw).slice(0, 20));
    }
  }

  return candidates.map((p) => {
    const soldRaw = p?.lastest_volume ?? p?.sale_num ?? p?.volume ?? p?.sales;
    return {
      id: p?.product_id || p?.item_id || p?.productId || p?.itemId,
      title: p?.product_title || p?.title,
      price:
        p?.target_app_sale_price ||
        p?.app_sale_price ||
        p?.sale_price ||
        p?.price,
      currency:
        p?.target_app_sale_price_currency ||
        p?.app_sale_price_currency ||
        p?.sale_price_currency ||
        p?.currency ||
        "KRW",
      sold: toInt(soldRaw),
      rating: p?.evaluate_rate || p?.rating,
      image:
        p?.product_main_image_url ||
        p?.image_url ||
        p?.main_image ||
        p?.imageUrl,
      url:
        p?.promotion_link || p?.product_detail_url || p?.product_url || p?.url,
    };
  });
}

// ===== 단일 요청 =====
async function requestOnce({
  endpoint,
  signMethod, // 'hmac-sha256' | 'md5'
  tsMode, // 'epoch' | 'topfmt'
  sortKey,
  pageNo,
  pageSize,
  keywords,
  extraBiz = {},
}) {
  const sys = {
    method: METHOD,
    app_key: APP_KEY,
    sign_method: signMethod,
    timestamp: tsMode === "epoch" ? tsEpochMs() : tsYYYYMMDD_HHMMSS_UTC(),
    v: "1.0",
    format: "json",
  };
  const biz = {
    tracking_id: TRACKING_ID,
    page_no: pageNo,
    page_size: pageSize,
    country: "KR",
    ship_to_country: "KR",
    target_language: "ko",
    target_currency: "KRW",
    sort: sortKey,
    fields: FIELDS,
    ...(keywords ? { keywords } : {}),
    ...extraBiz,
  };
  const params = { ...sys, ...biz };
  const url = buildSignedURL(endpoint, params, signMethod);

  if (DEBUG) {
    console.log("[DEBUG] request", {
      endpoint,
      signMethod,
      tsMode,
      sortKey,
      pageNo,
      pageSize,
      hasKeywords: !!keywords,
    });
  }

  const res = await fetch(url, { method: "GET" });
  const data = await res.json().catch(() => ({}));

  if (data?.error_response || (data?.resp_code && data?.resp_code !== 200)) {
    console.log(
      "[API ERROR]",
      data?.error_response || { code: data?.resp_code, msg: data?.resp_msg }
    );
  }
  const items = parseItems(data);

  if (DEBUG && pageNo === 1 && items.length === 0) {
    const redacted = structuredClone(data ?? {});
    if (redacted?.request_params) {
      delete redacted.request_params.sign;
      delete redacted.request_params.app_key;
    }
    console.log(
      "[DEBUG] raw sample (redacted)",
      JSON.stringify(redacted, null, 2).slice(0, 4000)
    );
  }

  return { data, items };
}

// ===== 공개 API =====
export async function getHotItemsKR({
  wantCount = 40,
  pageSize = 50,
  minSold = 0,
  keywords = "",
  extraBiz = {},
} = {}) {
  const results = [];
  const sortKeys = SORT_PREFERENCES;
  const combos = [
    { endpoint: API_REST, signMethod: "hmac-sha256", tsMode: "epoch" },
    { endpoint: API_REST, signMethod: "md5", tsMode: "epoch" },
    { endpoint: API_REST, signMethod: "hmac-sha256", tsMode: "topfmt" },
    { endpoint: API_REST, signMethod: "md5", tsMode: "topfmt" },
    { endpoint: API_SYNC, signMethod: "md5", tsMode: "topfmt" }, // 백업
  ];

  for (const sortKey of sortKeys) {
    let page = 1;
    while (results.length < wantCount && page <= 20) {
      let pageGot = false;

      for (const c of combos) {
        const { items } = await requestOnce({
          endpoint: c.endpoint,
          signMethod: c.signMethod,
          tsMode: c.tsMode,
          sortKey,
          pageNo: page,
          pageSize,
          keywords,
          extraBiz,
        });

        if (items.length) {
          results.push(
            ...(minSold ? items.filter((it) => it.sold >= minSold) : items)
          );
          pageGot = true;
          break;
        }
      }

      if (!pageGot) break; // 이 페이지 조합 실패 → 다음 정렬키
      page++;
    }

    if (results.length >= wantCount) break;
  }

  return results.slice(0, wantCount);
}

// ===== 메인 실행 감지(윈도우/리눅스 공통) =====
const isMain =
  process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;

process.on("unhandledRejection", (e) =>
  console.error("[UNHANDLED REJECTION]", e)
);
process.on("uncaughtException", (e) =>
  console.error("[UNCAUGHT EXCEPTION]", e)
);

if (isMain) {
  (async () => {
    console.log("[RUN] getHotItemsKR as main");
    const items = await getHotItemsKR({
      wantCount: 40,
      pageSize: 50,
      minSold: 0, // 우선 0으로 데이터 유입 확인 → 이후 상향
      keywords: "", // 전체 인기
      // extraBiz: { category_ids: "100003109" }, // 필요 시 카테고리 제한
    });

    console.log("count:", items.length);
    console.log(items);
  })();
}
