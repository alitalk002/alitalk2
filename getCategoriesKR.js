// filename: fetchPopularKR.js
// Node 18+ (fetch 내장), package.json: { "type": "module" }
// 준비: npm i dotenv
// .env: AE_APP_KEY=..., AE_APP_SECRET=..., AE_TRACKING_ID=...

import crypto from "crypto";
import "dotenv/config";
const { writeFile } = await import("fs/promises");

const APP_KEY = process.env.AE_APP_KEY;
const APP_SECRET = process.env.AE_APP_SECRET;
const TRACKING_ID = process.env.AE_TRACKING_ID;

if (!APP_KEY || !APP_SECRET || !TRACKING_ID) {
  console.error(
    "환경변수(AE_APP_KEY, AE_APP_SECRET, AE_TRACKING_ID)를 확인하세요."
  );
  process.exit(1);
}

// ===== 상수 =====
const METHOD = "aliexpress.affiliate.category.get";
// const METHOD = "aliexpress.affiliate.product.query";
const API_REST = "https://api-sg.aliexpress.com/rest";
const API_SYNC = "https://api-sg.aliexpress.com/sync";
const SORT_POPULAR = "LAST_VOLUME_DESC"; // 최근 판매량 내림차순

// ── 카테고리 필드까지 포함(서버 필터 확인용) ──
const FIELDS = [
  "product_id",
  "product_title",
  "product_detail_url",
  "product_main_image_url",
  "app_sale_price",
  "app_sale_price_currency",
  "sale_price",
  "sale_price_currency",
  "target_app_sale_price",
  "target_app_sale_price_currency",
  "evaluate_rate",
  "promotion_link",
  "lastest_volume",
  "review_count",
  "total_review_num",
  "evaluate_count",
  "first_level_category_id",
  "first_level_category_name",
  "second_level_category_id",
  "second_level_category_name",
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
function signMD5(params, secret) {
  const base = buildBase(params);
  return crypto
    .createHash("md5")
    .update(secret + base + secret, "utf8")
    .digest("hex")
    .toUpperCase();
}
function signHMAC256(params, secret) {
  const base = buildBase(params);
  return crypto
    .createHmac("sha256", secret)
    .update(base, "utf8")
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

// ===== 파싱 & 필터 유틸 =====
const toInt = (v) => {
  if (v === undefined || v === null) return 0;
  const n = Number(String(v).replace(/[^\d]/g, ""));
  return Number.isFinite(n) ? n : 0;
};

function parseItems(data) {
  const candidates =
    [
      data?.resp_result?.result?.products,
      data?.resp_result?.result?.items,
      data?.data?.products,
      data?.result?.products,
      data?.aliexpress_affiliate_product_query_response?.result?.products,
    ].find(Boolean) || [];

  return (Array.isArray(candidates) ? candidates : []).map((p) => {
    const soldRaw = p?.lastest_volume ?? p?.sale_num ?? p?.volume ?? p?.sales;
    const reviewCount =
      p?.review_count ?? p?.total_review_num ?? p?.evaluate_count ?? undefined;

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
      reviewCount,
      image:
        p?.product_main_image_url ||
        p?.image_url ||
        p?.main_image ||
        p?.imageUrl,
      url:
        p?.promotion_link || p?.product_detail_url || p?.product_url || p?.url,
      // 카테고리 확인용
      first_level_category_id: p?.first_level_category_id,
      first_level_category_name: p?.first_level_category_name,
      second_level_category_id: p?.second_level_category_id,
      second_level_category_name: p?.second_level_category_name,
    };
  });
}

function matchesCategory(it, wantedId) {
  if (!wantedId) return true;
  const c1 = Number(it.first_level_category_id || 0);
  const c2 = Number(it.second_level_category_id || 0);
  return c1 === Number(wantedId) || c2 === Number(wantedId);
}

// ===== 단일 요청 =====
async function requestOnce({
  endpoint,
  signMethod, // 'hmac-sha256' | 'md5'
  tsMode, // 'epoch' | 'topfmt'
  keywords,
  pageNo,
  pageSize,
  categoryId, // 단일 카테고리 ID
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

  // 게이트웨이별 키 편차를 모두 포함해서 시도
  const biz = {
    tracking_id: TRACKING_ID,
    page_no: pageNo,
    page_size: pageSize,
    target_language: "ko",
    target_currency: "KRW",
    country: "KR",
    ship_to_country: "KR",
    sort: SORT_POPULAR,
    fields: FIELDS,
    ...(keywords ? { keywords } : {}),
    ...(categoryId
      ? {
          category_id: categoryId,
          category_ids: String(categoryId),
          categoryId: categoryId,
        }
      : {}),
    ...extraBiz,
  };

  const params = { ...sys, ...biz };
  const url = buildSignedURL(endpoint, params, signMethod);

  const res = await fetch(url, { method: "GET" });
  const data = await res.json().catch(() => ({}));

  if (data?.error_response || (data?.resp_code && data?.resp_code !== 200)) {
    console.log(
      "[API ERROR]",
      data?.error_response || { code: data?.resp_code, msg: data?.resp_msg }
    );
  }
  const items = parseItems(data);
  return { data, items };
}

// ===== 자동 재시도 + 카테고리/판매량 필터 =====
async function fetchPopularKR({
  keywords = "",
  pageNo = 1,
  pageSize = 100,
  minSold = 4000,
  categoryId = 100003109, // 예시: 축산 카테고리
  extraBiz = {},
} = {}) {
  // 1) HMAC + epoch + /rest
  let r = await requestOnce({
    endpoint: API_REST,
    signMethod: "hmac-sha256",
    tsMode: "epoch",
    keywords,
    pageNo,
    pageSize,
    categoryId,
    extraBiz,
  });
  if (r.items.length) {
    const filtered = r.items.filter(
      (it) => matchesCategory(it, categoryId) && it.sold >= minSold
    );
    if (filtered.length) return filtered;
  }

  // 2) MD5 + epoch + /rest
  r = await requestOnce({
    endpoint: API_REST,
    signMethod: "md5",
    tsMode: "epoch",
    keywords,
    pageNo,
    pageSize,
    categoryId,
    extraBiz,
  });
  if (r.items.length) {
    const filtered = r.items.filter(
      (it) => matchesCategory(it, categoryId) && it.sold >= minSold
    );
    if (filtered.length) return filtered;
  }

  // 3) HMAC + "YYYY-MM-DD HH:mm:ss" + /rest
  r = await requestOnce({
    endpoint: API_REST,
    signMethod: "hmac-sha256",
    tsMode: "topfmt",
    keywords,
    pageNo,
    pageSize,
    categoryId,
    extraBiz,
  });
  if (r.items.length) {
    const filtered = r.items.filter(
      (it) => matchesCategory(it, categoryId) && it.sold >= minSold
    );
    if (filtered.length) return filtered;
  }

  // 4) MD5 + "YYYY-MM-DD HH:mm:ss" + /rest
  r = await requestOnce({
    endpoint: API_REST,
    signMethod: "md5",
    tsMode: "topfmt",
    keywords,
    pageNo,
    pageSize,
    categoryId,
    extraBiz,
  });
  if (r.items.length) {
    const filtered = r.items.filter(
      (it) => matchesCategory(it, categoryId) && it.sold >= minSold
    );
    if (filtered.length) return filtered;
  }

  // 5) /sync 교차 테스트
  r = await requestOnce({
    endpoint: API_SYNC,
    signMethod: "md5",
    tsMode: "topfmt",
    keywords,
    pageNo,
    pageSize,
    categoryId,
    extraBiz,
  });
  if (r.items.length) {
    const filtered = r.items.filter(
      (it) => matchesCategory(it, categoryId) && it.sold >= minSold
    );
    if (filtered.length) return filtered;
  }

  console.log("[RAW RESPONSE FOR DEBUG]");
  console.dir(r.data, { depth: null });
  return r.data.aliexpress_affiliate_category_get_response.resp_result.result
    .categories.category;
}

// ===== 실행 예시 =====
(async () => {
  const list = await fetchPopularKR({
    keywords: "", // 카테고리 전체에서 인기
    pageNo: 1,
    pageSize: 120, // 넉넉히
    minSold: 4000, // 4000개 이상
    categoryId: 2, // 축산 카테고리 ID 예시
  });

  console.log("count:", list.length);
  console.log(list.slice(0, 5));

  const payload = Array.isArray(list)
    ? { count: list.length, items: list }
    : list ?? {};
  await writeFile(
    "categorieList.json",
    JSON.stringify(payload, null, 2),
    "utf8"
  );
})();
