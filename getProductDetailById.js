// filename: fetchByProductId.js
// Node 18+ (fetch 내장), package.json: { "type": "module" }
// 준비: npm i dotenv  &&  .env에 AE_APP_KEY, AE_APP_SECRET, AE_TRACKING_ID 설정

import crypto from "crypto";
import { fileURLToPath } from "url";
import "dotenv/config";

const APP_KEY = process.env.AE_APP_KEY;
const APP_SECRET = process.env.AE_APP_SECRET;
const TRACKING_ID = process.env.AE_TRACKING_ID;

if (!APP_KEY || !APP_SECRET || !TRACKING_ID) {
  console.error(
    "환경변수(AE_APP_KEY, AE_APP_SECRET, AE_TRACKING_ID)를 확인하세요."
  );
  process.exit(1);
}

// ─────────────────────────────────────────────────────────────
// SG 엔드포인트 + Affiliate 상품 상세 조회 메서드
// ─────────────────────────────────────────────────────────────
const API_SYNC = "https://api-sg.aliexpress.com/sync";
const METHOD_DETAIL = "aliexpress.affiliate.productdetail.get";

// AliExpress(신 플랫폼) 서명: 파라미터 키로 정렬 → key+value 연결 → HMAC-SHA256 → HEX 대문자
function signParamsSha256(allParams, appSecret) {
  const sortedKeys = Object.keys(allParams).sort();
  const concat = sortedKeys.map((k) => `${k}${allParams[k]}`).join("");
  return crypto
    .createHmac("sha256", appSecret)
    .update(concat)
    .digest("hex")
    .toUpperCase();
}

async function callAliExpress(method, bizParams) {
  const sys = {
    app_key: APP_KEY,
    method,
    sign_method: "sha256",
    timestamp: Date.now().toString(), // SG 엔드포인트는 epoch(ms)
    v: "2.0",
    format: "json",
  };

  const all = { ...sys, ...bizParams };
  const sign = signParamsSha256(all, APP_SECRET);
  const qs = new URLSearchParams({ ...all, sign });

  const url = `${API_SYNC}?${qs.toString()}`;
  const res = await fetch(url, { method: "GET" });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(
      `HTTP ${res.status} ${res.statusText} :: ${text.slice(0, 500)}`
    );
  }

  let json;
  try {
    json = await res.json();
  } catch (e) {
    const text = await res.text().catch(() => "");
    throw new Error(`JSON 파싱 실패 :: ${text.slice(0, 500)}`);
  }
  return json;
}

// 결과를 안전하게 정규화
function normalizeDetailResponse(json) {
  const resp =
    json?.aliexpress_affiliate_productdetail_get_response ??
    json?.aliexpress_affiliate_productdetail_get_response;
  const result = resp?.resp_result?.result ?? resp?.result ?? resp;
  const list =
    result?.products?.product ?? result?.products ?? result?.product ?? [];

  return list.map((p) => ({
    product_id: p.product_id ?? p.productId,
    title: p.product_title ?? p.productTitle,
    detail_url: p.product_detail_url ?? p.productDetailUrl,
    main_image: p.product_main_image_url ?? p.productMainImageUrl,
    sale_price: p.sale_price ?? p.app_sale_price ?? p.final_sale_price,
    original_price: p.original_price ?? p.originalPrice,
    discount: p.discount,
    evaluate_rate: p.evaluate_rate ?? p.evaluateRate,
    volume: p.volume ?? p.lastest_volume,
    commission_rate: p.commission_rate ?? p.commissionRate,
    _raw: p,
  }));
}

/**
 * 상품 ID로 디테일 조회
 * @param {string|string[]} productIds - 단일 ID 또는 배열(배열이면 콤마 join)
 * @param {object} options - 조회 옵션
 */
export async function getProductDetailsById(
  productIds,
  {
    country = "KR",
    target_language = "KO",
    target_currency = "KRW",
    tracking_id = TRACKING_ID,
    fields = "commission_rate,sale_price,original_price,product_title,product_main_image_url,product_detail_url,evaluate_rate,volume",
    app_signature,
  } = {}
) {
  const ids = Array.isArray(productIds)
    ? productIds.join(",")
    : String(productIds);

  const bizParams = {
    product_ids: ids,
    country,
    target_language,
    target_currency,
    tracking_id,
    fields,
  };
  if (app_signature) bizParams.app_signature = app_signature;

  const raw = await callAliExpress(METHOD_DETAIL, bizParams);
  const items = normalizeDetailResponse(raw);

  return { raw, items };
}

// ─────────────────────────────────────────────────────────────
// 직접 실행 감지 (Windows 안전)
// ─────────────────────────────────────────────────────────────
const isDirect = process.argv[1] === fileURLToPath(import.meta.url);

if (isDirect) {
  (async () => {
    // CLI 인자에서 상품ID 받기: node fetchByProductId.js 1005007856343236 4000669887458
    // const cliIds = process.argv.slice(2);
    const ids = ["1005008290630438"]; // 없으면 예시 값

    const product = await getProductDetailsById(ids, {
      country: "KR",
      target_language: "KO",
      target_currency: "KRW",
    });

    // console.dir(product.slice(0, 2), { depth: 2, maxArrayLength: 20 });
    // const r = product?.aliexpress_affiliate_productdetail_get_response;
    // console.log("resp_code / resp_msg:", r?.resp_code, "/", product?.resp_msg);
  })().catch((e) => {
    console.error("[에러]", e?.message ?? e);
    process.exit(1);
  });
}
