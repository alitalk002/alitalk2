// filename: affiliateSkuDetail.js
// Node 18+, package.json: { "type": "module" }
import crypto from "crypto";
import "dotenv/config";

const API = "https://api-sg.aliexpress.com/sync";
const METHOD = "aliexpress.affiliate.product.sku.detail.get";

const APP_KEY = process.env.AE_APP_KEY;
const APP_SECRET = process.env.AE_APP_SECRET;
const TRACKING_ID = process.env.AE_TRACKING_ID;

// 서명: HMAC-SHA256(정렬된 key+value 연결 후 HEX 대문자)
function signParamsSha256(params, secret) {
  const sorted = Object.keys(params).sort();
  let str = "";
  for (const k of sorted) str += k + params[k];
  return crypto
    .createHmac("sha256", secret)
    .update(str)
    .digest("hex")
    .toUpperCase();
}

async function getSkuDetail(productId) {
  const params = {
    app_key: APP_KEY,
    method: METHOD,
    sign_method: "sha256",
    timestamp: Date.now(), // ms 타임스탬프
    tracking_id: TRACKING_ID,
    // ❗️여기: 단수형 키
    product_id: String(productId), // 예: "1005008351985792"
    target_currency: "KRW",
    target_language: "KO",
    // ❗️여기: ISO 2자리 국가코드
    ship_to_country: "KR",
  };

  params.sign = signParamsSha256(params, APP_SECRET);

  const url = API + "?" + new URLSearchParams(params).toString();
  const res = await fetch(url);
  const data = await res.json();
  console.log(JSON.stringify(data, null, 2));
  return data;
}

getSkuDetail("1005007141690589");
