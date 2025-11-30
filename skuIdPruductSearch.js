import crypto from "crypto";
import "dotenv/config";
import { translateSkuPropertiesSimple } from "./utils/skuTranslate.js";

const API = "https://api-sg.aliexpress.com/sync";
const METHOD = "aliexpress.affiliate.product.sku.detail.get";
const APP_KEY = process.env.AE_APP_KEY;
const APP_SECRET = process.env.AE_APP_SECRET;
const TRACKING_ID = process.env.AE_TRACKING_ID;

function signSha256(params, secret) {
  // 1) sign을 제외하고, 값이 undefined/null 아닌 것만
  const entries = Object.entries(params).filter(
    ([k, v]) => k !== "sign" && v !== undefined && v !== null
  );
  // 2) 키 오름차순 정렬
  entries.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  // 3) key+value concat
  const raw = entries.map(([k, v]) => `${k}${v}`).join("");
  // 4) HMAC-SHA256 → HEX 대문자
  return crypto
    .createHmac("sha256", secret)
    .update(raw)
    .digest("hex")
    .toUpperCase();
}

async function postAli(formObj) {
  const body = new URLSearchParams(formObj).toString();
  const res = await fetch(API, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body,
  });
  const json = await res.json().catch(async () => {
    const txt = await res.text();
    return { _non_json: txt };
  });
  return json;
}

export async function getSkuDetail(productId) {
  try {
    const params = {
      app_key: APP_KEY,
      method: METHOD,
      sign_method: "sha256",
      timestamp: Math.floor(Date.now() / 1000), // ← 초 단위
      tracking_id: TRACKING_ID,
      product_id: String(productId),
      target_currency: "KRW",
      target_language: "KO",
      ship_to_country: "KR",
      fields: "product_id",
    };

    // 서명
    const sign = signSha256(params, APP_SECRET);
    const form = { ...params, sign };

    const data = await postAli(form);

    // API 에러 우선 처리
    if (data?.error_response) {
      const err = new Error(data.error_response.msg || "API_ERROR");
      err.code = String(data.error_response.code ?? "API_ERROR");
      err.sub_code = data.error_response.sub_code;
      err.productId = productId;
      err.response = data;
      return err;
    }

    const result =
      data?.aliexpress_affiliate_product_sku_detail_get_response?.result
        ?.result;
    if (!result) {
      const e = new Error("EMPTY_RESULT");
      e.code = "EMPTY_RESULT";
      e.productId = productId;
      e.response = data;
      return e;
    }

    // const list = Array.isArray(result?.ae_item_sku_info?.traffic_sku_info_list)
    //   ? result.ae_item_sku_info.traffic_sku_info_list
    //   : [];

    // 각 item.sku_properties만 변환
    // const translatedList = list.map((item) => ({
    //   ...item,
    //   sku_properties: translateSkuPropertiesSimple(item.sku_properties),
    // }));

    // // 원래 구조에 다시 꽂기(불변 업데이트)
    // const newResult = {
    //   ...result,
    //   ae_item_sku_info: {
    //     ...result.ae_item_sku_info,
    //     traffic_sku_info_list: translatedList,
    //   },
    // };

    return result;
  } catch (e) {
    console.log("e:", e);
  }
}

// getSkuDetail(1005009943157236);
