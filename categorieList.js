// filename: fetchPopularKR.fixed.js
// Node 18+, package.json: { "type": "module" }
import crypto from "crypto";
import "dotenv/config";
import pLimit from "p-limit";
import { getSkuDetail } from "./skuIdPruductSearch.js";
import ProductDetail from "./models/ProductDetail.js";
import categorieList from "./categorieList.json" assert { type: "json" };
import dbConnect from "./utils/dbConnect.js";
import { dateKeyKST } from "./utils/dateKeyKST.js";
import mongoose from "mongoose";
import { assert } from "console";
import { writeFile } from "node:fs/promises";
const API = "https://api-sg.aliexpress.com/sync";
const METHOD = "aliexpress.affiliate.product.query";

const APP_KEY = process.env.AE_APP_KEY;
const APP_SECRET = process.env.AE_APP_SECRET;
const TRACKING_ID = process.env.AE_TRACKING_ID;

const parseSkuProps = (val) => {
  if (!val) return [];
  if (Array.isArray(val)) return val;
  if (typeof val === "string") {
    try {
      const arr = JSON.parse(val);
      return Array.isArray(arr) ? arr : [];
    } catch {
      return [];
    }
  }
  return [];
};

const isEmptyProps = (arr) =>
  !arr ||
  arr.length === 0 ||
  (arr.length === 1 && Object.keys(arr[0] || {}).length === 0);

const canonSkuProps = (arr) => {
  const a = parseSkuProps(arr);
  if (isEmptyProps(a)) return "∅";
  const canonArr = a.map((obj) => {
    const entries = Object.entries(obj).map(([k, v]) => [
      norm(k),
      norm(String(v)),
    ]);
    entries.sort(([k1], [k2]) => (k1 > k2 ? 1 : k1 < k2 ? -1 : 0));
    return Object.fromEntries(entries);
  });
  return JSON.stringify(canonArr);
};

const norm = (v) =>
  (v ?? "") // null/undefined 방어
    .toString() // 문자열화
    .replace(/[\s\u200B-\u200D\uFEFF]/g, ""); // 일반 공백 + 제로폭 공백 제거

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

// ───────────────────────── 재시도 유틸 ─────────────────────────
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function calcDelay({ base, factor, attempt, jitter, max }) {
  const backoff = Math.min(base * Math.pow(factor, attempt), max);
  const rand = 1 + (Math.random() * 2 - 1) * jitter; // 1±jitter
  return Math.round(backoff * rand);
}

/**
 * fetch → JSON 파싱까지 포함한 재시도 래퍼
 * - 429/5xx/타임아웃/네트워크 오류(ECONNRESET 등) 시 지수백오프(+지터)로 재시도
 */
async function fetchJsonWithRetry(
  url,
  {
    retries = 4, // 총 5회(0..4)
    base = 600, // 시작 지연(ms)
    factor = 2,
    jitter = 0.35,
    max = 10000,
    timeoutMs = 18000,
    fetchInit = {},
  } = {}
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

/**
 * 임의 함수 재시도(예: getSkuDetail)
 */
async function withRetry(fn, opts = {}) {
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

function signSha256(params, secret) {
  const base = Object.keys(params)
    .filter(
      (k) => params[k] !== undefined && params[k] !== null && k !== "sign"
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
  return {
    _id: p.product_id,
    title: p.product_title,
    price: p.target_app_sale_price,
    currency: p.target_app_sale_price_currency,
    image: p.product_main_image_url,
    promotion_link: p.promotion_link,
    c1_id: p.first_level_category_id,
    c1_name: p.first_level_category_name,
    c2_id: p.second_level_category_id,
    c2_name: p.second_level_category_name,
    volume: p.lastest_volume,
    reviews: p.review_count,
  };
}

async function fetchByCategory({ categoryId }) {
  const pageSize = 50;
  const allItems = [];
  let pageNo = 1;
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
      target_language: "ko",
      target_currency: "KRW",
      ship_to_country: "KR",
      // country: "KR", // 필요 시만 사용
      sort: "LAST_VOLUME_DESC",
      fields: FIELDS,
      // 카테고리: 서버가 먹는 키를 모두 전달
      category_ids: String(categoryId),
      category_id: String(categoryId),
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
        Number(p.second_level_category_id) === Number(categoryId)
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
      break;
    }

    pageNo++;
  }
  return {
    items: allItems,
    raw: lastRaw, // 마지막 페이지 raw
    serverCount: totalServerCount,
    filteredCount: totalFilteredCount,
  };
}

(async () => {
  const limit = pLimit(10); // 동시에 7개만 실행

  await dbConnect();

  const listTasks = categorieList.items.map((item) =>
    limit(async () => {
      const { items, raw, serverCount, filteredCount, note } =
        await fetchByCategory({
          categoryId: item.category_id,
        });

      // 기존 DB에 동일 카테고리 상품들 조회 (짧은 저장 키 사용)
      // let res;
      // if (!item.parent_category_id) {
      //   res = await ProductDetail.find({ ci1: item.category_id });
      // } else {
      //   res = await ProductDetail.find({ ci2: item.category_id });
      // }

      if (items.length) {
        console.log(items.slice(0, 5));
      } else {
        console.log(raw?.error_response ?? raw);
      }

      return items;
    })
  );

  // 모든 태스크 실행
  const productIdList = (await Promise.all(listTasks)).flat();
  const uniqueList = [
    ...new Map(
      productIdList
        .filter((item) => item.volume >= 50) // 🔹 volume 조건(외부 데이터 키가 volume이면 유지)
        .map((item) => {
          console.log("item._id:", item._id);
          return [item._id, item];
        })
    ).values(),
  ];
  const map = new Map();

  console.log("uniqueList:", uniqueList);

  for (const item of uniqueList ?? []) {
    if (item?.c1_id != null) {
      map.set(String(item.c1_id), {
        cId: String(item.c1_id),
        cn: item.c1_name ?? String(item.c1_id), // 이름 없으면 id로 대체
      });
    }
    if (item?.c2_id != null) {
      map.set(String(item.c2_id), {
        cId: String(item.c2_id),
        cn: item.c2_name ?? String(item.c2_id),
      });
    }
  }

  const data = Array.from(map.values());

  console.log("data:", data);

  const failedIds = [];

  console.log("실패한 상품 IDs:", failedIds);

  await writeFile(
    "categorieList_kr_all_v1.json",
    JSON.stringify(data, null, 2),
    "utf8"
  );

  console.log("data:", data.length);

  process.exit(0);
})();
