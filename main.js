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
// import ProductCategories from "./models/ProductCategories.js";
import ProductCategories from "./models/ProductCategories.js";
import { getProductDetailsById } from "./getProductDetailById.js";
const API = "https://api-sg.aliexpress.com/sync";
const METHOD = "aliexpress.affiliate.product.query";

const APP_KEY = process.env.AE_APP_KEY;
const APP_SECRET = process.env.AE_APP_SECRET;
const TRACKING_ID = process.env.AE_TRACKING_ID;

const USE_SYNONYM_MAP = true;
const SYNONYM_KEY_MAP = { 색깔: "색상" };

// ─────────────────────────────────────────────────────────────────────────────
// 특수문자 이스케이프 + 문자 사이사이에 \s* 허용
//  실패 무해 try/catch, 배열 정규화
const tryCatch = async (fn) => {
  try {
    return { ok: true, value: await fn() };
  } catch (e) {
    return { ok: false, error: e };
  }
};

const escapeRegExp = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const makeSpaceAgnosticPattern = (raw) => {
  const canon = String(raw ?? "")
    .normalize("NFKC")
    .replace(/\s+/g, ""); // 모든 공백 제거
  return canon.length
    ? canon.split("").map(escapeRegExp).join("\\s*") // 문자 사이에 임의 공백 허용
    : ".*";
};

// 비교용 정규화: 지정 특수문자 + 공백 제거
function stripForCompare(s) {
  return String(s ?? "").replace(/[{}\[\]\(\)\"\s]/g, "");
}

// c 필드 비교용 정규화
function normalizeCForCompare(c) {
  return stripForCompare(c);
}

// sp 비교용 정규화
function normalizeSpForCompare(spStr) {
  if (typeof spStr !== "string") return stripForCompare(spStr);
  // 1) JSON 파싱 시도
  try {
    let arr = JSON.parse(spStr);
    if (!Array.isArray(arr)) arr = [arr];
    // 2) 동의어 키 매핑 (선택)
    const mapped = arr.map((obj) => {
      const out = {};
      for (const [k, v] of Object.entries(obj || {})) {
        const nk = USE_SYNONYM_MAP ? SYNONYM_KEY_MAP[k] || k : k;
        out[nk] = v;
      }
      // 키 정렬로 직렬화 안정화
      return Object.fromEntries(
        Object.entries(out).sort(([a], [b]) => (a > b ? 1 : -1))
      );
    });
    // 3) 안정적 직렬화 후 strip
    const stable = JSON.stringify(mapped);
    return stripForCompare(stable);
  } catch {
    // 파싱 불가 → 그냥 strip 규칙만 적용
    return stripForCompare(spStr);
  }
}

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

// 키 동의어: '색깔' → '색상'
const KEY_SYNONYM = Object.freeze({
  색깔: "색상",
});

const canonSkuProps = (arr) => {
  const a = parseSkuProps(arr);
  if (isEmptyProps(a)) return "";

  const canonArr = a.map((obj) => {
    // 1) 키/값 정규화 + 동의어 치환
    const pairs = [];
    for (const [k, v] of Object.entries(obj || {})) {
      const kNorm = norm(k);
      // 원본 키와 정규화된 키 모두에 대해 치환 시도
      const mapped = KEY_SYNONYM[k] ?? KEY_SYNONYM[kNorm] ?? kNorm;

      const vNorm = norm(String(v));
      pairs.push([mapped, vNorm]);
    }

    // 2) 키 정렬(직렬화 안정화)
    pairs.sort(([k1], [k2]) => (k1 > k2 ? 1 : k1 < k2 ? -1 : 0));

    // 3) 동의어 치환으로 생긴 중복 키 병합(첫 값 우선)
    const merged = {};
    for (const [k, v] of pairs) {
      if (!(k in merged)) merged[k] = v;
    }

    return merged;
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
      target_language: "KO",
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

  // 정확히 10등분하기

  const productCategories = await ProductCategories.find();
  const total = productCategories.length;
  const baseSize = Math.floor(total / 10); // 기본 크기
  let remainder = total % 10; // 남는 개수

  const divided = [];
  let start = 0;

  for (let i = 0; i < 10; i++) {
    // 나머지가 남아있으면 이 그룹은 +1개 더 받음
    const extra = remainder > 0 ? 1 : 0;
    const end = start + baseSize + extra;

    divided.push(productCategories.slice(start, end));
    start = end;
    remainder--;
  }

  //

  const listTasks = { item: [], dataBaseRes: [] };

  const categoryRes = divided[1].map((item) =>
    limit(async () => {
      const cat = await ProductCategories.findOne({
        cId: String(item.cId),
      });

      if (!cat?._id) {
        console.log("카테고리 없음:", item.cId);
        return;
      }

      let res = await ProductDetail.find({ cId1: cat._id })
        .populate("cId1", "cId cn")
        .populate("cId2", "cId cn")
        .lean({ virtuals: true });

      if (!res?.length) {
        res = await ProductDetail.find({ cId2: cat._id })
          .populate("cId1", "cId cn")
          .populate("cId2", "cId cn")
          .lean({ virtuals: true });
      }

      // const res = await ProductDetail.find({
      //   $or: [{ cId1: cat._id }, { cId2: cat._id }],
      // })
      //   .populate("cId1", "cId cn")
      //   .populate("cId2", "cId cn")
      //   .lean({ virtuals: true });

      const { items, raw, serverCount, filteredCount, note } =
        await fetchByCategory({
          categoryId: item.cId,
        });

      // 기존 DB에 동일 카테고리 상품들 조회 (짧은 저장 키 사용)
      // if (!item.parent_category_id) {
      //   res = await ProductDetail.find({ cId1: item.category_id });
      // } else {
      //   res = await ProductDetail.find({ ci2: item.category_id });
      // }

      // console.log("items:", items);

      // if (items.length) {
      //   console.log(items.slice(0, 5));
      // } else {
      //   console.log(raw?.error_response ?? raw);
      // }

      listTasks.item.push(...items);
      listTasks.dataBaseRes.push(...res);
    })
  );

  // 데이터베이스에 있는건 volume 200 안넘어도 업데이트

  await Promise.allSettled(categoryRes);
  // const ProductIdList = listTasks;
  console.log("dataBaseRes", listTasks.dataBaseRes.length);
  console.log("item", listTasks.item.length);

  const items = (listTasks.item ?? []).filter((p) => Number(p?.volume) >= 200);
  const dbs = listTasks.dataBaseRes ?? [];

  const merged = [
    ...dbs, // DB 먼저
    ...items, // item 뒤 (우선권)
  ];

  console.log("items:", items.length);
  console.log("dbs:", dbs.length);
  console.log("merged:", merged.length);

  // ------------ 중복검사 --------------

  const seen = new Set();
  const uniqueList = [];
  for (const p of merged) {
    const id = String(p._id);
    if (seen.has(id)) continue;
    seen.add(id);
    uniqueList.push(p);
  }

  // // ----------------중복검사---------------

  console.log("uniqueList:", uniqueList.length);

  // ------------------------------

  const failedIds = [];

  await Promise.all(
    uniqueList.map((item) =>
      limit(async () => {
        try {
          // 0) 외부 API
          const productIds = [item._id];
          let volume = undefined;

          const skuData = await withRetry(() => getSkuDetail(item._id), {
            retries: 3,
            base: 800,
            max: 10000,
          });
          const pdRes = await tryCatch(() =>
            withRetry(() => getProductDetailsById(productIds), {
              retries: 3,
              base: 800,
              max: 10000,
            })
          );

          const productData = pdRes.ok ? pdRes.value : null;

          if (productData?.items[0]?.volume) {
            volume = productData.items[0].volume;
          }
          const info = skuData?.ae_item_info ?? {};
          const sku = skuData?.ae_item_sku_info ?? {};
          const skuList = sku.traffic_sku_info_list ?? [];

          // ---- 카테고리 참조 매핑 (두 개 한번에) ----

          const cId1 = await ProductCategories.findOne({
            cId: String(info?.display_category_id_l1),
          });
          const cId2 = await ProductCategories.findOne({
            cId: String(info?.display_category_id_l2),
          });
          // console.log("cId1:", cId1);

          // 1) 파생값
          const productId = String(item._id); // ← 스키마가 String이므로 문자열 고정
          const todayKey = dateKeyKST(); // "YYYY-MM-DD" (KST)

          // 2) 본문(upsert) 베이스

          const baseDoc = {};

          // console.log("item:", item);

          if (Number(volume) > 0) {
            baseDoc.volume = volume;
          } else if (item.volume && Number(item.volume) !== 0) {
            baseDoc.volume = item.volume;
          }

          if (
            info.original_link &&
            stripForCompare(info.original_link) !== ""
          ) {
            baseDoc.original_link = info.original_link;
          }

          if (
            item.promotion_link &&
            stripForCompare(item.promotion_link) !== ""
          ) {
            baseDoc.promotion_link = item.promotion_link;
          }
          if (cId1) {
            baseDoc.cId1 = cId1;
          }
          if (cId2) {
            baseDoc.cId2 = cId2;
          }
          if (info.title && stripForCompare(info.title) !== "") {
            baseDoc.tt = info.title;
          }
          if (info.store_name && stripForCompare(info.store_name) !== "") {
            baseDoc.st = info.store_name;
          }
          if (info.product_score && Number(info.product_score) !== 0) {
            baseDoc.ps = info.product_score;
          }
          if (info.review_number && Number(info.review_number) !== 0) {
            baseDoc.rn = info.review_number;
          }
          if (info.image_link && stripForCompare(info.image_link) !== "") {
            baseDoc.il = info.image_link;
          }
          if (
            info.additional_image_links?.string &&
            info.additional_image_links?.string.length >= 1
          ) {
            baseDoc.ail = info.additional_image_links?.string;
          }

          // const baseDoc = {
          //   vol: item.volume ?? 0,
          //   ol: info.original_link ?? "",
          //   pl: item.promotion_link ?? "",

          //   // ref 필드에는 반드시 _id(ObjectId)만
          //   cId1: cId1, // 없으면 undefined → $set에서 무시됨
          //   cId2: cId2,

          //   tt: info.title ?? "",
          //   st: info.store_name ?? "",
          //   ps: info.product_score ?? 0,
          //   rn: info.review_number ?? 0,
          //   il: info.image_link ?? "",
          //   ail: info.additional_image_links?.string ?? [],
          // };

          // 3) 최초 생성 시에만 넣을 SKU 전체(오늘 포인트 포함) — 임베디드 구조
          const skusForInsert = skuList.map((s) => {
            return {
              sId: String(s.sku_id), // 문자열로 통일
              c: norm(s.color ?? ""), // 정규화 통일
              link: s.link ?? "",
              sp: normalizeSpForCompare(s.sku_properties ?? ""), // 정규화 통일
              cur: s.currency ?? "KRW",
              pd: {
                [todayKey]: {
                  p: Number(s.price_with_tax),
                  s: Number(s.sale_price_with_tax ?? 1),
                  t: new Date(),
                },
              },
            };
          });

          // 4) 기존 문서의 sku_id 집합만 얇게 조회 — 경로 "sku_info.sil"
          const doc = await ProductDetail.findById(productId)
            .select(
              "sku_info.sil.sId sku_info.sil.c sku_info.sil.sp sku_info.sil.pd"
            )
            .lean();

          const toNum = (v) => (v == null ? NaN : +v);
          const safeNorm = (v) => norm(v ?? "");
          const toKey = (sid, color, props) =>
            `${String(sid)}\u0001${safeNorm(
              color
            )}\u0001${normalizeSpForCompare(props)}`;

          // 필요한 필드만

          const sil = doc?.sku_info?.sil ?? [];
          const existingIds = new Set(
            (doc?.sku_info?.sil ?? []).map((d) => String(d?.sId))
          );
          const skuMap = new Map();
          for (const sku of sil) {
            const k = toKey(sku?.sId, sku?.c, sku?.sp);
            skuMap.set(k, sku);
          }

          const newSkus = [];
          const updSkus = [];
          const lowPriceUpdSkus = [];

          for (const item of skuList) {
            const sid = String(item?.sku_id);
            if (sid == null) continue;

            if (!existingIds.has(sid)) {
              newSkus.push(item);
              continue;
            }

            const key = toKey(sid, item?.color, item?.sku_properties);

            const exist = skuMap.get(key);

            if (!exist) {
              newSkus.push(item);
              continue;
            }
            // 문제 지점 전후로 세분화 try-catch
            let incomingSale;
            try {
              incomingSale = toNum(item?.sale_price_with_tax ?? null);
              // incomingSale = toNum(1 ?? null);
            } catch (e) {
              throw e;
            }
            let docToday, docSale;
            try {
              docToday = exist?.pd?.[todayKey];
              docSale = toNum(docToday?.s);
            } catch (e) {
              throw e;
            }

            if (docToday) {
              if (docSale > incomingSale) {
                lowPriceUpdSkus.push(item);
              }
            } else {
              updSkus.push(item);
            }
          }

          // 5) bulkWrite 준비
          const ops = [];

          // 5-1) 본문 upsert
          ops.push({
            updateOne: {
              filter: { _id: productId },
              update: {
                $set: baseDoc,
                $setOnInsert: {
                  // _id는 filter에서 고정
                  "sku_info.sil": skusForInsert,
                },
              },
              upsert: true,
            },
          });

          const colorNorm = (v) => norm(v ?? "");

          // 5-2) 금일 첫 sku 업데이트 (오늘 키가 없던 케이스)
          for (const s of updSkus) {
            const sId = String(s.sku_id);
            const cNorm = colorNorm(s.color);
            const spCanon = canonSkuProps(s.sku_properties);

            const spRegex = makeSpaceAgnosticPattern(spCanon);
            const cRegex = makeSpaceAgnosticPattern(cNorm);

            console.log("금일 첫 업데이트!");

            const pricePoint = {
              p: Number(s.price_with_tax),
              s: Number(s.sale_price_with_tax),
              t: new Date(),
            };

            ops.push({
              updateOne: {
                filter: { _id: productId },
                update: {
                  $set: {
                    "sku_info.sil.$[e].sId": sId,
                    "sku_info.sil.$[e].c": cNorm,
                    "sku_info.sil.$[e].link": s.link ?? "",
                    "sku_info.sil.$[e].sp": spCanon,
                    "sku_info.sil.$[e].cur": s.currency ?? "KRW",
                    [`sku_info.sil.$[e].pd.${todayKey}`]: pricePoint,
                  },
                },
                arrayFilters: [
                  {
                    "e.sId": sId,
                    $and: [
                      {
                        $or: [
                          { "e.c": cNorm },
                          { "e.c": { $regex: cRegex, $options: "i" } },
                        ],
                      },
                      {
                        $or: [
                          { "e.sp": spCanon },
                          { "e.sp": { $regex: spRegex, $options: "i" } },
                        ],
                      },
                    ],
                  },
                ],
              },
            });
          }

          // 5-3) 오늘 최저가 갱신 (문서의 오늘가 > 신규가)
          for (const s of lowPriceUpdSkus) {
            const sId = String(s.sku_id);
            const cNorm = colorNorm(s.color);
            const spCanon = canonSkuProps(s.sku_properties);

            const spRegex = makeSpaceAgnosticPattern(spCanon);
            const cRegex = makeSpaceAgnosticPattern(cNorm);

            console.log("당일 최저가:!!");

            const pricePoint = {
              p: Number(s.price_with_tax),
              s: Number(s.sale_price_with_tax),
              t: new Date(),
            };

            ops.push({
              updateOne: {
                filter: { _id: productId },
                update: {
                  $set: {
                    "sku_info.sil.$[e].sId": sId,
                    "sku_info.sil.$[e].c": cNorm,
                    "sku_info.sil.$[e].link": s.link ?? "",
                    "sku_info.sil.$[e].sp": spCanon,
                    "sku_info.sil.$[e].cur": s.currency ?? "KRW",
                    [`sku_info.sil.$[e].pd.${todayKey}`]: pricePoint,
                  },
                },
                arrayFilters: [
                  {
                    "e.sId": sId,
                    $and: [
                      {
                        $or: [
                          { "e.c": cNorm },
                          { "e.c": { $regex: cRegex, $options: "i" } },
                        ],
                      },
                      {
                        $or: [
                          { "e.sp": spCanon },
                          { "e.sp": { $regex: spRegex, $options: "i" } },
                        ],
                      },
                    ],
                  },
                ],
              },
            });
          }

          // 5-4) 새로 발견된 sku들을 push
          if (newSkus.length > 0 && doc) {
            const toPush = newSkus.map((s) => ({
              sId: String(s?.sku_id),
              c: s?.color ?? "",
              link: s.link,
              sp: s.sku_properties ?? "",
              cur: s.currency ?? "KRW",
              pd: {
                [todayKey]: {
                  p: s.price_with_tax,
                  s: s.sale_price_with_tax,
                  t: new Date(),
                },
              },
            }));

            ops.push({
              updateOne: {
                filter: { _id: productId }, // ✅ 저장 키 사용
                update: {
                  $push: { "sku_info.sil": { $each: toPush } },
                },
              },
            });
          }

          // 6) 일괄 실행
          if (ops.length) {
            await ProductDetail.bulkWrite(ops, {
              ordered: false,
              writeConcern: { w: 1 },
            });
          }
        } catch (err) {
          const pid =
            (err &&
              typeof err === "object" &&
              "productId" in err &&
              err.productId) ||
            item._id;
          failedIds.push(pid);
          console.warn("getSkuDetail 실패", {
            productId: pid,
            code: err?.code,
            sub_code: err?.sub_code,
            message: err?.message,
          });
        }
      })
    )
  );

  console.log("실패한 상품 IDs:", failedIds);

  process.exit(0);
})();
