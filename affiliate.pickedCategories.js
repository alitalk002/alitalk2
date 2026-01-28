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
import {
  translateSkuPropertiesSimple,
  VALUE_MAP,
} from "./utils/skuTranslate.js";
import {
  normalizeCForCompare,
  normalizeSpForCompare,
  stripForCompare,
} from "./utils/normalize.js";
import { withRetry } from "./utils/withRetry.js";
import { fetchByCategory } from "./utils/fetchByCategory.js";

const SYNONYM_KEY_MAP = { 색깔: "색상" };

const limit = pLimit(10); // 동시에 10개만 실행

// ─────────────────────────────────────────────────────────────────────────────
//  실패 무해 try/catch, 배열 정규화

const PL_BASE1 = "https://s.click.aliexpress.com/s/";
const PL_BASE2 = "http://s.click.aliexpress.com/s/";
const IMAGE_BASE1 = "https://ae-pic-a1.aliexpress-media.com/kf/";
const IMAGE_BASE2 = "http://ae-pic-a1.aliexpress-media.com/kf/";

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
    // 1) 키/값 정규화 + 동의어 치환 (키/값 모두 KEY_SYNONYM 사용)
    const pairs = [];
    for (const [k, v] of Object.entries(obj || {})) {
      const kNorm = norm(k);
      const kMapped = VALUE_MAP[k] ?? VALUE_MAP[kNorm] ?? kNorm;

      const vRaw = String(v).trim();
      const vNorm = norm(vRaw);
      const vMapped = VALUE_MAP[vRaw] ?? VALUE_MAP[vNorm] ?? vNorm;

      pairs.push([kMapped, vMapped]);
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
function deepSortObjectKeysKo(input) {
  if (Array.isArray(input)) return input.map(deepSortObjectKeysKo);
  if (input && typeof input === "object") {
    const sorted = Object.entries(input)
      .map(([k, v]) => [normKey(k), deepSortObjectKeysKo(v)])
      .sort(([a], [b]) => koCollator.compare(a, b));
    return Object.fromEntries(sorted);
  }
  return input;
}

const tryCatch = async (fn) => {
  try {
    return { ok: true, value: await fn() };
  } catch (e) {
    return { ok: false, error: e };
  }
};
// 특수문자 이스케이프 + 문자 사이사이에 \s* 허용

const ZWSP = "\u200B"; // 제로폭 공백(실제 문자)
const NBSP = "\u00A0"; // NBSP(실제 문자)

function escapeRegex(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); // 특수문자 리터럴화
}

function makeSpaceAgnosticPattern(raw) {
  const cleaned = String(raw).normalize("NFKC");

  // 허용할 잡음 문자 집합(괄호/공백/구두점/제로폭/NBSP/하이픈/언더스코어)
  const SEP = `[\\s()\\[\\]{}:;,'"\`·•・ㆍ·\\-_${ZWSP}${NBSP}]*`;

  // ❗️여기 바뀜: 문자 단위로 나눈 후 각 문자 escape → SEP로 join
  const body = Array.from(cleaned)
    .map((ch) => escapeRegex(ch))
    .join(SEP);

  return `^${SEP}${body}${SEP}$`;
}

(async () => {
  const divided = [
    {
      cId: "708042", // cpu 1077개
    },
    {
      cId: "708044", // 그래픽 카드 300개
    },
    {
      cId: "701", // 데스크톱 1440개
    },
    {
      cId: "702", // 노트북 vol 0일때 549개
    },
    {
      cId: "7080401", // lcd 모니터 1485개
    },
    {
      cId: "200001082", // 마우스 및 키보드 vol >= 10 일 때  4539개
    },
    {
      cId: "200001086", // 태블릿 2200개
    },
    {
      cId: "200002248", // ssd 813개
    },
    {
      cId: "711002", // 램 1046개
    },
    {
      cId: "200004374", // 게임상품 686개
    },
    {
      cId: "708045", // 메인보드 2375개
    },
    {
      cId: "70901", // 파워서플라이 1790개
    },
    {
      cId: "720", // 컴퓨터 케이스 1073개
    },
    {
      cId: "201612003", // 공냉쿨러 2453개
    },
    {
      cId: "201600006", // 수냉쿨러 552개
    },
    {
      cId: "2", // 음식 3575개
    },
    {
      cId: "100005537", // 낚시 4700개
    },
    {
      cId: "100005544", // 낚시 미끼 4651개
    },
    {
      cId: "200075142", // 낚시 도구 4768개
    },
    {
      cId: "100005539", // 낚시바늘 4279개
    },
    {
      cId: "100005542", // 낚시 릴 4651개
    },
    {
      cId: "100005541", // 낚시 줄 3809개
    },
    {
      cId: "100005546", // 태클박스 3004개
    },
    {
      cId: "100005543", // 낚시대 3526개
    },
    {
      cId: "200003620", // 낚시모자 2066개
    },
    {
      cId: "100007201", // 낚시 찌 3432개
    },
    {
      cId: "200003639", // 낚시조끼 644개
    },
    {
      cId: "200003652", // 낚시 선글라스 922개
    },
    {
      cId: "200003641", // 낚시 장갑 1600개
    },
    {
      cId: "100005879", // 낚시 가방 3100개
    },
    {
      cId: "100007482", // 낚시 의자 1500개
    },
    {
      cId: "201170309", // 로드 거치대 1500개
    },
  ];

  const listTasks = { item: [], dataBaseRes: [] };

  const categoryRes = divided

    // .slice(Math.round(divided[10].length / 2), Math.round(divided[10].length))
    .map((item) =>
      limit(async () => {
        console.log("item", item);
        const { items, raw, serverCount, filteredCount, note } =
          await fetchByCategory({
            categoryId: item.cId,
          });

        console.log("cid:", item.cId);
        console.log("items:", items.length);

        // fetchByCategory안에 filtered 변수도 볼 것 !

        // fetchByCategory 에서 요청을 volume 이 170 이상인것만 받아옴 수정할려면 normalize함수 볼 것

        listTasks.item.push(...items);
      }),
    );

  await Promise.allSettled(categoryRes);

  console.log("dataBaseRes", listTasks.dataBaseRes.length);
  console.log("item", listTasks.item.length);

  const dbs = listTasks.dataBaseRes ?? [];
  const items = (listTasks.item ?? []).filter((p) => {
    // console.log("p", p);
    return Number(p?.volume) >= 0;
  });

  const merged = [
    ...items, // item 뒤 (우선권)
    ...dbs, // DB 먼저
  ];

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
  const failedIds = [];
  await dbConnect();
  await Promise.all(
    uniqueList.map((item) =>
      limit(async () => {
        try {
          // 0) 외부 API
          const productIds = [item._id];

          const skuData = await withRetry(() => getSkuDetail(item._id), {
            retries: 1,
            base: 800,
            max: 10000,
          });

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
          // console.log("volume:", volume);
          // console.log("item.volume:", item.volume);
          // console.log("item._id:", item._id);

          if (item.volume && Number(item.volume) !== 0) {
            baseDoc.vol = item.volume;
          } else {
            const pdRes = await tryCatch(() =>
              withRetry(() => getProductDetailsById(productIds), {
                retries: 2,
                base: 800,
                max: 10000,
              }),
            );
            const productData = pdRes.ok ? pdRes.value : null;

            if (Number(productData?.items[0]?.volume) > 0) {
              baseDoc.vol = productData.items[0].volume;
            }
          }

          // if (
          //   info.original_link &&
          //   stripForCompare(info.original_link) !== ""
          // ) {
          //   baseDoc.ol = info.original_link;
          // }

          // console.log("item.promotion_link", item.promotion_link);

          // https://s.click.aliexpress.com/s/ 문자열을 빼서 데이터공간 저장 확보

          if (
            item.promotion_link &&
            stripForCompare(item.promotion_link) !== ""
          ) {
            if (
              item?.promotion_link &&
              item.promotion_link.startsWith(PL_BASE1)
            ) {
              item.promotion_link = item.promotion_link.slice(PL_BASE1.length);
            } else if (
              item?.promotion_link &&
              item.promotion_link.startsWith(PL_BASE2)
            ) {
              item.promotion_link = item.promotion_link.slice(PL_BASE2.length);
            }
            baseDoc.pl = item.promotion_link;
          } else if (item.pl && stripForCompare(item.pl) !== "") {
            if (item?.pl && item.pl.startsWith(PL_BASE1)) {
              item.pl = item.pl.slice(PL_BASE1.length);
            } else if (item?.pl && item.pl.startsWith(PL_BASE2)) {
              item.pl = item.pl.slice(PL_BASE2.length);
            }
            baseDoc.pl = item.pl;

            // pl값이 비어있으면 새로운 pl값 넣기
          } else if (!item?.pl && stripForCompare(item.pl) === "") {
            const pdRes = await tryCatch(() =>
              withRetry(() => getProductDetailsById(productIds), {
                retries: 2,
                base: 800,
                max: 10000,
              }),
            );

            const productData = pdRes.ok ? pdRes.value : null;
            let promotion_link = productData.items[0]._raw.promotion_link;

            if (promotion_link && promotion_link.startsWith(PL_BASE1)) {
              promotion_link = promotion_link.slice(PL_BASE1.length);
            } else if (promotion_link && promotion_link.startsWith(PL_BASE2)) {
              promotion_link = promotion_link.slice(PL_BASE2.length);
            }

            baseDoc.pl = promotion_link;
          }

          //  ----------------------------il https://ae-pic-a1.aliexpress-media.com/kf/ 데이터베이스 저장공간 줄이기-------------------------------------------

          if (info.image_link && stripForCompare(info.image_link) !== "") {
            if (info.image_link && info.image_link.startsWith(IMAGE_BASE1)) {
              info.image_link = info.image_link.slice(IMAGE_BASE1.length);
            } else if (
              info.image_link &&
              info.image_link.startsWith(IMAGE_BASE2)
            ) {
              info.image_link = info.image_link.slice(IMAGE_BASE2.length);
            }
            baseDoc.il = info.image_link;
          }

          //  ----------------------------ail https://ae-pic-a1.aliexpress-media.com/kf/ 데이터베이스 저장공간 줄이기-------------------------------------------

          if (
            info.additional_image_links?.string &&
            info.additional_image_links?.string.length >= 1
          ) {
            const imgLink = [];
            for (let ImgLink of info.additional_image_links?.string) {
              if (ImgLink && ImgLink.startsWith(IMAGE_BASE1)) {
                ImgLink = ImgLink.slice(IMAGE_BASE1.length);
              } else if (ImgLink && ImgLink.startsWith(IMAGE_BASE2)) {
                ImgLink = ImgLink.slice(IMAGE_BASE2.length);
              }
              imgLink.push(ImgLink);
            }
            baseDoc.ail = imgLink;
          }

          if (cId1) {
            baseDoc.cId1 = cId1;
          }
          if (cId2) {
            baseDoc.cId2 = cId2;
          }

          if (info.display_category_name_l1) {
            baseDoc.c1n = info.display_category_name_l1;
          }
          if (info.display_category_name_l2) {
            baseDoc.c2n = info.display_category_name_l2;
          }
          if (info.display_category_name_l3) {
            baseDoc.c3n = info.display_category_name_l3;
          }
          if (info.display_category_name_l3) {
            baseDoc.c4n = info.display_category_name_l4;
          }

          if (info.title && stripForCompare(info.title) !== "") {
            baseDoc.tt = info.title;
          }
          if (info.product_score && Number(info.product_score) !== 0) {
            baseDoc.ps = info.product_score;
          }
          if (info.review_number && Number(info.review_number) !== 0) {
            baseDoc.rn = info.review_number;
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
              // sId: String(s.sku_id), // 문자열로 통일
              c: normalizeCForCompare(s.color ?? ""), // 정규화 통일
              sp: canonSkuProps(s.sku_properties ?? ""), // 정규화 통일
              spKey: normalizeSpForCompare(s.sku_properties ?? ""), // 정규화 통일
              cur: s.currency ?? "KRW",
              pd: {
                [todayKey]: {
                  s: Number(s.sale_price_with_tax ?? 1),
                  t: new Date(),
                },
              },
            };
          });

          // 4) 기존 문서의 sku_id 집합만 얇게 조회 — 경로 "sku_info.sil"
          const doc = await ProductDetail.findById(productId)
            .select(
              "sku_info.sil.c sku_info.sil.sp sku_info.sil.pd sku_info.sil.spKey",
            )
            .lean();

          const toNum = (v) => (v == null ? NaN : +v);
          const safeNorm = (v) => norm(v ?? "");
          const toKey1 = (color, props) =>
            `\u0001${normalizeCForCompare(color)}\u0001${normalizeSpForCompare(
              props,
            )}`;
          const toKey2 = (color, props) =>
            `\u0001${normalizeCForCompare(color)}\u0001${canonSkuProps(props)}`;
          // const toKey3 = (sid, color, props) =>
          //   `${String(sid)}
          //   \u0001${normalizeSpForCompare(props)}`;
          // const toKey4 = (sid, color, props) =>
          //   `${String(sid)}
          //   \u0001${canonSkuProps(props)}`;

          // 필요한 필드만

          const sil = doc?.sku_info?.sil ?? [];
          // const existingIds = new Set(
          //   (doc?.sku_info?.sil ?? []).map((d) => String(d?.sId))
          // );
          const skuMap1 = new Map();
          const skuMap2 = new Map();
          // const skuMap3 = new Map();
          // const skuMap4 = new Map();
          for (const sku of sil) {
            const i = toKey1(sku?.c, sku?.sp);
            const j = toKey2(sku?.c, sku?.sp);
            // const k = toKey2(sku?.sId, sku?.sp);
            // const z = toKey2(sku?.sId, sku?.sp);

            skuMap1.set(i, sku);
            skuMap2.set(j, sku);
            // skuMap3.set(k, sku);
            // skuMap4.set(z, sku);
          }

          const newSkus = [];
          const updSkus = [];
          const lowPriceUpdSkus = [];

          for (const item1 of skuList) {
            // const sid = String(item1?.sku_id);
            // if (sid == null) continue;

            // if (!existingIds.has(sid)) {
            //   newSkus.push(item1);
            //   continue;
            // }
            const key1 = toKey1(item1?.color, item1?.sku_properties);
            const exist1 = skuMap1.get(key1);
            // console.log("exist1:", exist1);

            if (!exist1) {
              const key2 = toKey2(item1?.color, item1?.sku_properties);
              const exist2 = skuMap2.get(key2);

              if (!exist2) {
                newSkus.push(item1);
                continue;
              }
              // if (!exist2) {
              //   const key3 = toKey3(sid, item1?.sku_properties);
              //   const exist3 = skuMap3.get(key3);
              //   if (!exist3) {
              //     const key4 = toKey4(sid, item1?.sku_properties);
              //     const exist4 = skuMap4.get(key4);
              //     if (!exist4) {
              //       newSkus.push(item1);
              //       continue;
              //     }
              //   }
            }

            // 문제 지점 전후로 세분화 try-catch
            let incomingSale;
            try {
              incomingSale = toNum(item1?.sale_price_with_tax ?? null);
              // incomingSale = toNum(1 ?? null);
            } catch (e) {
              throw e;
            }
            let docToday, docSale;
            try {
              docToday = exist1?.pd?.[todayKey];
              docSale = toNum(docToday?.s);
            } catch (e) {
              throw e;
            }

            if (docToday) {
              if (docSale > incomingSale) {
                lowPriceUpdSkus.push(item1);
              }
            } else {
              updSkus.push(item1);
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
            // const sId = String(s.sku_id);
            const cNorm = normalizeCForCompare(s.color);
            const spCanon = canonSkuProps(s.sku_properties);

            const spRegex = makeSpaceAgnosticPattern(spCanon);
            const cRegex = makeSpaceAgnosticPattern(cNorm);

            const spKey = normalizeSpForCompare(s.sku_properties);

            console.log("item:", item._id);
            console.log("금일 첫 업데이트!");

            const pricePoint = {
              s: Number(s.sale_price_with_tax),
            };

            ops.push({
              updateOne: {
                filter: { _id: productId },
                update: {
                  $set: {
                    // "sku_info.sil.$[e].sId": sId,
                    "sku_info.sil.$[e].c": cNorm,
                    "sku_info.sil.$[e].link": s.link ?? "",
                    "sku_info.sil.$[e].sp": spCanon,
                    "sku_info.sil.$[e].spKey": spKey,
                    "sku_info.sil.$[e].cur": s.currency ?? "KRW",
                    [`sku_info.sil.$[e].pd.${todayKey}`]: pricePoint,
                  },
                },
                arrayFilters: [
                  {
                    // "e.sId": sId,
                    $and: [
                      {
                        $or: [
                          { "e.c": cNorm },
                          { "e.c": { $regex: cRegex, $options: "x" } },
                        ],
                      },
                      {
                        $or: [
                          { "e.spKey": spKey },
                          { "e.sp": spCanon },
                          { "e.sp": s.sku_properties },
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
            // const sId = String(s.sku_id);
            const cNorm = normalizeCForCompare(s.color);
            const spCanon = canonSkuProps(s.sku_properties);

            const spKey = normalizeSpForCompare(s.sku_properties);

            const spRegex = makeSpaceAgnosticPattern(spCanon);
            const cRegex = makeSpaceAgnosticPattern(cNorm);

            console.log("item:", item._id);
            console.log("당일 최저가:!!");

            const pricePoint = {
              s: Number(s.sale_price_with_tax),
            };

            ops.push({
              updateOne: {
                filter: { _id: productId },
                update: {
                  $set: {
                    // "sku_info.sil.$[e].sId": sId,
                    "sku_info.sil.$[e].c": cNorm,
                    "sku_info.sil.$[e].link": s.link ?? "",
                    "sku_info.sil.$[e].sp": spCanon,
                    "sku_info.sil.$[e].spKey": spKey,
                    "sku_info.sil.$[e].cur": s.currency ?? "KRW",
                    [`sku_info.sil.$[e].pd.${todayKey}`]: pricePoint,
                  },
                },
                arrayFilters: [
                  {
                    // "e.sId": sId,
                    $and: [
                      {
                        $or: [
                          { "e.c": cNorm },
                          { "e.c": { $regex: cRegex, $options: "x" } },
                        ],
                      },
                      {
                        $or: [
                          { "e.spKey": spKey },
                          { "e.sp": spCanon },
                          { "e.sp": s.sku_properties },
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
            const toPush = newSkus.map((s) => {
              const spKey = normalizeSpForCompare(s.sku_properties);
              const cNorm = normalizeCForCompare(s.color);
              const spCanon = canonSkuProps(s.sku_properties);

              console.log("새로운 업데이트");

              return {
                // sId: String(s?.sku_id),
                c: cNorm ?? "",
                link: s.link,
                sp: spCanon ?? "",
                spKey: spKey ?? "",
                cur: s.currency ?? "KRW",
                pd: {
                  [todayKey]: {
                    s: s.sale_price_with_tax,
                  },
                },
              };
            });

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
      }),
    ),
  );

  console.log("실패한 상품 IDs:", failedIds);

  process.exit(0);
})();
