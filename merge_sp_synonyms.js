import dotenv from "dotenv";
import mongoose from "mongoose";
import dbConnect from "./utils/dbConnect.js";
import ProductDetail from "./models/ProductDetail.js";
import {
  translateSkuPropertiesSimple,
  VALUE_MAP,
} from "./utils/skuTranslate.js";
import {
  normalizeCForCompare,
  normalizeSpForCompare,
} from "./utils/normalize.js";

dotenv.config();

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");

const limit = (() => {
  const byEq = args.find((a) => a.startsWith("--limit="));
  if (byEq) return Number(byEq.split("=")[1]) || 0;
  const idx = args.indexOf("--limit");
  if (idx >= 0 && args[idx + 1]) return Number(args[idx + 1]) || 0;
  return 0;
})();

// 필요 시 끄기: false 로 두면 '색깔'을 '색상'으로 바꾸지 않음
const USE_SYNONYM_MAP = true;
const SYNONYM_KEY_MAP = { 색깔: "색상" };

const _SP_MAP = { 색깔: "색상" }; // 라벨 동의어

// ─────────────────────────────────────────────────────────────────────────────
// 얕은 정렬: 1-depth 객체의 키만 정렬

const norm = (v) =>
  (v ?? "") // null/undefined 방어
    .toString() // 문자열화
    .replace(/[\s\u200B-\u200D\uFEFF]/g, ""); // 일반 공백 + 제로폭 공백 제거

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

// 한국어 Collator: 대소문/자모 분해 차이 최소화
const koCollator = new Intl.Collator("ko", {
  sensitivity: "base", // ㅂ vs ㅃ처럼 미세한 차이는 무시
  ignorePunctuation: false,
  numeric: true, // "키2" < "키10" 같은 숫자 인식 정렬
});

// 키 문자열을 NFC로 표준화 (동일 글자 다른 조합을 통일)
function normKey(k) {
  return String(k).normalize("NFC");
}

// 얕은 정렬
function sortObjectKeysKo(obj) {
  return Object.fromEntries(
    Object.entries(obj)
      .map(([k, v]) => [normKey(k), v])
      .sort(([a], [b]) => koCollator.compare(a, b))
  );
}

// function deepSortObjectKeysKo(input) {
//   if (Array.isArray(input)) return input.map(deepSortObjectKeysKo);
//   if (input && typeof input === "object") {
//     const sorted = Object.entries(input)
//       .map(([k, v]) => [normKey(k), deepSortObjectKeysKo(v)])
//       .sort(([a], [b]) => koCollator.compare(a, b));
//     return Object.fromEntries(sorted);
//   }
//   return input;
// }

// function sortArrayOfObjectsStable(arr) {
//   return arr
//     .map((o) => deepSortObjectKeysKo(o))
//     .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
// }

// function stripMinimal(s) {
//   return String(s ?? "").replace(/[{}\[\]\(\)\"\s]/g, "");
// }

// // 비교용 정규화: 지정 특수문자 + 공백 제거
// function stripForCompare(s) {
//   const raw = String(s ?? "");
//   const translated = translateSkuPropertiesSimple(raw); // ← 먼저 치환

//   // 치환 결과가 JSON이면 정렬/직렬화 후 strip, 아니면 문자열 strip
//   if (typeof translated === "string") {
//     try {
//       const parsed = JSON.parse(translated);
//       const arr = Array.isArray(parsed) ? parsed : [parsed];
//       const sortedArr = sortArrayOfObjectsStable(arr);
//       return stripMinimal(JSON.stringify(sortedArr));
//     } catch {
//       return stripMinimal(translated);
//     }
//   } else if (translated && typeof translated === "object") {
//     const arr = Array.isArray(translated) ? translated : [translated];
//     const sortedArr = sortArrayOfObjectsStable(arr);
//     return stripMinimal(JSON.stringify(sortedArr));
//   }
//   return stripMinimal(translated);
// }

// // 3) c / sp 정규화

// // c 필드 비교용 정규화
// export function normalizeCForCompare(c) {
//   return stripForCompare(c);
// }

// // sp 비교용 정규화
// export function normalizeSpForCompare(spStr) {
//   // 1) JSON 파싱 시도
//   try {
//     let arr = JSON.parse(spStr);
//     if (!Array.isArray(arr)) arr = [arr];

//     const trans = stripForCompare(spStr);

//     // 3) 안정적 직렬화 후 strip
//     const stable = JSON.stringify(trans);
//     return stripForCompare(stable);
//   } catch {
//     // 파싱 불가 → 그냥 strip 규칙만 적용
//     return stripForCompare(spStr);
//   }
// }

// 원본 보존 + 보기 좋은 sp 선택: "색상" 표기 선호, 없으면 첫 번째
function pickSurvivor(items) {
  const idx = items.findIndex((x) => /"색상"\s*:/.test(x?.sp || ""));
  return idx >= 0 ? items[idx] : items[0];
}

// pd 병합: 날짜키 합집합(충돌 시 기존값 유지). 반환: 추가된 키 수
function mergePdKeepExisting(basePd, addPd) {
  if (!basePd || !addPd) return 0;
  const baseIsMap = typeof basePd?.set === "function";
  const addIsMap = typeof addPd?.entries === "function";
  let added = 0;

  if (baseIsMap) {
    if (addIsMap) {
      for (const [day, point] of addPd.entries()) {
        if (!basePd.has(day)) {
          basePd.set(day, point);
          added++;
        }
      }
    } else {
      for (const [day, point] of Object.entries(addPd || {})) {
        if (!basePd.has(day)) {
          basePd.set(day, point);
          added++;
        }
      }
    }
  } else {
    if (addIsMap) {
      for (const [day, point] of addPd.entries()) {
        if (!(day in basePd)) {
          basePd[day] = point;
          added++;
        }
      }
    } else {
      for (const [day, point] of Object.entries(addPd || {})) {
        if (!(day in basePd)) {
          basePd[day] = point;
          added++;
        }
      }
    }
  }
  return added;
}

// 한 문서 처리: (sId && c && sp) 정규화 값이 같은 것들만 병합
async function processOneDoc1(doc) {
  const sil = doc?.sku_info?.sil || [];
  if (!sil.length) return { changed: false, before: 0, after: 0, metrics: {} };

  // key = sId||cNorm||spNorm
  const buckets = new Map();
  for (const it of sil) {
    const sid = it?.sId;
    if (!sid) continue; // sId 없는 비정상은 병합 대상 제외
    const cNorm = normalizeCForCompare(it?.c ?? "");
    let spNorm;
    if (it?.spKey) {
      spNorm = it.spKey;
    } else {
      spNorm = canonSkuProps(it?.sp ?? "");
    }
    // const key = `${String(sid)}||
    // ${String(cNorm)}||
    // ${String(
    //   normalizeSpForCompare(it?.sp)
    // )}`;
    const key1 = `${String(sid)}||
    ${canonSkuProps(it?.sp ?? "")}`;
    const key2 = `${String(sid)}||
   ${normalizeSpForCompare(it?.sp)}`;

    if (!buckets.has(key1)) buckets.set(key1, []);
    buckets.get(key1).push(it);

    // if (!buckets.has(key2)) buckets.set(key2, []);
    // buckets.get(key2).push(it);
  }

  let changed = false;
  let pdAddedTotal = 0;
  let rowsDeleted = 0;
  let groupsMerged = 0;

  const survivors = [];
  for (const [, items] of buckets.entries()) {
    if (items.length === 1) {
      survivors.push(items[0]);
      continue;
    }
    // 병합 그룹: 대표를 고르고 나머지 pd 합침
    const survivor = pickSurvivor(items);
    if (!survivor.pd) survivor.pd = new Map();

    for (const it of items) {
      if (it === survivor) continue;
      if (!it.pd) it.pd = new Map();
      pdAddedTotal += mergePdKeepExisting(survivor.pd, it.pd);
      rowsDeleted++;
      changed = true;
    }
    survivors.push(survivor);
    groupsMerged++;
  }

  // buckets에 들어가지 않은(= sId 없던) 잔여 붙이기
  for (const it of sil) {
    if (!it?.sId) survivors.push(it);
  }

  const before = sil.length;
  const after = survivors.length;

  if (changed) {
    doc.sku_info.sil = survivors;
    doc.markModified("sku_info.sil");
    if (!dryRun) await doc.save();
  }

  return {
    changed,
    before,
    after,
    metrics: { pdAddedTotal, rowsDeleted, groupsMerged },
  };
}

async function processOneDoc2(doc) {
  const sil = doc?.sku_info?.sil || [];
  if (!sil.length) return { changed: false, before: 0, after: 0, metrics: {} };

  // key = sId||cNorm||spNorm
  const buckets = new Map();
  for (const it of sil) {
    const sid = it?.sId;
    if (!sid) continue; // sId 없는 비정상은 병합 대상 제외
    const cNorm = normalizeCForCompare(it?.c ?? "");
    let spNorm;
    if (it?.spKey) {
      spNorm = it.spKey;
    } else {
      spNorm = canonSkuProps(it?.sp ?? "");
    }
    // const key = `${String(sid)}||
    // ${String(cNorm)}||
    // ${String(
    //   normalizeSpForCompare(it?.sp)
    // )}`;
    // const key1 = `${String(sid)}||
    // ${canonSkuProps(it?.sp ?? "")}`;
    const key2 = `${String(sid)}||
   ${normalizeSpForCompare(it?.sp)}`;

    // if (!buckets.has(key1)) buckets.set(key1, []);
    // buckets.get(key1).push(it);

    if (!buckets.has(key2)) buckets.set(key2, []);
    buckets.get(key2).push(it);
  }

  let changed = false;
  let pdAddedTotal = 0;
  let rowsDeleted = 0;
  let groupsMerged = 0;

  const survivors = [];
  for (const [, items] of buckets.entries()) {
    if (items.length === 1) {
      survivors.push(items[0]);
      continue;
    }
    // 병합 그룹: 대표를 고르고 나머지 pd 합침
    const survivor = pickSurvivor(items);
    if (!survivor.pd) survivor.pd = new Map();

    for (const it of items) {
      if (it === survivor) continue;
      if (!it.pd) it.pd = new Map();
      pdAddedTotal += mergePdKeepExisting(survivor.pd, it.pd);
      rowsDeleted++;
      changed = true;
    }
    survivors.push(survivor);
    groupsMerged++;
  }

  // buckets에 들어가지 않은(= sId 없던) 잔여 붙이기
  for (const it of sil) {
    if (!it?.sId) survivors.push(it);
  }

  const before = sil.length;
  const after = survivors.length;

  if (changed) {
    doc.sku_info.sil = survivors;
    doc.markModified("sku_info.sil");
    if (!dryRun) await doc.save();
  }

  return {
    changed,
    before,
    after,
    metrics: { pdAddedTotal, rowsDeleted, groupsMerged },
  };
}
async function processOneDoc3(doc) {
  const sil = doc?.sku_info?.sil || [];
  if (!sil.length) return { changed: false, before: 0, after: 0, metrics: {} };

  // key = sId||cNorm||spNorm
  const buckets = new Map();
  for (const it of sil) {
    const sid = it?.sId;
    if (!sid) continue; // sId 없는 비정상은 병합 대상 제외
    const cNorm = normalizeCForCompare(it?.c ?? "");
    let spNorm;
    if (it?.spKey) {
      spNorm = it.spKey;
    } else {
      spNorm = canonSkuProps(it?.sp ?? "");
    }
    // const key = `${String(sid)}||
    // ${String(cNorm)}||
    // ${String(
    //   normalizeSpForCompare(it?.sp)
    // )}`;
    // const key1 = `${String(sid)}||
    // ${canonSkuProps(it?.sp ?? "")}`;
    //   const key2 = `${String(sid)}||
    //  ${normalizeSpForCompare(it?.sp)}`;
    const key3 = `${cNorm}||
   ${normalizeSpForCompare(it?.sp)}`;

    // if (!buckets.has(key1)) buckets.set(key1, []);
    // buckets.get(key1).push(it);

    if (!buckets.has(key3)) buckets.set(key3, []);
    buckets.get(key3).push(it);
  }

  let changed = false;
  let pdAddedTotal = 0;
  let rowsDeleted = 0;
  let groupsMerged = 0;

  const survivors = [];
  for (const [, items] of buckets.entries()) {
    if (items.length === 1) {
      survivors.push(items[0]);
      continue;
    }
    // 병합 그룹: 대표를 고르고 나머지 pd 합침
    const survivor = pickSurvivor(items);
    if (!survivor.pd) survivor.pd = new Map();

    for (const it of items) {
      if (it === survivor) continue;
      if (!it.pd) it.pd = new Map();
      pdAddedTotal += mergePdKeepExisting(survivor.pd, it.pd);
      rowsDeleted++;
      changed = true;
    }
    survivors.push(survivor);
    groupsMerged++;
  }

  // buckets에 들어가지 않은(= sId 없던) 잔여 붙이기
  for (const it of sil) {
    if (!it?.sId) survivors.push(it);
  }

  const before = sil.length;
  const after = survivors.length;

  if (changed) {
    doc.sku_info.sil = survivors;
    doc.markModified("sku_info.sil");
    if (!dryRun) await doc.save();
  }

  return {
    changed,
    before,
    after,
    metrics: { pdAddedTotal, rowsDeleted, groupsMerged },
  };
}
async function processOneDoc4(doc) {
  const sil = doc?.sku_info?.sil || [];
  if (!sil.length) return { changed: false, before: 0, after: 0, metrics: {} };

  // key = sId||cNorm||spNorm
  const buckets = new Map();
  for (const it of sil) {
    const sid = it?.sId;
    if (!sid) continue; // sId 없는 비정상은 병합 대상 제외
    const cNorm = normalizeCForCompare(it?.c ?? "");
    let spNorm;
    if (it?.spKey) {
      spNorm = it.spKey;
    } else {
      spNorm = canonSkuProps(it?.sp ?? "");
    }
    // const key = `${String(sid)}||
    // ${String(cNorm)}||
    // ${String(
    //   normalizeSpForCompare(it?.sp)
    // )}`;
    // const key1 = `${String(sid)}||
    // ${canonSkuProps(it?.sp ?? "")}`;
    //   const key2 = `${String(sid)}||
    //  ${normalizeSpForCompare(it?.sp)}`;
    //   const key3 = `${cNorm}||
    //  ${normalizeSpForCompare(it?.sp)}`;
    const key4 = `${cNorm}||
   ${canonSkuProps(it?.sp)}`;

    // if (!buckets.has(key1)) buckets.set(key1, []);
    // buckets.get(key1).push(it);

    if (!buckets.has(key4)) buckets.set(key4, []);
    buckets.get(key4).push(it);
  }

  let changed = false;
  let pdAddedTotal = 0;
  let rowsDeleted = 0;
  let groupsMerged = 0;

  const survivors = [];
  for (const [, items] of buckets.entries()) {
    if (items.length === 1) {
      survivors.push(items[0]);
      continue;
    }
    // 병합 그룹: 대표를 고르고 나머지 pd 합침
    const survivor = pickSurvivor(items);
    if (!survivor.pd) survivor.pd = new Map();

    for (const it of items) {
      if (it === survivor) continue;
      if (!it.pd) it.pd = new Map();
      pdAddedTotal += mergePdKeepExisting(survivor.pd, it.pd);
      rowsDeleted++;
      changed = true;
    }
    survivors.push(survivor);
    groupsMerged++;
  }

  // buckets에 들어가지 않은(= sId 없던) 잔여 붙이기
  for (const it of sil) {
    if (!it?.sId) survivors.push(it);
  }

  const before = sil.length;
  const after = survivors.length;

  if (changed) {
    doc.sku_info.sil = survivors;
    doc.markModified("sku_info.sil");
    if (!dryRun) await doc.save();
  }

  return {
    changed,
    before,
    after,
    metrics: { pdAddedTotal, rowsDeleted, groupsMerged },
  };
}

async function main1() {
  await dbConnect();
  console.log(
    `🚀 Bulk merge by (sId,c,sp) 시작 (dry-run: ${dryRun ? "YES" : "NO"})`
  );

  const query = {};
  const projection = { "sku_info.sil": 1 };
  const cursor = ProductDetail.find(query, projection).cursor();

  let visited1 = 0;
  let changedDocs1 = 0;
  let totalRowsDeleted1 = 0;
  let totalPdAdded1 = 0;
  let totalGroupsMerged1 = 0;

  for await (const doc of cursor) {
    visited1++;
    console.log("id:", doc._id);
    const { changed, before, after, metrics } = await processOneDoc3(doc);
    if (changed) {
      changedDocs1++;
      totalRowsDeleted1 += metrics.rowsDeleted || 0;
      totalPdAdded1 += metrics.pdAddedTotal || 0;
      totalGroupsMerged1 += metrics.groupsMerged || 0;

      console.log(
        `✔ _id=${doc._id} | sil ${before} → ${after} | +pd:${metrics.pdAddedTotal} | del:${metrics.rowsDeleted} | groups:${metrics.groupsMerged}`
      );
    }
    if (limit && visited1 >= limit) break;
  }

  let visited2 = 0;
  let changedDocs2 = 0;
  let totalRowsDeleted2 = 0;
  let totalPdAdded2 = 0;
  let totalGroupsMerged2 = 0;

  for await (const doc of cursor) {
    visited2++;
    console.log("id:", doc._id);
    const { changed, before, after, metrics } = await processOneDoc4(doc);
    if (changed) {
      changedDocs2++;
      totalRowsDeleted2 += metrics.rowsDeleted || 0;
      totalPdAdded2 += metrics.pdAddedTotal || 0;
      totalGroupsMerged2 += metrics.groupsMerged || 0;

      console.log(
        `✔ _id=${doc._id} | sil ${before} → ${after} | +pd:${metrics.pdAddedTotal} | del:${metrics.rowsDeleted} | groups:${metrics.groupsMerged}`
      );
    }
    if (limit && visited2 >= limit) break;
  }

  console.log("\n===== SUMMARY =====");
  console.log(`Visited docs : ${visited1}`);
  console.log(`Changed docs : ${changedDocs1}`);
  console.log(`Rows deleted : ${totalRowsDeleted1}`);
  console.log(`pd added (keys): ${totalPdAdded1}`);
  console.log(`Groups merged : ${totalGroupsMerged1}`);
  console.log(` Mode : ${dryRun ? "DRY-RUN (no save)" : "APPLY (saved)"}`);

  await mongoose.connection.close();
}

main1().catch((e) => {
  console.error(e);
  process.exit(1);
});
