import dotenv from "dotenv";
import mongoose from "mongoose";
import dbConnect from "./utils/dbConnect.js";
import ProductDetail from "./models/ProductDetail.js";
import { translateSkuPropertiesSimple } from "./utils/skuTranslate.js";

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

// ─────────────────────────────────────────────────────────────────────────────
// 비교용 정규화: 지정 특수문자 + 공백 제거
function stripForCompare(s) {
  const a = String(s ?? "").replace(/[{}\[\]\(\)\"\s]/g, "");
  const trans = translateSkuPropertiesSimple(a);
  console.log("trans:", trans);
  return trans;
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
    const trans = stripForCompare(spStr);
    let arr = JSON.parse(trans);
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
async function processOneDoc(doc) {
  const sil = doc?.sku_info?.sil || [];
  if (!sil.length) return { changed: false, before: 0, after: 0, metrics: {} };

  // key = sId||cNorm||spNorm
  const buckets = new Map();
  for (const it of sil) {
    const sid = it?.sId;
    if (!sid) continue; // sId 없는 비정상은 병합 대상 제외
    const cNorm = normalizeCForCompare(it?.c ?? "");
    const spNorm = normalizeSpForCompare(it?.sp ?? "");
    console.log("cNorm:", cNorm);
    console.log("spNorm:", spNorm);
    const key = `${sid}||${cNorm}||${spNorm}`;
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key).push(it);
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

async function main() {
  await dbConnect();
  console.log(
    `🚀 Bulk merge by (sId,c,sp) 시작 (dry-run: ${dryRun ? "YES" : "NO"})`
  );

  const query = { _id: "1005008077615451" };
  const projection = { "sku_info.sil": 1 };
  const cursor = ProductDetail.find(query, projection).cursor();

  let visited = 0;
  let changedDocs = 0;
  let totalRowsDeleted = 0;
  let totalPdAdded = 0;
  let totalGroupsMerged = 0;

  for await (const doc of cursor) {
    visited++;
    const { changed, before, after, metrics } = await processOneDoc(doc);
    if (changed) {
      changedDocs++;
      totalRowsDeleted += metrics.rowsDeleted || 0;
      totalPdAdded += metrics.pdAddedTotal || 0;
      totalGroupsMerged += metrics.groupsMerged || 0;

      console.log(
        `✔ _id=${doc._id} | sil ${before} → ${after} | +pd:${metrics.pdAddedTotal} | del:${metrics.rowsDeleted} | groups:${metrics.groupsMerged}`
      );
    }
    if (limit && visited >= limit) break;
  }

  console.log("\n===== SUMMARY =====");
  console.log(`Visited docs : ${visited}`);
  console.log(`Changed docs : ${changedDocs}`);
  console.log(`Rows deleted : ${totalRowsDeleted}`);
  console.log(`pd added (keys): ${totalPdAdded}`);
  console.log(`Groups merged : ${totalGroupsMerged}`);
  console.log(` Mode : ${dryRun ? "DRY-RUN (no save)" : "APPLY (saved)"}`);

  await mongoose.connection.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
