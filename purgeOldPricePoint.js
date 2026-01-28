// scripts/prune-stale-skus.js
// ✅ “상품 문서 삭제”가 아니라, sku_info.sil 배열 안에서
//    최근 N일 내 가격 포인트가 없는 SKU 객체(=sil의 원소)만 제거합니다.
// ESM (.js) — package.json에 { "type": "module" } 필요

import mongoose from "mongoose";
import ProductDetail from "./models/ProductDetail.js";
import dbConnect from "./utils/dbConnect.js";

/**
 * 현재 시각 기준 N일 전(threshold) 이후의 가격 포인트가 "단 하나도 없는" sil 원소만 제거
 *
 * @param {Object|string} params.query     MongoDB find 조건. 문자열이면 {_id:"..."} 로 변환
 * @param {number} [params.days=13]        최근 기준 일수
 * @param {boolean} [params.verbose=false] 상세 로그
 * @param {boolean} [params.disconnectAfter=false] 처리 후 연결 종료
 * @param {number} [params.batchSize=300]  bulkWrite 배치 크기
 * @param {number} [params.progressEvery=1000] 진행 로그 간격
 * @returns {Promise<{ now:string, threshold:string, query:Object, total:number, updated:number, unchanged:number, removedSkuCount:number, updatedIds:string[] }>}
 */
export async function main({
  query = {}, // 예: "1005009764703022" 또는 { _id: "..." } 또는 {}
  days = 20,
  verbose = false,
  disconnectAfter = false,
  batchSize = 300,
  progressEvery = 1000,
} = {}) {
  console.log("🔧 [START] prune-stale-skus");
  console.time("⏱️ 전체 소요");

  await dbConnect();
  const stateName =
    { 0: "disconnected", 1: "connected", 2: "connecting", 3: "disconnecting" }[
      mongoose.connection.readyState
    ] || "unknown";
  console.log(`🔌 mongoose 연결 상태: ${stateName}`);

  query = coerceQuery(query);

  const now = new Date();
  const threshold = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);

  const isSingle = hasIdQuery(query);
  console.log(`🧭 now=${now.toISOString()}`);
  console.log(`🧭 threshold=${threshold.toISOString()} (${days}일 기준)`);
  console.log(
    `🧭 대상: ${isSingle ? "단일(_id 지정)" : "전체"} | query=${JSON.stringify(
      query
    )}`
  );
  console.log(`📦 배치크기=${batchSize}, 진행로그 간격=${progressEvery}`);

  const cursor = ProductDetail.find(query)
    .select({ _id: 1, "sku_info.sil": 1 })
    .lean()
    .cursor();

  let bulkOps = [];
  let total = 0;
  let updated = 0;
  let unchanged = 0;
  let removedSkuCount = 0;
  const updatedIds = [];

  console.time("⏱️ 스캔");
  for await (const doc of cursor) {
    total++;

    const sil = doc?.sku_info?.sil || [];
    if (!Array.isArray(sil) || sil.length === 0) {
      unchanged++;
      continue;
    }

    // ✅ 최근 포인트가 있는 SKU만 남김
    const keep = [];
    const removed = [];

    for (let i = 0; i < sil.length; i++) {
      const sku = sil[i];
      const hit = findRecentHitForSku(sku, threshold);

      if (hit.recent) {
        keep.push(sku);
      } else {
        removed.push({ index: i, sku });
      }
    }

    if (removed.length === 0) {
      unchanged++;
    } else {
      removedSkuCount += removed.length;
      updated++;
      updatedIds.push(String(doc._id));

      if (verbose) {
        const newestAll = getNewestPointISOFromSil(sil);
        const newestKept = getNewestPointISOFromSil(keep);
        console.log(
          `🧹 _id=${doc._id} | sil ${sil.length} → ${keep.length} (제거 ${
            removed.length
          }) | threshold>=${threshold.toISOString()}`
        );
        console.log(
          `   📌 전체 최신=${newestAll ?? "없음"} | 유지 후 최신=${
            newestKept ?? "없음"
          }`
        );

        // 제거된 SKU들의 "대략적인 식별 정보" 출력 (sId가 없으니 c/sp 일부만)
        for (const r of removed.slice(0, 5)) {
          const c = r.sku?.c ?? "";
          const sp = r.sku?.sp ?? "";
          const keyCount = countPdKeys(r.sku?.pd);
          console.log(
            `   🗑️ remove skuIndex=${r.index} | c="${String(c).slice(
              0,
              30
            )}" | sp="${String(sp).slice(0, 30)}" | pdKeys=${keyCount}`
          );
        }
        if (removed.length > 5)
          console.log(`   ... +${removed.length - 5}개 더 제거`);
      }

      // ✅ 배열 전체를 "필터된 값"으로 교체 (가장 안전/확실)
      bulkOps.push({
        updateOne: {
          filter: { _id: doc._id },
          update: { $set: { "sku_info.sil": keep } },
        },
      });

      // 단일 테스트면 즉시 반영
      if (isSingle) {
        await flushBulk(bulkOps);
        bulkOps = [];
      }
    }

    if (progressEvery > 0 && total % progressEvery === 0) {
      console.log(
        `⏩ 진행: 처리=${total} | 업데이트=${updated} | 변화없음=${unchanged} | 제거SKU누적=${removedSkuCount} | 배치대기=${bulkOps.length}`
      );
    }

    if (bulkOps.length >= batchSize) {
      await flushBulk(bulkOps);
      bulkOps = [];
    }
  }
  console.timeEnd("⏱️ 스캔");

  if (bulkOps.length) {
    await flushBulk(bulkOps, true);
    bulkOps = [];
  }

  const result = {
    now: now.toISOString(),
    threshold: threshold.toISOString(),
    query,
    total,
    updated,
    unchanged,
    removedSkuCount,
    updatedIds,
  };

  console.log("📊 요약:", {
    total,
    updated,
    unchanged,
    removedSkuCount,
    sampleUpdatedIds: updatedIds.slice(0, 10),
  });

  if (disconnectAfter) {
    try {
      await mongoose.connection.close();
      console.log("🔌 mongoose 연결 종료");
    } catch (e) {
      console.warn("⚠️ 연결 종료 중 오류:", e?.message || e);
    }
  }

  console.timeEnd("⏱️ 전체 소요");
  console.log("✅ [END] prune-stale-skus");

  return result;
}

// ───────────────────────────────────────────
// bulkWrite flush helper
async function flushBulk(ops, isLast = false) {
  if (!ops.length) return;
  const label = isLast ? "bulkWrite(마지막)" : "bulkWrite";
  console.time(`⏱️ ${label}`);
  try {
    const res = await ProductDetail.bulkWrite(ops, { ordered: false });
    console.log(
      `💥 ${label} 실행: modified=${res?.modifiedCount ?? 0}, matched=${
        res?.matchedCount ?? 0
      }, 배치크기=${ops.length}`
    );
  } catch (err) {
    console.error(`❌ ${label} 에러:`, err?.message || err);
  } finally {
    console.timeEnd(`⏱️ ${label}`);
  }
}

// 문자열 query 방어
function coerceQuery(q) {
  if (typeof q === "string" && q.trim()) return { _id: q.trim() };
  if (q && typeof q === "object") return q;
  return {};
}
function hasIdQuery(q) {
  return !!(q && Object.prototype.hasOwnProperty.call(q, "_id"));
}

// ───────────────────────────────────────────
// ✅ 엄격한 key 파싱: YYYY-MM-DD 또는 ISO(YYYY-MM-DDTHH:mm:ss...)만 허용
function parseDateKeyStrict(dateKey) {
  if (typeof dateKey !== "string") return null;

  const ok =
    /^\d{4}-\d{2}-\d{2}$/.test(dateKey) || /^\d{4}-\d{2}-\d{2}T/.test(dateKey);

  if (!ok) return null;

  const d = new Date(dateKey);
  return Number.isNaN(d.valueOf()) ? null : d;
}

function parsePointFallback(p) {
  if (!p) return null;
  const t = p.t || p.collected_at;
  if (!t) return null;
  const d = new Date(t);
  return Number.isNaN(d.valueOf()) ? null : d;
}

/**
 * SKU 한 개(sil 원소)에 대해, threshold 이후 포인트가 있는지 판단
 * - 1순위: pd key(엄격 파싱)
 * - 2순위: value의 t/collected_at(구 데이터)
 */
function findRecentHitForSku(sku, threshold) {
  const pd = sku?.pd;
  if (!pd) return { recent: false, hit: null };

  const entries =
    pd instanceof Map ? Array.from(pd.entries()) : Object.entries(pd);

  for (const [dateKey, p] of entries) {
    const dKey = parseDateKeyStrict(dateKey);
    if (dKey && dKey >= threshold) {
      return {
        recent: true,
        hit: { key: dateKey, dt: dKey, via: "key(strict)" },
      };
    }

    // key가 엄격 파싱 실패했을 때만 fallback을 보게 해서 오판정 줄이기
    if (!dKey) {
      const dFallback = parsePointFallback(p);
      if (dFallback && dFallback >= threshold) {
        return {
          recent: true,
          hit: { key: dateKey, dt: dFallback, via: "value(t/collected_at)" },
        };
      }
    }
  }

  return { recent: false, hit: null };
}

// ───────────────────────────────────────────
// 로그용 유틸
function getNewestPointISOFromSil(sil) {
  let newest = null;
  for (const sku of sil || []) {
    const pd = sku?.pd;
    if (!pd) continue;

    const entries =
      pd instanceof Map ? Array.from(pd.entries()) : Object.entries(pd);

    for (const [dateKey, p] of entries) {
      const dKey = parseDateKeyStrict(dateKey);
      const dt = dKey || (!dKey ? parsePointFallback(p) : null);
      if (!dt) continue;
      if (!newest || dt > newest) newest = dt;
    }
  }
  return newest ? newest.toISOString() : null;
}

function countPdKeys(pd) {
  if (!pd) return 0;
  const entries = pd instanceof Map ? Array.from(pd.keys()) : Object.keys(pd);
  return entries.length;
}

// ───────────────────────────────────────────
// 직접 실행 예시
main({
  // query: "1005009764703022", // 단일 테스트
  days: 13,
  verbose: true,
  disconnectAfter: true,
}).catch((e) => {
  console.error("❌ 실행 오류:", e);
});
