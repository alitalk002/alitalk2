// scripts/prune-stale-products.js
// ESM (.js) — package.json에 { "type": "module" } 필요

import mongoose from "mongoose";
import ProductDetail from "./models/ProductDetail.js";
import dbConnect from "./utils/dbConnect.js";

/**
 * 현재 시각(new Date()) 기준 **N일 전(기본 20일)** 이후의 pd[*].t(=collected_at)가
 * '단 하나도 없는' 상품을 삭제합니다.
 *
 * @param {Object|string} params.query     MongoDB find 조건. 문자열이면 {_id: "<문자열>"}로 자동 변환
 * @param {number} [params.days=20]        일 수 기준(기본 20일)
 * @param {boolean} [params.verbose=false] 문서별 상세 로그
 * @param {boolean} [params.disconnectAfter=false] 처리 후 mongoose 연결 종료
 * @param {number} [params.batchSize=500]  bulkWrite 배치 크기
 * @param {number} [params.progressEvery=1000] 진행 로그 출력 간격(도큐먼트 수)
 * @returns {Promise<{ now:string, threshold:string, query:Object, total:number, deleted:number, kept:number, deletedIds:string[] }>}
 */
export async function main({
  query = {},
  days = 16, // ✅ 기본 16일
  verbose = false,
  disconnectAfter = false,
  batchSize = 500,
  progressEvery = 1000,
} = {}) {
  console.log("🔧 [START] prune-stale-products");
  console.time("⏱️ 전체 소요");

  await dbConnect();
  const stateName =
    { 0: "disconnected", 1: "connected", 2: "connecting", 3: "disconnecting" }[
      mongoose.connection.readyState
    ] || "unknown";
  console.log(`🔌 mongoose 연결 상태: ${stateName}`);

  // 문자열 query면 _id로 강제
  query = coerceQuery(query);

  const now = new Date(); // ✅ 현재 시간
  const threshold = new Date(now.getTime() - days * 24 * 60 * 60 * 1000); // ✅ 20일(기본)

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
    .select({ _id: 1, "sku_info.sil.pd": 1 })
    .lean()
    .cursor();

  let bulkOps = [];
  let total = 0;
  let deleted = 0;
  let kept = 0;
  const deletedIds = [];

  console.time("⏱️ 스캔");
  for await (const doc of cursor) {
    total++;
    const recent = hasRecentPricePoint(doc, threshold);

    if (!recent) {
      // 삭제 후보
      bulkOps.push({ deleteOne: { filter: { _id: doc._id } } });
      deletedIds.push(String(doc._id));
      deleted++;

      if (verbose) {
        const newest = getNewestPointISO(doc);
        const points = countPricePoints(doc);
        console.log(
          `🗑️ 삭제대상: _id=${doc._id} | 포인트수=${points} | 최신=${
            newest ?? "없음"
          } | 기준>=${threshold.toISOString()}`
        );
      }

      // 단일 대상 테스트 시 즉시 flush
      if (isSingle) {
        await flushBulk(bulkOps);
        bulkOps = [];
      }
    } else {
      kept++;
      if (verbose && isSingle) {
        const newest = getNewestPointISO(doc);
        const points = countPricePoints(doc);
        console.log(
          `✔️ 유지: _id=${doc._id} | 포인트수=${points} | 최신=${
            newest ?? "없음"
          } (>= ${threshold.toISOString()})`
        );
      }
    }

    if (progressEvery > 0 && total % progressEvery === 0) {
      console.log(
        `⏩ 진행: 처리=${total} | 삭제예정=${deleted} | 유지=${kept} | 배치대기=${bulkOps.length}`
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

  if (deleted === 0) {
    console.log(
      "ℹ️ 삭제 후보가 0건입니다. (모든 문서가 기준 내 최근 포인트를 보유하거나, 질의 결과가 비었습니다)"
    );
  }

  const result = {
    now: now.toISOString(),
    threshold: threshold.toISOString(),
    query,
    total,
    deleted,
    kept,
    deletedIds,
  };

  console.log("📊 요약:", {
    total,
    deleted,
    kept,
    sampleDeletedIds: deletedIds.slice(0, 10),
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
  console.log("✅ [END] prune-stale-products");

  return result;
}

// ───────────────────────────────────────────
// bulkWrite flush helper (로그 포함)
async function flushBulk(ops, isLast = false) {
  if (!ops.length) return;
  const label = isLast ? "bulkWrite(마지막)" : "bulkWrite";
  console.time(`⏱️ ${label}`);
  try {
    const res = await ProductDetail.bulkWrite(ops, { ordered: false });
    console.log(
      `💥 ${label} 실행: 삭제=${res?.deletedCount ?? 0}, 배치크기=${ops.length}`
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

// pd(Map|Object)에 threshold 이상 t 존재 여부
function hasRecentPricePoint(doc, threshold) {
  const sil = doc?.sku_info?.sil || [];
  for (const sku of sil) {
    const pd = sku?.pd;
    if (!pd) continue;
    const values =
      pd instanceof Map ? Array.from(pd.values()) : Object.values(pd);
    for (const p of values) {
      if (!p) continue;
      const t = p.t || p.collected_at;
      if (!t) continue;
      const dt = new Date(t);
      if (!Number.isNaN(dt.valueOf()) && dt >= threshold) return true;
    }
  }
  return false;
}

// 최신 t ISO
function getNewestPointISO(doc) {
  let newest = null;
  const sil = doc?.sku_info?.sil || [];
  for (const sku of sil) {
    const pd = sku?.pd;
    if (!pd) continue;
    const values =
      pd instanceof Map ? Array.from(pd.values()) : Object.values(pd);
    for (const p of values) {
      const t = p?.t || p?.collected_at;
      if (!t) continue;
      const dt = new Date(t);
      if (!Number.isNaN(dt.valueOf()) && (!newest || dt > newest)) newest = dt;
    }
  }
  return newest ? newest.toISOString() : null;
}

// 포인트 개수(로그용)
function countPricePoints(doc) {
  let count = 0;
  const sil = doc?.sku_info?.sil || [];
  for (const sku of sil) {
    const pd = sku?.pd;
    if (!pd) continue;
    const values =
      pd instanceof Map ? Array.from(pd.values()) : Object.values(pd);
    count += values.length;
  }
  return count;
}

// ───────────────────────────────────────────
// 직접 실행 예시 — 필요 시 _id 지정 가능
// 단일 테스트: query: "1005007288239328"
main({
  // query: "1005007288239328",
  // days: 20, // 기본 20일, 변경 가능
  verbose: true,
  disconnectAfter: true,
}).catch((e) => {
  console.error("❌ 실행 오류:", e);
});
