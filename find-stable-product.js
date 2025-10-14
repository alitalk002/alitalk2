// scripts/find-stale-products.js
// ESM (.js) 파일 — package.json에 { "type": "module" } 필요

import mongoose from "mongoose";
import ProductDetail from "./models/ProductDetail.js";
import dbConnect from "./utils/dbConnect.js";

/**
 * 현재 시각(new Date()) 기준 한 달 전(또는 days 지정 시 N일 전) 이후의
 * pd[*].t(=collected_at)가 단 하나도 없는 상품들의 _id만 수집합니다. (삭제 없음)
 *
 * @param {Object|string} params.query     MongoDB find 조건. 예) { _id: "100..." } 또는 {}
 *                                         문자열이면 자동으로 { _id: "<문자열>" }로 변환
 * @param {number} [params.months=1]       달력 기준 개월 수 (기본 1개월)
 * @param {number} [params.days]           일 수 기준 (지정 시 months 무시)
 * @param {boolean} [params.verbose=false] 상세 로그
 * @param {boolean} [params.disconnectAfter=false] 처리 후 mongoose 연결 종료
 * @param {number} [params.progressEvery=1000]     진행 로그 출력 간격(도큐먼트 수)
 * @returns {Promise<{ now:string, threshold:string, query:Object, total:number, staleCount:number, keptCount:number, staleIds:string[] }>}
 */
export async function main({
  query = {},
  months = 1,
  days,
  verbose = false,
  disconnectAfter = false,
  progressEvery = 1000,
} = {}) {
  console.log("🔧 [START] find-stale-products");
  console.time("⏱️ 전체 소요");

  await dbConnect();
  const stateName =
    { 0: "disconnected", 1: "connected", 2: "connecting", 3: "disconnecting" }[
      mongoose.connection.readyState
    ] || "unknown";
  console.log(`🔌 mongoose 연결 상태: ${stateName}`);

  query = coerceQuery(query);

  const now = new Date(); // ✅ 현재 시각
  let threshold = new Date(now);
  if (Number.isFinite(days)) {
    threshold = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);
  } else {
    threshold.setMonth(
      threshold.getMonth() - (Number.isFinite(months) ? months : 1)
    );
  }

  const isSingle = hasIdQuery(query);
  console.log(`🧭 now=${now.toISOString()}`);
  console.log(
    `🧭 threshold=${threshold.toISOString()} (${
      Number.isFinite(days) ? `${days}일` : `${months}개월`
    } 기준)`
  );
  console.log(
    `🧭 대상: ${isSingle ? "단일(_id 지정)" : "전체"} | query=${JSON.stringify(
      query
    )}`
  );

  const cursor = ProductDetail.find(query)
    .select({ _id: 1, "sku_info.sil.pd": 1 })
    .lean()
    .cursor();

  let total = 0;
  let staleCount = 0;
  let keptCount = 0;
  const staleIds = [];

  console.time("⏱️ 스캔");
  for await (const doc of cursor) {
    total++;
    const recent = hasRecentPricePoint(doc, threshold);

    if (!recent) {
      staleIds.push(String(doc._id));
      staleCount++;
      if (verbose) {
        const newest = getNewestPointISO(doc);
        const points = countPricePoints(doc);
        console.log(
          `🟠 매칭(_id 수집): _id=${doc._id} | 포인트수=${points} | 최신=${
            newest ?? "없음"
          } | 기준>=${threshold.toISOString()}`
        );
      }
    } else {
      keptCount++;
      if (verbose && isSingle) {
        const newest = getNewestPointISO(doc);
        const points = countPricePoints(doc);
        console.log(
          `✔️ 제외(최근 존재): _id=${doc._id} | 포인트수=${points} | 최신=${
            newest ?? "없음"
          }`
        );
      }
    }

    if (progressEvery > 0 && total % progressEvery === 0) {
      console.log(
        `⏩ 진행: 처리=${total} | 매칭=${staleCount} | 제외=${keptCount}`
      );
    }
  }
  console.timeEnd("⏱️ 스캔");

  const result = {
    now: now.toISOString(),
    threshold: threshold.toISOString(),
    query,
    total,
    staleCount,
    keptCount,
    staleIds,
  };

  console.log("📊 요약:", {
    total,
    staleCount,
    keptCount,
    sample: staleIds.slice(0, 10),
  });

  // 결과 출력(두 형태)
  console.log("🧾 staleIds(JSON):", JSON.stringify(staleIds));
  if (staleIds.length) {
    console.log("🧾 staleIds(lines):");
    for (const id of staleIds) console.log(id);
  }

  if (disconnectAfter) {
    try {
      await mongoose.connection.close();
      console.log("🔌 mongoose 연결 종료");
    } catch (e) {
      console.warn("⚠️ 연결 종료 오류:", e?.message || e);
    }
  }

  console.timeEnd("⏱️ 전체 소요");
  console.log("✅ [END] find-stale-products");

  return result;
}

// ────────────────────────────────────────────
// 유틸: 문자열 query → {_id: "..."} 로 강제
function coerceQuery(q) {
  if (typeof q === "string" && q.trim()) return { _id: q.trim() };
  if (q && typeof q === "object") return q;
  return {};
}
function hasIdQuery(q) {
  return !!(q && Object.prototype.hasOwnProperty.call(q, "_id"));
}

// 유틸: pd(Map|Object)에 threshold 이상 t 존재 여부
function hasRecentPricePoint(doc, threshold) {
  const sil = doc?.sku_info?.sil || [];
  for (const sku of sil) {
    const pd = sku?.pd;
    if (!pd) continue;

    let values;
    if (pd instanceof Map) values = Array.from(pd.values());
    else if (pd && typeof pd === "object") values = Object.values(pd);
    else continue;

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

// 유틸: 최신 t ISO
function getNewestPointISO(doc) {
  let newest = null;
  const sil = doc?.sku_info?.sil || [];
  for (const sku of sil) {
    const pd = sku?.pd;
    if (!pd) continue;

    let values;
    if (pd instanceof Map) values = Array.from(pd.values());
    else if (pd && typeof pd === "object") values = Object.values(pd);
    else continue;

    for (const p of values) {
      const t = p?.t || p?.collected_at;
      if (!t) continue;
      const dt = new Date(t);
      if (!Number.isNaN(dt.valueOf()) && (!newest || dt > newest)) newest = dt;
    }
  }
  return newest ? newest.toISOString() : null;
}

// 유틸: 포인트 개수(로그용)
function countPricePoints(doc) {
  let count = 0;
  const sil = doc?.sku_info?.sil || [];
  for (const sku of sil) {
    const pd = sku?.pd;
    if (!pd) continue;

    if (pd instanceof Map) count += pd.size;
    else if (pd && typeof pd === "object") count += Object.values(pd).length;
  }
  return count;
}

// ────────────────────────────────────────────
// 직접 실행 예시(문자열로 _id 전달 가능)
main({ verbose: true, disconnectAfter: true }).catch((e) => {
  console.error("❌ 실행 오류:", e);
});
