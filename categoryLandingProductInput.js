import mongoose from "mongoose";
import ProductCategories from "./models/ProductCategories.js";
import ProductDetail from "./models/ProductDetail.js";
import dbConnect from "./utils/dbConnect.js";
import CategoryLandingProduct from "./models/CategoryLandingProduct.js";

// ── 기준: 현재로부터 4일
const FOUR_DAYS_MS = 4 * 24 * 60 * 60 * 1000;

function analyzePd(pdObj, start, end, productId) {
  if (!pdObj || typeof pdObj !== "object") {
    return {
      hasData: false,
      lowestSale: null,
      avgSale: null,
      latestSale: null,
      latestPoint: null,
      avgToCurrentDiscountPct: null,
      avgToCurrentDiscountPctRounded: null,
      isFlat: false,
    };
  }

  const parseDateKey = (k) => {
    if (typeof k !== "string") return null;

    if (/^\d{8}$/.test(k)) {
      const y = k.slice(0, 4);
      const m = k.slice(4, 6);
      const d = k.slice(6, 8);
      const dt = new Date(`${y}-${m}-${d}T00:00:00.000Z`);
      return Number.isNaN(dt.valueOf()) ? null : dt;
    }

    const dt = new Date(k);
    return Number.isNaN(dt.valueOf()) ? null : dt;
  };

  const getPointDate = (key, v) => {
    const byKey = parseDateKey(key);
    if (byKey) return byKey;

    if (v && (v.t || v.collected_at)) {
      const dt = new Date(v.t || v.collected_at);
      if (!Number.isNaN(dt.valueOf())) return dt;
    }
    return null;
  };

  let lowestSale = null;
  let latestPoint = null;
  const uniqueSales = new Set();
  let hasData = false;

  let sumSale = 0;
  let cntSale = 0;

  const entries =
    pdObj instanceof Map ? Array.from(pdObj.entries()) : Object.entries(pdObj);

  for (const [key, v] of entries) {
    if (!v) continue;

    const t = getPointDate(key, v);
    if (!t) continue;

    if (t < start || t >= end) continue;

    const sRaw = v.s ?? v.p;
    if (sRaw == null) continue;

    const s = Number(sRaw);
    if (!Number.isFinite(s)) continue;

    hasData = true;

    // 평균 계산용 누적
    sumSale += s;
    cntSale += 1;

    if (lowestSale == null || s < lowestSale) {
      lowestSale = s;
    }

    if (!latestPoint || t > latestPoint.t) {
      latestPoint = { ...v, t, s };
    }

    uniqueSales.add(s);
  }

  const latestSale = latestPoint?.s ?? null;
  const avgSale = hasData && cntSale > 0 ? sumSale / cntSale : null;

  // 평균가 대비 현재가 할인율(%)
  const avgToCurrentDiscountPct =
    avgSale != null &&
    avgSale > 0 &&
    latestSale != null &&
    cntSale >= 30 &&
    avgSale - latestSale >= 0
      ? ((avgSale - latestSale) / avgSale) * 100
      : 0;

  // 표시용(소수 1자리) - 필요 없으면 빼도 됨
  const avgToCurrentDiscountPctRounded =
    avgToCurrentDiscountPct == null
      ? null
      : Math.round(avgToCurrentDiscountPct * 10) / 10;

  const isFlat = hasData && uniqueSales.size <= 1;

  return {
    hasData,
    lowestSale,
    avgSale,
    latestSale,
    latestPoint,
    avgToCurrentDiscountPct,
    avgToCurrentDiscountPctRounded,
    isFlat,
  };
}

function getLatestPdTime(pd) {
  if (!pd) return null;

  const vals = pd instanceof Map ? Array.from(pd.values()) : Object.values(pd);
  let latest = null;

  // 1) PricePoint 값의 t 사용
  for (const v of vals) {
    const ts = v?.t ? Date.parse(v.t) : NaN;
    if (!Number.isNaN(ts)) latest = latest == null ? ts : Math.max(latest, ts);
  }

  // 2) 값들에 t가 없으면 키(날짜 문자열) 파싱
  if (latest == null) {
    const keys = pd instanceof Map ? Array.from(pd.keys()) : Object.keys(pd);
    for (const k of keys) {
      const ts = Date.parse(k);
      if (!Number.isNaN(ts))
        latest = latest == null ? ts : Math.max(latest, ts);
    }
  }

  return latest == null ? null : new Date(latest);
}

const toNum = (v) =>
  v == null ? NaN : Number(String(v).replace(/[^\d.-]/g, ""));

// pd(Map|Object) → PricePoint[] 로 통일
const pdEntries = (pd) => {
  if (!pd) return [];
  if (pd instanceof Map) return Array.from(pd.values());
  if (typeof pd === "object") return Object.values(pd);
  return [];
};

// 날짜가 기간 안인지
const inRange = (t, start, end) => {
  const tt = t ? new Date(t).getTime() : NaN;
  if (!Number.isFinite(tt)) return true; // 날짜 없으면 포함
  if (start && tt < new Date(start).getTime()) return false;
  if (end && tt > new Date(end).getTime()) return false;
  return true;
};

// 평균 "판매가" 계산: s(세일가) 우선, 없으면 p 사용
const avgSaleFromPd = (pd, start, end) => {
  const nums = pdEntries(pd)
    .filter((pp) => inRange(pp?.t, start, end))
    .map((pp) => toNum(pp?.s ?? pp?.p))
    .filter((n) => Number.isFinite(n) && n > 0);

  if (!nums.length) return null;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
};

function getRange(rangeParam) {
  const now = new Date();
  if (rangeParam === "calendarMonth") {
    const start = new Date(now.getFullYear(), now.getMonth(), 1);
    return { start, end: now, label: "calendarMonth" };
  }
  const end = now;
  const start = new Date(end.getTime() - 60 * 24 * 60 * 60 * 1000);
  return { start, end, label: "rolling30" };
}

async function getServerSideProps(ctx) {
  // 기간 계산

  // pd 분석: 기간 내 포인트, 최저/최신, flat 여부

  // { [key]: {p,s,t} } → [{p,s,t}, ...]

  // 기간 내 + s 존재

  await dbConnect();

  // const categoryList = [
  //   { categoryName: "음식", categoryId: "2" },
  //   { categoryName: "가전제품", categoryId: "6" },
  //   { categoryName: "소비자 가전", categoryId: "44" },
  //   { categoryName: "태블릿", categoryId: "200001086" },
  //   { categoryName: "이동 전화", categoryId: "5090301" },
  //   { categoryName: "노트북", categoryId: "702" },
  //   { categoryName: "문구", categoryId: "21" },
  //   { categoryName: "생활용품", categoryId: "13" },
  //   { categoryName: "뷰티/헬스", categoryId: "66" },
  //   { categoryName: "주방용품", categoryId: "200000920" },
  //   { categoryName: "남성의류", categoryId: "200000343" },
  //   { categoryName: "여성의류", categoryId: "200000345" },
  //   { categoryName: "신발", categoryId: "322" },
  //   { categoryName: "스포츠", categoryId: "18" },
  //   { categoryName: "완구/취미", categoryId: "26" },
  //   { categoryName: "자동차용품", categoryId: "34" },
  //   { categoryName: "안전/보안", categoryId: "30" },
  //   { categoryName: "조명", categoryId: "39" },
  // ];
  const categoryList = [
    { categoryName: "음식", categoryId: "2" },
    // { categoryName: "그래픽 카드", categoryId: "2" },
    { categoryName: "태블릿", categoryId: "2" },
    // { categoryName: "노트북", categoryId: "2" },
    // { categoryName: "데스크탑", categoryId: "2" },
    { categoryName: "마우스 및 키보드", categoryId: "2" },
    { categoryName: "LCD 모니터", categoryId: "2" },
    { categoryName: "스피커", categoryId: "2" },
    { categoryName: "SSD", categoryId: "2" },
    { categoryName: "램", categoryId: "2" },
    { categoryName: "PC 전원 공급 장치", categoryId: "2" }, // 파워서플라이
    { categoryName: "컴퓨터 케이스 및 타워", categoryId: "2" }, // 컴퓨터 케이스
    // { categoryName: "PC 팬 & 쿨링", categoryId: "2" }, // 쿨러
    { categoryName: "AIO CPU 액체 냉각", categoryId: "2" }, // 쿨러
    { categoryName: "낚시 미끼", categoryId: "2" }, // 쿨러
    { categoryName: "낚시바늘", categoryId: "2" }, // 쿨러
    { categoryName: "낚시 릴", categoryId: "2" }, // 쿨러
    { categoryName: "낚시 줄", categoryId: "2" }, // 쿨러
    { categoryName: "태클박스", categoryId: "2" }, // 쿨러
    { categoryName: "낚시대", categoryId: "2" }, // 쿨러
    { categoryName: "낚시 찌", categoryId: "2" }, // 쿨러
    { categoryName: "낚시 가방", categoryId: "2" }, // 쿨러
    // { categoryName: "CPU 팬 및 히트싱크", categoryId: "2" }, // 쿨러
    // { categoryName: "CPU 팬 및 히트싱크", categoryId: "2" }, // 쿨러
    // { categoryName: "CPU 팬 및 히트싱크", categoryId: "2" }, // 쿨러
    // { categoryName: "CPU 팬 및 히트싱크", categoryId: "2" }, // 쿨러
    // { categoryName: "CPU 팬 및 히트싱크", categoryId: "2" }, // 쿨러
    // { categoryName: "CPU 팬 및 히트싱크", categoryId: "2" }, // 쿨러
    // { categoryName: "CPU 팬 및 히트싱크", categoryId: "2" }, // 쿨러
  ];

  const { start, end, label: range } = getRange(undefined);

  // 1) 원문 조회

  for (let category of categoryList) {
    let raw;

    // const catDoc = await ProductCategories.findOne({
    //   cId: String(category.categoryId),
    // }).lean();
    // const cid = catDoc?._id?.toString();

    raw = await ProductDetail.find({
      $or: [
        { c1n: category.categoryName },
        { c2n: category.categoryName },
        { c3n: category.categoryName },
        { c4n: category.categoryName },
      ],
    }).lean();

    // if (!raw?.length) raw = await ProductDetail.find({ cId2: cid }).lean();

    console.log("raw:", raw.length);

    const allSkus = [];

    // 평균가대비 최저가 싼 리스트

    const offList = raw
      .map((doc) => {
        const sil = doc?.sku_info?.sil || [];

        const sku_filtered = sil
          .map((sku) => {
            const { lowestSale, latestSale, avgToCurrentDiscountPct } =
              analyzePd(sku?.pd, start, end, doc._id);

            if (!doc._id) return null;

            // 기간 내 포인트 없거나 flat 제거
            if (lowestSale == null || latestSale == null) return null;

            if (Number(avgToCurrentDiscountPct <= 0)) return null;

            // if (isFlat) return null;

            // 최신가가 기간 최저가와 같지 않으면 제거
            // if (Number(latestSale) !== Number(lowestSale)) return null;

            const latestPdAt = getLatestPdTime(sku?.pd);
            const now = new Date();
            const newerThan4d =
              latestPdAt &&
              now.getTime() - latestPdAt.getTime() <= FOUR_DAYS_MS;

            if (!newerThan4d) return null;

            // ★ 평균 판매가 계산
            const avgSale = avgSaleFromPd(sku?.pd, start, end);
            if (avgSale == null || !Number.isFinite(avgSale) || avgSale <= 0)
              return null;

            const latest = Number(latestSale);
            const ratio = avgToCurrentDiscountPct;

            // 상위 랭킹용 풀 컬렉션에 적재
            allSkus.push({
              pid: String(doc._id),
              _id: String(doc._id),
              sId: sku?.sId,
              link: sku?.link,
              c: sku?.c,
              sp: sku?.sp,
              cur: sku?.cur || "KRW",
              latestSale: latest,
              tt: doc?.tt,

              avgSale,
              ratio,
            });

            // 필요 시 제품 내부용 데이터 유지하려면 리턴 유지
            return {
              pid: String(doc._id),
              _id: String(doc._id),
              sId: sku?.sId,
              link: sku?.link,
              c: sku?.c,
              sp: sku?.sp,
              cur: sku?.cur || "KRW",
              pd: sku?.pd || {},
              latest_sale: latest,
              avg_sale: avgSale,
              ratio,
            };
          })
          .filter(Boolean);

        if (sku_filtered.length === 0) return null;

        return {
          _id: doc._id,
          // 필요하면 sku_filtered를 보존:
          // sku_info: sku_filtered,
        };
      })
      .filter(Boolean);

    // 리뷰 많은 순서 리스트

    const rnList = raw
      .map((doc) => {
        const sil = doc?.sku_info?.sil || [];

        const sku_filtered = sil
          .map((sku) => {
            const { lowestSale, latestSale, isFlat } = analyzePd(
              sku?.pd,
              start,
              end,
            );

            // 기존 조건
            if (lowestSale == null || latestSale == null) return null;
            if (isFlat) return null;

            // 추가 조건: 현재 기준 4일 이내에 업데이트 되었는지 체크
            const latestPdAt = getLatestPdTime(sku?.pd);
            const now = new Date();
            const newerThan4d =
              latestPdAt &&
              now.getTime() - latestPdAt.getTime() <= FOUR_DAYS_MS;

            if (!newerThan4d) return null;

            return { sId: sku?.sId };
          })
          .filter(Boolean);

        if (sku_filtered.length === 0) return null;

        return {
          _id: doc._id,
          // 필요하면 sku_filtered를 보존:
          rn: doc.rn,
        };
      })
      .filter(Boolean);

    // 판매순 많은 순서 리스트

    const volList = raw
      .map((doc) => {
        const sil = doc?.sku_info?.sil || [];

        const sku_filtered = sil
          .map((sku) => {
            const { lowestSale, latestSale, isFlat } = analyzePd(
              sku?.pd,
              start,
              end,
            );

            // 기존 조건
            if (lowestSale == null || latestSale == null) return null;
            if (isFlat) return null;

            // 추가 조건: 현재 기준 4일 이내에 업데이트 되었는지 체크
            const latestPdAt = getLatestPdTime(sku?.pd);
            const now = new Date();
            const newerThan4d =
              latestPdAt &&
              now.getTime() - latestPdAt.getTime() <= FOUR_DAYS_MS;

            if (!newerThan4d) return null;

            return { sId: sku?.sId };
          })
          .filter(Boolean);

        if (sku_filtered.length === 0) return null;

        return {
          _id: doc._id,
          // 필요하면 sku_filtered를 보존:
          vol: doc.vol,
        };
      })
      .filter(Boolean);
    // 평점 높은 순서 리스트

    // ── psList 생성: 최신 pd가 '현재 기준 4일 이내'만 통과
    // ── psList 생성: 최신 pd가 '현재 기준 4일 이내'만 통과
    const psList = raw
      .map((doc) => {
        const sil = doc?.sku_info?.sil || [];

        const sku_filtered = sil
          .map((sku) => {
            const { lowestSale, latestSale, isFlat } = analyzePd(
              sku?.pd,
              start,
              end,
            );

            // 기존 조건
            if (lowestSale == null || latestSale == null) return null;
            if (isFlat) return null;

            // 추가 조건: 현재 기준 4일 이내에 업데이트 되었는지 체크
            const latestPdAt = getLatestPdTime(sku?.pd);
            const now = new Date();
            const newerThan4d =
              latestPdAt &&
              now.getTime() - latestPdAt.getTime() <= FOUR_DAYS_MS;

            if (!newerThan4d) return null;

            return { sId: sku?.sId };
          })
          .filter(Boolean);

        if (sku_filtered.length === 0) return null;

        return {
          _id: doc._id,
          ps: doc.ps,
          // sku: sku_filtered, // 필요하면 주석 해제
        };
      })
      .filter(Boolean);

    const psTop20 = psList
      .sort((a, b) => {
        // console.log("b:", b);
        return b.ps - a.ps;
      })

      .slice(0, 20)
      .map((item) => {
        return item._id;
      });
    const volTop20 = volList
      .sort((a, b) => b.vol - a.vol)
      .slice(0, 20)

      .map((item) => {
        return item._id;
      });
    const rnTop20 = rnList
      .sort((a, b) => b.rn - a.rn)
      .slice(0, 20)
      .map((item) => {
        // console.log("item:", item);
        return item._id;
      });

    // 할인탑 100 중복검사 코드

    const offTop20 = [];
    const seen = new Set();

    for (const item of allSkus.sort((a, b) => b.ratio - a.ratio)) {
      // 1) 저장에 쓸 product 확정(옵션 B: ProductDetail의 _id가 오길 기대)
      const product = item.productId ?? item._id ?? item.pid;
      if (!product) continue; // 필수값 없으면 스킵

      // 2) 동일 기준으로 중복 체크(문자열화 통일)
      const key =
        product?.toHexString?.() ?? product?.toString?.() ?? String(product);
      if (seen.has(key)) continue;
      seen.add(key);

      offTop20.push({
        product, // ← 중복키와 동일한 값으로 저장
        sId: item.sId ?? null,
        c: item.c ?? null,
        sp: item.sp,
        ratio: item.ratio,
        tt: item.tt,
      });

      if (offTop20.length === 20) break; // 100개에서 종료
    }

    console.log("offTop20", offTop20.slice(0, 20));

    const res = await CategoryLandingProduct.updateOne(
      { categoryName: category.categoryName },
      {
        $set: {
          rnList: rnTop20,
          volList: volTop20,
          psList: psTop20,
          offList: offTop20,
        },
        $setOnInsert: { categoryName: category.categoryName }, // 문서 없으면 생성 시 이름도 세팅
      },
      { runValidators: true, upsert: true }, // 유효성검사 + 없으면 생성
    );

    // console.log("updateOne result:", res); // matchedCount/modifiedCount 확인

    // 상품 정렬: 대표 최저가 오름차순 → 리뷰수 rn 내림차순
  }

  process.exit(0);
}

async function test() {
  await dbConnect();
  const res = await CategoryLandingProduct.find({
    categoryName: "음식",
  })
    .populate({
      path: "rnList", // 문자열 ref 배열
      model: "ProductDetail",
    })
    .lean();

  console.log("res:", res[0].rnList);
}
// test();

getServerSideProps();
