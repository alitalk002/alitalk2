import { translateSkuPropertiesSimple } from "./skuTranslate.js";

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

function sortArrayOfObjectsStable(arr) {
  return arr
    .map((o) => deepSortObjectKeysKo(o))
    .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
}

function stripMinimal(s) {
  return String(s ?? "").replace(/[{}\[\]\(\)\"\s]/g, "");
}

// 비교용 정규화: 지정 특수문자 + 공백 제거
export function stripForCompare(s) {
  const raw = String(s ?? "");
  const translated = translateSkuPropertiesSimple(raw); // ← 먼저 치환

  // 치환 결과가 JSON이면 정렬/직렬화 후 strip, 아니면 문자열 strip
  if (typeof translated === "string") {
    try {
      const parsed = JSON.parse(translated);
      const arr = Array.isArray(parsed) ? parsed : [parsed];
      const sortedArr = sortArrayOfObjectsStable(arr);
      return stripMinimal(JSON.stringify(sortedArr));
    } catch {
      return stripMinimal(translated);
    }
  } else if (translated && typeof translated === "object") {
    const arr = Array.isArray(translated) ? translated : [translated];
    const sortedArr = sortArrayOfObjectsStable(arr);
    return stripMinimal(JSON.stringify(sortedArr));
  }
  return stripMinimal(translated);
}

// 3) c / sp 정규화

// c 필드 비교용 정규화
export function normalizeCForCompare(c) {
  return stripForCompare(c);
}

// sp 비교용 정규화
export function normalizeSpForCompare(spStr) {
  // 1) JSON 파싱 시도
  try {
    let arr = JSON.parse(spStr);
    if (!Array.isArray(arr)) arr = [arr];

    const trans = stripForCompare(spStr);

    // 3) 안정적 직렬화 후 strip
    const stable = JSON.stringify(trans);
    return stripForCompare(stable);
  } catch {
    // 파싱 불가 → 그냥 strip 규칙만 적용
    return stripForCompare(spStr);
  }
}
