// utils/skuTranslate.simple.js (ESM)
// ✅ VALUE_MAP "한 곳"만 유지하면, 키/값 모두 여기 기준으로 치환됩니다.

export const VALUE_MAP = {
  // ── 검정
  black: "검정",
  "jet black": "검정",
  "matte black": "검정",
  blk: "검정",
  bk: "검정",
  블랙: "검정",
  검정: "검정",
  검정색: "검정",
  검은색: "검정",

  // ── 흰색
  white: "흰색",
  "pure white": "흰색",
  "snow white": "흰색",
  "off white": "흰색",
  화이트: "흰색",
  흰색: "흰색",
  백색: "흰색",
  백: "흰색",
  하양: "흰색",
  하양색: "흰색",
  하얀: "흰색",
  하얀색: "흰색",
  화이트색: "흰색",

  // ── 회색
  gray: "회색",
  grey: "회색",
  "light gray": "회색",
  "dark gray": "회색",
  charcoal: "회색",
  slate: "회색",
  ash: "회색",
  그레이: "회색",
  그레이색: "회색",
  회색: "회색",
  연회색: "회색",
  진회색: "회색",

  // ── 빨강
  red: "빨강",
  crimson: "빨강",
  scarlet: "빨강",
  ruby: "빨강",
  maroon: "빨강",
  burgundy: "빨강",
  wine: "빨강",
  부르고뉴: "빨강",
  레드: "빨강",
  레드색: "빨강",
  빨강: "빨강",
  빨간색: "빨강",
  와인: "빨강",
  버건디: "빨강",

  "light red": "밝은 빨강",
  "라이트 빨강": "밝은 빨강",
  lightred: "밝은 빨강",
  "다크 빨강": "어두운 빨강",
  darkred: "어두운 빨강",

  // ── 주황
  orange: "주황",
  tangerine: "주황",
  apricot: "주황",
  coral: "주황",
  salmon: "주황",
  오렌지: "주황",
  주황: "주황",
  주황색: "주황",
  코랄: "주황",
  살몬: "주황",

  "light orange": "밝은 주황",
  "라이트 오렌지": "밝은 주황",
  lightorange: "밝은 주황",
  "dark orange": "어두운 주황",
  "다크 오렌지": "어두운 주황",
  darkorange: "어두운 주황",

  // ── 노랑
  yellow: "노랑",
  amber: "노랑",
  mustard: "노랑",
  lemon: "노랑",
  "golden yellow": "노랑",
  옐로: "노랑",
  옐로우: "노랑",
  노랑: "노랑",
  노란색: "노랑",
  머스타드: "노랑",

  lightyellow: "밝은 노랑",
  "light yellow": "밝은 노랑",
  "라이트 옐로우": "밝은 노랑",
  darkyellow: "어두운 노랑",
  "dark yellow": "어두운 노랑",
  "다크 옐로우": "어두운 노랑",

  // ── 초록
  green: "초록",
  "forest green": "초록",
  lime: "초록",
  olive: "초록",
  "grass green": "초록",
  "yellow green": "초록",
  그린: "초록",
  초록: "초록",
  초록색: "초록",
  올리브: "초록",
  올리브색: "초록",
  라임: "초록",
  라임색: "초록",

  "라이트 그린": "밝은 초록색",
  "light green": "밝은 초록색",
  lightgreen: "밝은 초록색",
  darkgreen: "어두운 초록색",
  "dark green": "어두운 초록색",
  "다크 그린": "어두운 초록색",

  // ── 청록
  mint: "청록",
  teal: "청록",
  turquoise: "청록",
  aqua: "청록",
  cyan: "청록",
  민트: "청록",
  민트색: "청록",
  틸: "청록",
  터키석: "청록",
  아쿠아: "청록",
  아쿠아색: "청록",
  시아노: "청록",
  청록: "청록",
  청록색: "청록",

  // ── 파랑
  blue: "파랑",
  "royal blue": "파랑",
  "sky blue": "파랑",
  cobalt: "파랑",
  sapphire: "파랑",
  블루: "파랑",
  파랑: "파랑",
  파란: "파랑",
  파란색: "파랑",
  파랑색: "파랑",
  하늘색: "파랑",
  코발트: "파랑",
  코발트색: "파랑",

  "라이트 블루": "밝은 파랑",
  "light blue": "밝은 파랑",
  lightblue: "밝은 파랑",
  darkblue: "어두운 파랑",
  "dark blue": "어두운 파랑",
  "다크 블루": "어두운 파랑",

  // ── 남색
  navy: "남색",
  indigo: "남색",
  네이비: "남색",
  네이비색: "남색",
  인디고: "남색",
  인디고색: "남색",
  남색: "남색",

  // ── 보라
  purple: "보라",
  violet: "보라",
  lilac: "보라",
  lavender: "보라",
  plum: "보라",
  퍼플: "보라",
  퍼플색: "보라",
  바이올렛: "보라",
  바이올렛색: "보라",
  라벤더: "보라",
  라벤더색: "보라",
  보라: "보라",
  보라색: "보라",

  "라이트 보라": "밝은 보라",
  "light purple": "밝은 보라",
  lightpurple: "밝은 보라",
  darkpurple: "어두운 보라",
  "dark purple": "어두운 보라",
  "다크 보라": "어두운 보라",

  // ── 분홍
  pink: "분홍",
  fuchsia: "분홍",
  magenta: "분홍",
  "hot pink": "진한 분홍",
  rose: "분홍",
  핑크: "분홍",
  분홍: "분홍",
  분홍색: "분홍",
  로즈: "분홍",
  로즈색: "분홍",
  핫핑크: "분홍",
  핫핑크색: "분홍",

  "라이트 분홍": "밝은 분홍",
  "light pink": "밝은 분홍",
  lightpink: "밝은 분홍",
  darkpink: "어두운 분홍",
  "dark pink": "어두운 분홍",
  "다크 분홍": "어두운 분홍",

  // ── 갈색
  brown: "갈색",
  chocolate: "갈색",
  coffee: "갈색",
  caramel: "갈색",
  mahogany: "갈색",
  브라운: "갈색",
  브라운색: "갈색",
  갈색: "갈색",
  밤색: "갈색",
  카멜: "갈색",
  카멜색: "갈색",
  코코아: "갈색",
  코코아색: "갈색",

  "라이트 갈색": "밝은 갈색",
  "light brown": "밝은 갈색",
  lightbrown: "밝은 갈색",
  darkbrown: "어두운 갈색",
  "dark brown": "어두운 갈색",
  "다크 갈색": "어두운 갈색",

  // ── 베이지
  beige: "베이지",
  tan: "베이지",
  khaki: "베이지",
  sand: "베이지",
  ivory: "베이지",
  cream: "베이지",
  베이지: "베이지",
  베이지색: "베이지",
  아이보리: "베이지",
  아이보리색: "베이지",
  크림: "베이지",
  크림색: "베이지",
  샌드: "베이지",
  샌드색: "베이지",
  카키: "베이지",
  카키색: "베이지",

  "라이트 베이지": "밝은 베이지",
  "light beige": "밝은 베이지",
  lightbeige: "밝은 베이지",
  darkbeige: "어두운 베이지",
  "dark beige": "어두운 베이지",
  "다크 베이지": "어두운 베이지",

  // ── 메탈릭
  금: "금색",
  gold: "금색",
  golden: "금색",
  골드: "금색",
  금색: "금색",

  silver: "은색",
  은: "은색",
  실버: "은색",
  은색: "은색",

  // ── 투명
  transparent: "투명",
  clear: "투명",
  투명: "투명",
  투명색: "투명",

  "no box": "no box",

  // 일상 단어
  그렇습니다: "예",
  그러합니다: "예",
  그래요: "예",
  yes: "예",
  아닙니다: "아니요",
  아니에요: "아니요",
  아니오: "아니요",
  no: "아니요",

  // 플러그/지역 표기(일반화)
  us: "미국",
  eu: "유럽",

  // 키 단어가 값에 섞여 들어온 케이스 방지용
  Color: "색상",
  색깔: "색상",
  색: "색상",

  other: "기타",
};

// ────────────────────────────────────────────────────────────────
// 유틸
const escapeRegExp = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const toNFC = (x) => (typeof x === "string" ? x.normalize("NFC") : x);

/**
 * VALUE_MAP으로부터 두 종류의 규칙 생성:
 *  - enRules: 영문(또는 숫자 포함 라틴) 키는 단어경계 + 공백/하이픈 허용 (gi)
 *  - koExactRules: 한글 포함 키는 "전체 일치"만 치환 (g 금지)
 */
function buildRules(map) {
  const keys = Object.keys(map).sort((a, b) => b.length - a.length); // 긴 키 우선
  const enRules = [];
  const koExactRules = [];

  for (const rawKey of keys) {
    const k = toNFC(rawKey);
    const target = map[rawKey];
    const hasLatin = /[A-Za-z0-9]/.test(k);
    const hasHangul = /[가-힣]/.test(k);

    if (hasLatin && !hasHangul) {
      // 예: "sky blue" → /\bsky[\s\-]+blue\b/gi
      const tokens = k
        .trim()
        .replace(/\s*-\s*/g, "-")
        .split(/[\s-]+/)
        .map(escapeRegExp);
      const pat = `\\b${tokens.join("[\\s\\-]+")}\\b`;
      enRules.push([new RegExp(pat, "gi"), target]);
    } else {
      // 한글/기타는 전체 일치만 (lastIndex 문제 없도록 g 미사용)
      koExactRules.push([new RegExp(`^${escapeRegExp(k)}$`), target]);
    }
  }
  return { enRules, koExactRules };
}

const { enRules, koExactRules } = buildRules(VALUE_MAP);

/** 문자열 하나를 VALUE_MAP 규칙으로 치환 */
function replaceInString(str) {
  if (typeof str !== "string") return str;
  let out = toNFC(str);

  // 1) 영문 토큰 기반 치환 (부분 일치 허용: 단어경계/하이픈/공백)
  for (const [re, rep] of enRules) out = out.replace(re, rep);

  // 2) 한글 등 exact 치환 (전체 일치일 때만)
  for (const [re, rep] of koExactRules) {
    if (re.test(out)) {
      out = out.replace(re, rep);
      break;
    }
  }
  return out;
}

/**
 * 객체/배열/원시 전역 치환
 *  - 키도 replaceInString으로 치환
 *  - 값도 재귀적으로 치환
 *  - 키 충돌 시 마지막 값을 우선(필요하면 병합 로직으로 확장 가능)
 */
function replaceEverywhere(data) {
  if (Array.isArray(data)) {
    return data.map(replaceEverywhere);
  }
  if (data && typeof data === "object") {
    const res = {};
    for (const [k, v] of Object.entries(data)) {
      const newKey = replaceInString(k);
      const newVal = replaceEverywhere(v);
      res[newKey] = newVal; // 충돌 시 덮어씀
    }
    return res;
  }
  return replaceInString(data); // 문자열/그 외
}

/**
 * 진입점:
 *  - 문자열이면 JSON.parse 시도 → 성공 시 구조치환 → 다시 문자열로
 *  - 파싱 실패 시에는 "그 문자열 자체"만 토큰 치환
 *  - 비문자(객체/배열)면 구조치환 후 원형 반환
 */
export function translateSkuPropertiesSimple(input) {
  const isString = typeof input === "string";

  if (isString) {
    const raw = input.trim();
    try {
      const parsed = JSON.parse(raw);
      const transformed = replaceEverywhere(parsed);
      return JSON.stringify(transformed);
    } catch {
      // JSON 아니면 문자열만 치환
      return replaceInString(input);
    }
  }

  // 객체/배열이면 구조 치환
  return replaceEverywhere(input);
}

// ────────────────────────────────────────────────────────────────
// 사용 예시
// const sp = `[{"색깔":"NO  WHITE","플러그 유형":"us"}]`;
// console.log(translateSkuPropertiesSimple(sp));
// -> 키 "색깔" → "색상", 값 "NO  WHITE" → (VALUE_MAP에 매핑되어 있으면) "흰색", "us" → "미국"
