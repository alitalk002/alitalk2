// utils/skuTranslate.simple.js  (ESM)

const KEY_MAP = {
  Color: "색상",
  "Remote Control": "리모컨",
  Size: "사이즈",
  // 필요시 추가...
};

// 값 매핑은 소문자 기준으로 단순 매칭
// VALUE_MAP: 색상 동의어 → 대표색(16종) 통일
const VALUE_MAP = {
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

  // ── 청록(민트/틸/시아노/터키석/아쿠아 포함)
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
  파랑색: "파랑",
  파란색: "파랑",
  하늘색: "파랑",
  코발트: "파랑",
  코발트색: "파랑",

  // ── 남색(네이비/인디고)
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

  // ── 분홍
  pink: "분홍",
  fuchsia: "분홍",
  magenta: "분홍",
  "hot pink": "분홍",
  rose: "분홍",
  핑크: "분홍",
  분홍: "분홍",
  분홍색: "분홍",
  로즈: "분홍",
  로즈색: "분홍",
  핫핑크: "분홍",
  핫핑크색: "분홍",

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

  // ── 베이지(아이보리/크림/탠/카키톤/샌드)
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

  // ── 금색/은색(메탈릭)
  금: "금색",
  gold: "금색",
  golden: "금색",
  골드: "금색",
  금색: "금색",
  silver: "은색",
  은: "은색",
  실버: "은색",
  은색: "은색",

  // ── 기타(투명)
  transparent: "투명",
  clear: "투명",
  투명: "투명",
  투명색: "투명",

  // 일상단어

  그렇습니다: "예",
  그래요: "예",
  yes: "예",

  아닙니다: "아니요",
  아니오: "아니요",
  no: "아니요",

  us: "미국",
  eu: "영국",
  Color: "색상",
  색깔: "색상",
  색: "색상",
};

// 2) 유틸
const escapeRegExp = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

// 영어 키는 \b 경계 + 공백/하이픈 허용, 한글은 그대로 매칭
function buildRules(map) {
  const keys = Object.keys(map).sort((a, b) => b.length - a.length); // 긴 것 우선
  const enRules = [];
  const koExactRules = [];

  for (const k of keys) {
    const target = map[k];
    const hasLatin = /[A-Za-z]/.test(k);
    const hasHangul = /[가-힣]/.test(k);

    if (hasLatin && !hasHangul) {
      // 예: "sky blue" -> /\bsky[\s\-]+blue\b/gi
      const tokens = k
        .trim()
        .replace(/\s*-\s*/g, "-")
        .split(/[\s-]+/)
        .map(escapeRegExp);
      const pat = `\\b${tokens.join("[\\s\\-]+")}\\b`;
      enRules.push([new RegExp(pat, "gi"), target]);
    } else {
      // 한글은 "전체 일치"만
      koExactRules.push([new RegExp(`^${escapeRegExp(k)}$`, "g"), target]);
    }
  }
  return { enRules, koExactRules };
}

const { enRules, koExactRules } = buildRules(VALUE_MAP);

function replaceByDictInString(str) {
  if (typeof str !== "string") return str;
  let out = str;

  // 1) 영문 토큰 치환(부분 일치 허용: 경계 기반)
  for (const [re, rep] of enRules) out = out.replace(re, rep);

  // 2) 한글은 "정확히 동일할 때만" 치환
  for (const [re, rep] of koExactRules) {
    if (re.test(out)) {
      // 전체 일치하면 교체
      out = out.replace(re, rep);
      break; // 한 번 치환했으면 종료 (과잉 치환 방지)
    }
  }
  return out;
}

// 3) 키/값/중첩 전부 치환
function replaceEverywhere(data) {
  if (Array.isArray(data)) return data.map(replaceEverywhere);
  if (data && typeof data === "object") {
    const res = {};
    for (const [k, v] of Object.entries(data)) {
      const newKey = replaceByDictInString(k); // ← 키 치환
      res[newKey] = replaceEverywhere(v); // ← 값(재귀)
    }
    return res;
  }
  return replaceByDictInString(data); // ← 문자열/그 외
}

// 4) 진입점: 문자열(JSON)이면 파싱해서 처리 후, 입력이 문자열이었다면 다시 문자열로
export function translateSkuPropertiesSimple(skuProperties) {
  const isString = typeof skuProperties === "string";
  let data = skuProperties;

  if (isString) {
    try {
      data = JSON.parse(skuProperties);
    } catch {
      /* 그대로 처리 */
    }
  }

  const transformed = replaceEverywhere(data);

  return isString && typeof transformed !== "string"
    ? JSON.stringify(transformed)
    : transformed;
}
