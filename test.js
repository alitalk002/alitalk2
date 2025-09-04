// filename: extractSkuModule.js
// 실행: node extractSkuModule.js "https://www.aliexpress.com/item/1005009312385695.html"
// 또는: node extractSkuModule.js (아래 기본 URL 사용)
import axios from "axios";
import { JSDOM } from "jsdom";

// ── 입력 URL ───────────────────────────────────────────────
const inputUrl =
  process.argv[2] ||
  "https://ko.aliexpress.com/item/1005003822846099.html?spm=a2g0o.productlist.main.29.3e243b42gGgFOZ&algo_pvid=62788ffa-846e-4a5f-9f03-8dbb547cefcd&algo_exp_id=62788ffa-846e-4a5f-9f03-8dbb547cefcd-28&pdp_ext_f=%7B%22order%22%3A%22384%22%2C%22eval%22%3A%221%22%7D&pdp_npi=6%40dis%21KRW%2125440%2113928%21%21%2117.68%219.68%21%40212e508d17557896999285116ea03a%2112000037630172786%21sea%21KR%216205341300%21ABX%211%210%21n_tag%3A-29910%3Bd%3A76142d02%3Bm03_new_user%3A-29895%3BpisId%3A5000000176755829&curPageLogUid=D3wor8u49Kgf&utparam-url=scene%3Asearch%7Cquery_from%3A%7Cx_object_id%3A1005003822846099%7C_p_origin_prod%3A";

// ── 유틸: 특정 노드에서 "직계 div 자식들" 중 index번째를 골라 내려가는 함수 ──
// steps는 각 단계에서 선택할 div의 인덱스(0-base). null이면 첫 번째 div를 의미.
function descendByDivIndices(root, steps) {
  let cur = root;
  for (let i = 0; i < steps.length; i++) {
    if (!cur) return null;
    const divChildren = Array.from(cur.querySelectorAll(":scope > div"));
    const pick = steps[i] ?? 0; // null/undefined면 0으로
    cur = divChildren[pick];
  }
  return cur;
}

async function main() {
  // 일부 페이지는 UA 없으면 빈껍데기 HTML이 올 수 있어 헤더 지정
  const res = await axios.get(inputUrl, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "ko,en;q=0.9",
    },
    // 리다이렉트 따라가기
    maxRedirects: 5,
    timeout: 30000,
  });

  const dom = new JSDOM(res.data);
  const document = dom.window.document;

  console.log("document:", res.data);

  // 1) .pdp-info-right 찾기
  const infoRight = document.querySelector(".pdp-info-right");
  if (!infoRight) {
    console.log("결과: .pdp-info-right 를 찾지 못함");
    return;
  }

  // 2) 7번째 자식 요소(1-base) → children[6] (0-base)
  const seventhChild = infoRight.children[6];
  if (!seventhChild) {
    console.log("결과: .pdp-info-right 의 7번째 자식 요소가 없음");
    return;
  }

  // 3) 경로 해석
  //    ".div  .div[0]  .div[1]  .div  .div  .div  .div"
  //    → 인덱스 시퀀스: [0, 0, 1, 0, 0, 0, 0]
  //    (첫 '.div'도 'div 자식 중 0번째'로 해석)
  const steps = [0, 0, 1, 0, 0, 0, 0];

  // 디버깅 로그: 각 단계에서 몇 개의 div 자식이 있는지 출력
  let cursor = seventhChild;
  for (let i = 0; i < steps.length; i++) {
    const divChildren = Array.from(cursor.querySelectorAll(":scope > div"));
    console.log(
      `[단계 ${i}] 현재 노드 직계 div 자식 수: ${divChildren.length}, 선택 인덱스: ${steps[i]}`
    );
    cursor = divChildren[steps[i]];
    if (!cursor) {
      console.log(`결과: 단계 ${i}에서 경로가 끊김 (요소 없음)`);
      return;
    }
  }

  // 4) 최종 노드의 "자식 요소 개수"
  const childCount = cursor.children.length;
  console.log(`결과: 최종 노드의 자식 요소 개수 = ${childCount}`);
}

main().catch((e) => {
  console.error("에러:", e.message);
});
