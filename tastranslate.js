import translateText from "tastranslate";

const out = await translateText("Yes", "en", "ko");
console.log(out); // 번역된 영어 문자열
