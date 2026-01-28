// seedSynonyms.js

import dbConnect from "./utils/dbConnect.js";
import mongoose from "mongoose";
// 위에서 드린 데이터를 여기에 변수로 넣습니다.
const synonymsData = [
  // ... (아까 복사한 200개 JSON 데이터를 여기에 붙여넣으세요) ...
  // 예시:

  // ─────────────────────────────────────────────
  // 1. 정육 / 축산물 (Meat & Livestock) - 부위별 상세
  // ─────────────────────────────────────────────
  { mappingType: "equivalent", synonyms: ["고기", "육류", "정육", "meat"] },
  {
    mappingType: "equivalent",
    synonyms: ["소고기", "한우", "비프", "우육", "beef", "쇠고기"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["돼지고기", "돈육", "포크", "한돈", "pork"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["닭고기", "계육", "치킨", "닭", "chicken", "poultry", "통닭"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["양고기", "양갈비", "램", "lamb", "mutton"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["오리고기", "오리", "덕", "duck", "훈제오리"],
  },

  // 소고기 부위
  { mappingType: "equivalent", synonyms: ["등심", "꽃등심", "sirloin"] },
  { mappingType: "equivalent", synonyms: ["안심", "tenderloin"] },
  { mappingType: "equivalent", synonyms: ["채끝", "채끝살", "striploin"] },
  { mappingType: "equivalent", synonyms: ["차돌박이", "차돌", "brisket"] },
  {
    mappingType: "equivalent",
    synonyms: ["갈비", "소갈비", "ribs", "short ribs"],
  },
  { mappingType: "equivalent", synonyms: ["우삼겹", "업진살"] },
  { mappingType: "equivalent", synonyms: ["양지", "국거리"] },
  { mappingType: "equivalent", synonyms: ["사골", "우족", "잡뼈"] },
  { mappingType: "equivalent", synonyms: ["육회", "육사시미"] },

  // 돼지고기 부위
  {
    mappingType: "equivalent",
    synonyms: ["삼겹살", "삼겹", "대패삼겹살", "pork belly"],
  },
  { mappingType: "equivalent", synonyms: ["목살", "목심", "pork neck"] },
  { mappingType: "equivalent", synonyms: ["항정살", "천겹살"] },
  { mappingType: "equivalent", synonyms: ["갈매기살"] },
  { mappingType: "equivalent", synonyms: ["등갈비", "쪽갈비", "back ribs"] },
  { mappingType: "equivalent", synonyms: ["앞다리살", "전지"] },
  { mappingType: "equivalent", synonyms: ["뒷다리살", "후지"] },
  { mappingType: "equivalent", synonyms: ["족발", "미니족"] },

  // 닭고기 부위/가공
  {
    mappingType: "equivalent",
    synonyms: ["닭가슴살", "닭가슴", "chicken breast"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["닭다리", "북채", "chicken drumstick"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["닭날개", "윙", "봉", "chicken wing"],
  },
  { mappingType: "equivalent", synonyms: ["닭볶음탕용", "볶음탕용"] },
  { mappingType: "equivalent", synonyms: ["백숙용", "삼계탕용"] },

  // 내장/특수부위
  { mappingType: "equivalent", synonyms: ["곱창", "소곱창", "돼지곱창"] },
  { mappingType: "equivalent", synonyms: ["대창", "소대창"] },
  { mappingType: "equivalent", synonyms: ["막창", "소막창", "돼지막창"] },
  { mappingType: "equivalent", synonyms: ["순대", "토종순대", "찰순대"] },
  { mappingType: "equivalent", synonyms: ["간", "천엽"] },

  // 가공육
  { mappingType: "equivalent", synonyms: ["햄", "스팸", "리챔", "ham"] },
  {
    mappingType: "equivalent",
    synonyms: ["소시지", "소세지", "비엔나", "후랑크", "sausage"],
  },
  { mappingType: "equivalent", synonyms: ["베이컨", "bacon"] },
  { mappingType: "equivalent", synonyms: ["돈까스", "돈가스", "pork cutlet"] },
  { mappingType: "equivalent", synonyms: ["떡갈비", "너비아니"] },
  { mappingType: "equivalent", synonyms: ["육포", "beef jerky"] },

  // ─────────────────────────────────────────────
  // 2. 수산물 / 해산물 (Seafood)
  // ─────────────────────────────────────────────
  { mappingType: "equivalent", synonyms: ["생선", "물고기", "어류", "fish"] },
  {
    mappingType: "equivalent",
    synonyms: ["해산물", "해물", "수산물", "seafood"],
  },
  { mappingType: "equivalent", synonyms: ["회", "사시미", "sashimi", "횟감"] },

  // 어류 상세
  { mappingType: "equivalent", synonyms: ["고등어", "자반고등어", "mackerel"] },
  { mappingType: "equivalent", synonyms: ["갈치", "은갈치", "먹갈치"] },
  {
    mappingType: "equivalent",
    synonyms: ["연어", "생연어", "훈제연어", "salmon"],
  },
  { mappingType: "equivalent", synonyms: ["참치", "튜나", "마구로", "tuna"] },
  { mappingType: "equivalent", synonyms: ["광어", "넙치"] },
  { mappingType: "equivalent", synonyms: ["우럭", "조피볼락"] },
  { mappingType: "equivalent", synonyms: ["조기", "굴비", "참조기"] },
  {
    mappingType: "equivalent",
    synonyms: ["장어", "민물장어", "바다장어", "eel"],
  },
  { mappingType: "equivalent", synonyms: ["삼치"] },
  { mappingType: "equivalent", synonyms: ["가자미"] },
  { mappingType: "equivalent", synonyms: ["대구", "cod"] },
  {
    mappingType: "equivalent",
    synonyms: ["동태", "명태", "생태", "코다리", "황태", "북어"],
  }, // 명태의 다양한 이름들

  // 갑각류/연체류
  {
    mappingType: "equivalent",
    synonyms: ["새우", "쉬림프", "대하", "칵테일새우", "shrimp", "prawn"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["게", "꽃게", "대게", "킹크랩", "crab"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["랍스터", "로브스터", "바닷가재", "lobster"],
  },
  { mappingType: "equivalent", synonyms: ["오징어", "squid", "calamari"] },
  { mappingType: "equivalent", synonyms: ["문어", "자숙문어", "octopus"] },
  { mappingType: "equivalent", synonyms: ["낙지", "산낙지"] },
  { mappingType: "equivalent", synonyms: ["쭈꾸미", "주꾸미"] },

  // 패류/해조류
  { mappingType: "equivalent", synonyms: ["조개", "패류", "clam"] },
  { mappingType: "equivalent", synonyms: ["전복", "abalone"] },
  { mappingType: "equivalent", synonyms: ["굴", "석화", "oyster"] },
  { mappingType: "equivalent", synonyms: ["홍합"] },
  { mappingType: "equivalent", synonyms: ["바지락"] },
  { mappingType: "equivalent", synonyms: ["가리비"] },
  {
    mappingType: "equivalent",
    synonyms: ["김", "조미김", "구운김", "laver", "seaweed"],
  },
  { mappingType: "equivalent", synonyms: ["미역", "자른미역"] },
  { mappingType: "equivalent", synonyms: ["다시마"] },

  // 건어물
  { mappingType: "equivalent", synonyms: ["건어물", "마른반찬"] },
  {
    mappingType: "equivalent",
    synonyms: ["멸치", "잔멸치", "국물멸치", "anchovy"],
  },
  { mappingType: "equivalent", synonyms: ["오징어채", "진미채", "일미"] },
  { mappingType: "equivalent", synonyms: ["쥐포"] },
  { mappingType: "equivalent", synonyms: ["황태채", "북어채"] },

  // ─────────────────────────────────────────────
  // 3. 채소 / 야채 (Vegetables)
  // ─────────────────────────────────────────────
  {
    mappingType: "equivalent",
    synonyms: ["채소", "야채", "vegetable", "veggie"],
  },
  { mappingType: "equivalent", synonyms: ["쌈채소", "쌈"] },
  { mappingType: "equivalent", synonyms: ["샐러드", "샐러드야채", "salad"] },

  // 잎채소/줄기채소
  {
    mappingType: "equivalent",
    synonyms: ["배추", "알배기", "알배추", "napa cabbage"],
  },
  { mappingType: "equivalent", synonyms: ["양배추", "cabbage"] },
  {
    mappingType: "equivalent",
    synonyms: ["상추", "꽃상추", "청상추", "lettuce"],
  },
  { mappingType: "equivalent", synonyms: ["깻잎"] },
  { mappingType: "equivalent", synonyms: ["시금치", "spinach"] },
  {
    mappingType: "equivalent",
    synonyms: ["파", "대파", "쪽파", "실파", "green onion"],
  },
  { mappingType: "equivalent", synonyms: ["부추"] },
  { mappingType: "equivalent", synonyms: ["아스파라거스", "asparagus"] },
  { mappingType: "equivalent", synonyms: ["브로콜리", "broccoli"] },

  // 열매채소
  {
    mappingType: "equivalent",
    synonyms: ["고추", "청양고추", "풋고추", "pepper", "chili"],
  },
  { mappingType: "equivalent", synonyms: ["오이", "cucumber"] },
  {
    mappingType: "equivalent",
    synonyms: ["호박", "애호박", "단호박", "pumpkin", "zucchini"],
  },
  { mappingType: "equivalent", synonyms: ["가지", "eggplant"] },
  {
    mappingType: "equivalent",
    synonyms: ["토마토", "방울토마토", "방토", "tomato", "cherry tomato"],
  },
  { mappingType: "equivalent", synonyms: ["피망", "파프리카", "bell pepper"] },
  { mappingType: "equivalent", synonyms: ["옥수수", "corn"] },
  { mappingType: "equivalent", synonyms: ["아보카도", "avocado"] },

  // 뿌리채소
  { mappingType: "equivalent", synonyms: ["무", "무우", "radish"] },
  { mappingType: "equivalent", synonyms: ["당근", "carrot"] },
  { mappingType: "equivalent", synonyms: ["감자", "햇감자", "potato"] },
  {
    mappingType: "equivalent",
    synonyms: ["고구마", "호박고구마", "밤고구마", "sweet potato"],
  },
  { mappingType: "equivalent", synonyms: ["양파", "onion"] },
  {
    mappingType: "equivalent",
    synonyms: ["마늘", "다진마늘", "통마늘", "garlic"],
  },
  { mappingType: "equivalent", synonyms: ["생강", "ginger"] },
  { mappingType: "equivalent", synonyms: ["연근", "lotus root"] },
  { mappingType: "equivalent", synonyms: ["우엉", "burdock"] },

  // 버섯류
  { mappingType: "equivalent", synonyms: ["버섯", "mushroom"] },
  { mappingType: "equivalent", synonyms: ["표고버섯", "표고"] },
  { mappingType: "equivalent", synonyms: ["느타리버섯", "느타리"] },
  { mappingType: "equivalent", synonyms: ["팽이버섯", "팽이"] },
  { mappingType: "equivalent", synonyms: ["새송이버섯", "새송이"] },
  { mappingType: "equivalent", synonyms: ["양송이버섯", "양송이"] },

  // ─────────────────────────────────────────────
  // 4. 과일 (Fruits)
  // ─────────────────────────────────────────────
  { mappingType: "equivalent", synonyms: ["과일", "청과", "fruit"] },

  // 국산과일
  {
    mappingType: "equivalent",
    synonyms: ["사과", "애플", "부사", "홍로", "apple"],
  },
  { mappingType: "equivalent", synonyms: ["배", "나주배", "신고배", "pear"] },
  { mappingType: "equivalent", synonyms: ["포도", "캠벨", "거봉", "grape"] },
  { mappingType: "equivalent", synonyms: ["샤인머스켓", "망고포도"] },
  {
    mappingType: "equivalent",
    synonyms: ["복숭아", "백도", "황도", "천도복숭아", "peach"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["감", "단감", "홍시", "곶감", "persimmon"],
  },
  { mappingType: "equivalent", synonyms: ["수박", "watermelon"] },
  { mappingType: "equivalent", synonyms: ["참외", "melon"] },
  { mappingType: "equivalent", synonyms: ["딸기", "설향", "strawberry"] },
  { mappingType: "equivalent", synonyms: ["자두", "plum"] },

  // 감귤류
  {
    mappingType: "equivalent",
    synonyms: ["귤", "감귤", "밀감", "조생귤", "tangerine", "mandarin"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["한라봉", "천혜향", "레드향", "황금향"],
  },
  { mappingType: "equivalent", synonyms: ["오렌지", "orange"] },
  { mappingType: "equivalent", synonyms: ["레몬", "lemon"] },
  { mappingType: "equivalent", synonyms: ["자몽", "grapefruit"] },
  { mappingType: "equivalent", synonyms: ["라임", "lime"] },

  // 수입/열대과일
  { mappingType: "equivalent", synonyms: ["바나나", "빠나나", "banana"] },
  { mappingType: "equivalent", synonyms: ["파인애플", "pineapple"] },
  { mappingType: "equivalent", synonyms: ["망고", "애플망고", "mango"] },
  { mappingType: "equivalent", synonyms: ["키위", "골드키위", "kiwi"] },
  { mappingType: "equivalent", synonyms: ["체리", "cherry"] },
  { mappingType: "equivalent", synonyms: ["블루베리", "blueberry"] },
  { mappingType: "equivalent", synonyms: ["멜론", "메론", "muskmelon"] },
  { mappingType: "equivalent", synonyms: ["석류", "pomegranate"] },

  // ─────────────────────────────────────────────
  // 5. 쌀 / 잡곡 / 견과 (Grains & Nuts)
  // ─────────────────────────────────────────────
  { mappingType: "equivalent", synonyms: ["쌀", "백미", "rice"] },
  { mappingType: "equivalent", synonyms: ["찹쌀", "glutinous rice"] },
  { mappingType: "equivalent", synonyms: ["현미", "발아현미", "brown rice"] },
  { mappingType: "equivalent", synonyms: ["흑미", "검은콩"] },
  { mappingType: "equivalent", synonyms: ["잡곡", "혼합곡", "grain"] },
  { mappingType: "equivalent", synonyms: ["보리", "보리쌀", "barley"] },
  {
    mappingType: "equivalent",
    synonyms: ["콩", "대두", "서리태", "bean", "soybean"],
  },
  { mappingType: "equivalent", synonyms: ["귀리", "오트밀", "oat"] },

  // 견과류
  {
    mappingType: "equivalent",
    synonyms: ["견과", "견과류", "하루견과", "nuts"],
  },
  { mappingType: "equivalent", synonyms: ["아몬드", "almond"] },
  { mappingType: "equivalent", synonyms: ["호두", "walnut"] },
  { mappingType: "equivalent", synonyms: ["땅콩", "peanut"] },
  { mappingType: "equivalent", synonyms: ["캐슈넛", "cashew"] },
  { mappingType: "equivalent", synonyms: ["마카다미아"] },
  { mappingType: "equivalent", synonyms: ["피스타치오"] },
  { mappingType: "equivalent", synonyms: ["밤", "약단밤", "chestnut"] },

  // ─────────────────────────────────────────────
  // 6. 유제품 / 계란 / 두부 (Dairy, Eggs, Tofu)
  // ─────────────────────────────────────────────
  { mappingType: "equivalent", synonyms: ["우유", "밀크", "흰우유", "milk"] },
  { mappingType: "equivalent", synonyms: ["저지방우유", "무지방우유"] },
  { mappingType: "equivalent", synonyms: ["두유", "soy milk"] },
  {
    mappingType: "equivalent",
    synonyms: ["치즈", "모짜렐라", "체다치즈", "cheese"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["버터", "가염버터", "무염버터", "butter"],
  },
  { mappingType: "equivalent", synonyms: ["생크림", "whipping cream"] },
  {
    mappingType: "equivalent",
    synonyms: ["요거트", "요구르트", "요플레", "그릭요거트", "yogurt"],
  },

  // 계란/두부
  {
    mappingType: "equivalent",
    synonyms: ["계란", "달걀", "egg", "전란", "특란", "왕란"],
  },
  { mappingType: "equivalent", synonyms: ["메추리알", "quail egg"] },
  { mappingType: "equivalent", synonyms: ["두부", "tofu", "bean curd"] },
  { mappingType: "equivalent", synonyms: ["순두부", "연두부"] },
  { mappingType: "equivalent", synonyms: ["콩나물", "soybean sprouts"] },
  { mappingType: "equivalent", synonyms: ["숙주", "숙주나물"] },

  // ─────────────────────────────────────────────
  // 7. 가공식품 / 면 / 간편식 (Processed Food)
  // ─────────────────────────────────────────────
  // 면류
  { mappingType: "equivalent", synonyms: ["라면", "봉지라면", "ramen"] },
  { mappingType: "equivalent", synonyms: ["컵라면", "용기면"] },
  { mappingType: "equivalent", synonyms: ["비빔면", "쫄면"] },
  { mappingType: "equivalent", synonyms: ["짜장면", "짜장라면", "짜파게티"] },
  { mappingType: "equivalent", synonyms: ["국수", "소면", "중면", "noodle"] },
  {
    mappingType: "equivalent",
    synonyms: ["파스타", "스파게티", "푸실리", "pasta"],
  },
  { mappingType: "equivalent", synonyms: ["우동", "가락국수", "udon"] },
  { mappingType: "equivalent", synonyms: ["칼국수"] },
  { mappingType: "equivalent", synonyms: ["냉면", "물냉면", "비빔냉면"] },
  { mappingType: "equivalent", synonyms: ["당면"] },

  // 간편식/HMR
  { mappingType: "equivalent", synonyms: ["햇반", "즉석밥", "오뚜기밥"] },
  {
    mappingType: "equivalent",
    synonyms: ["만두", "교자", "왕만두", "물만두", "군만두", "dumpling"],
  },
  { mappingType: "equivalent", synonyms: ["피자", "냉동피자", "pizza"] },
  { mappingType: "equivalent", synonyms: ["핫도그", "hotdog"] },
  { mappingType: "equivalent", synonyms: ["치킨너겟", "너겟"] },
  { mappingType: "equivalent", synonyms: ["떡볶이", "라볶이", "tteokbokki"] },
  { mappingType: "equivalent", synonyms: ["죽", "본죽", "porridge"] },
  { mappingType: "equivalent", synonyms: ["스프", "soup"] },
  {
    mappingType: "equivalent",
    synonyms: ["시리얼", "씨리얼", "그래놀라", "콘푸로스트", "cereal"],
  },
  { mappingType: "equivalent", synonyms: ["밀키트", "쿠킹박스", "meal kit"] },
  { mappingType: "equivalent", synonyms: ["도시락", "컵밥", "lunch box"] },

  // 통조림
  { mappingType: "equivalent", synonyms: ["통조림", "캔", "canned food"] },
  {
    mappingType: "equivalent",
    synonyms: ["참치캔", "참치통조림", "canned tuna"],
  },
  { mappingType: "equivalent", synonyms: ["골뱅이"] },
  { mappingType: "equivalent", synonyms: ["옥수수캔", "스위트콘"] },

  // ─────────────────────────────────────────────
  // 8. 생수 / 음료 / 커피 / 주류 (Beverages)
  // ─────────────────────────────────────────────
  {
    mappingType: "equivalent",
    synonyms: ["생수", "물", "먹는샘물", "삼다수", "water"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["탄산수", "스파클링", "sparkling water"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["음료", "음료수", "드링크", "beverage", "drink"],
  },

  // 탄산/주스
  {
    mappingType: "equivalent",
    synonyms: ["콜라", "코카콜라", "펩시", "coke", "cola"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["사이다", "칠성사이다", "스프라이트", "cider", "soda"],
  },
  { mappingType: "equivalent", synonyms: ["주스", "쥬스", "과즙", "juice"] },
  { mappingType: "equivalent", synonyms: ["오렌지주스", "orange juice"] },
  { mappingType: "equivalent", synonyms: ["사과주스", "apple juice"] },
  { mappingType: "equivalent", synonyms: ["이온음료", "스포츠음료"] },

  // 커피/차
  { mappingType: "equivalent", synonyms: ["커피", "coffee", "cafe"] },
  { mappingType: "equivalent", synonyms: ["아메리카노", "아아", "americano"] },
  { mappingType: "equivalent", synonyms: ["라떼", "카페라떼", "latte"] },
  { mappingType: "equivalent", synonyms: ["믹스커피", "커피믹스"] },
  { mappingType: "equivalent", synonyms: ["원두", "홀빈", "coffee bean"] },
  { mappingType: "equivalent", synonyms: ["캡슐커피", "캡슐"] },
  { mappingType: "equivalent", synonyms: ["차", "티", "tea"] },
  { mappingType: "equivalent", synonyms: ["녹차", "green tea"] },
  { mappingType: "equivalent", synonyms: ["홍차", "black tea"] },
  { mappingType: "equivalent", synonyms: ["보리차"] },
  { mappingType: "equivalent", synonyms: ["콤부차"] },

  // 주류
  { mappingType: "equivalent", synonyms: ["술", "주류", "alcohol", "liquor"] },
  { mappingType: "equivalent", synonyms: ["소주", "soju"] },
  { mappingType: "equivalent", synonyms: ["맥주", "beer", "캔맥주"] },
  {
    mappingType: "equivalent",
    synonyms: ["와인", "레드와인", "화이트와인", "wine"],
  },
  { mappingType: "equivalent", synonyms: ["막걸리", "rice wine"] },
  { mappingType: "equivalent", synonyms: ["위스키", "양주", "whisky"] },
  { mappingType: "equivalent", synonyms: ["무알콜", "논알콜"] },

  // ─────────────────────────────────────────────
  // 9. 과자 / 간식 (Snacks)
  // ─────────────────────────────────────────────
  {
    mappingType: "equivalent",
    synonyms: ["과자", "스낵", "봉지과자", "snack"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["쿠키", "비스킷", "cookie", "biscuit"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["초콜릿", "초콜렛", "초코", "chocolate"],
  },
  { mappingType: "equivalent", synonyms: ["사탕", "캔디", "candy"] },
  { mappingType: "equivalent", synonyms: ["젤리", "구미", "jelly", "gummy"] },
  { mappingType: "equivalent", synonyms: ["껌", "gum"] },
  { mappingType: "equivalent", synonyms: ["빵", "베이커리", "bread"] },
  { mappingType: "equivalent", synonyms: ["식빵", "toast"] },
  { mappingType: "equivalent", synonyms: ["케이크", "케익", "cake"] },
  { mappingType: "equivalent", synonyms: ["떡", "rice cake"] },
  { mappingType: "equivalent", synonyms: ["아이스크림", "빙과", "ice cream"] },
  { mappingType: "equivalent", synonyms: ["육포", "beef jerky"] },
  { mappingType: "equivalent", synonyms: ["김스낵", "김부각"] },

  // ─────────────────────────────────────────────
  // 10. 조미료 / 소스 / 오일 (Condiments)
  // ─────────────────────────────────────────────
  { mappingType: "equivalent", synonyms: ["식용유", "기름", "오일", "oil"] },
  { mappingType: "equivalent", synonyms: ["올리브유", "olive oil"] },
  { mappingType: "equivalent", synonyms: ["카놀라유"] },
  { mappingType: "equivalent", synonyms: ["포도씨유"] },
  { mappingType: "equivalent", synonyms: ["참기름", "sesame oil"] },
  { mappingType: "equivalent", synonyms: ["들기름"] },

  // 가루/장류
  { mappingType: "equivalent", synonyms: ["소금", "천일염", "꽃소금", "salt"] },
  {
    mappingType: "equivalent",
    synonyms: ["설탕", "백설탕", "흑설탕", "sugar"],
  },
  { mappingType: "equivalent", synonyms: ["고춧가루"] },
  { mappingType: "equivalent", synonyms: ["후추", "pepper"] },
  { mappingType: "equivalent", synonyms: ["밀가루", "flour"] },
  { mappingType: "equivalent", synonyms: ["부침가루", "튀김가루"] },
  {
    mappingType: "equivalent",
    synonyms: ["간장", "진간장", "국간장", "soy sauce"],
  },
  { mappingType: "equivalent", synonyms: ["고추장", "gochujang"] },
  { mappingType: "equivalent", synonyms: ["된장", "soybean paste"] },
  { mappingType: "equivalent", synonyms: ["쌈장"] },
  { mappingType: "equivalent", synonyms: ["식초", "vinegar"] },
  { mappingType: "equivalent", synonyms: ["액젓", "멸치액젓", "까나리액젓"] },

  // 소스
  {
    mappingType: "equivalent",
    synonyms: ["소스", "드레싱", "sauce", "dressing"],
  },
  { mappingType: "equivalent", synonyms: ["케첩", "케찹", "ketchup"] },
  { mappingType: "equivalent", synonyms: ["마요네즈", "mayonnaise"] },
  { mappingType: "equivalent", synonyms: ["머스타드", "머스터드", "mustard"] },
  { mappingType: "equivalent", synonyms: ["돈까스소스"] },
  {
    mappingType: "equivalent",
    synonyms: ["파스타소스", "토마토소스", "크림소스"],
  },
  { mappingType: "equivalent", synonyms: ["굴소스", "oyster sauce"] },
  { mappingType: "equivalent", synonyms: ["잼", "딸기잼", "jam"] },

  // ─────────────────────────────────────────────
  // 11. 건강식품 (Health Supplements)
  // ─────────────────────────────────────────────
  { mappingType: "equivalent", synonyms: ["영양제", "비타민", "vitamin"] },
  {
    mappingType: "equivalent",
    synonyms: ["유산균", "프로바이오틱스", "probiotics"],
  },
  { mappingType: "equivalent", synonyms: ["오메가3", "omega3"] },
  { mappingType: "equivalent", synonyms: ["홍삼", "red ginseng"] },
  { mappingType: "equivalent", synonyms: ["루테인"] },
  { mappingType: "equivalent", synonyms: ["콜라겐", "collagen"] },
  { mappingType: "equivalent", synonyms: ["단백질", "프로틴", "protein"] },
  { mappingType: "equivalent", synonyms: ["다이어트", "diet"] },
  {
    mappingType: "equivalent",
    synonyms: ["닭가슴살", "닭가슴", "chicken breast"],
  },

  // ===================== RTX 5090 =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 5090",
      "rtx5090",
      "geforce rtx 5090",
      "geforce 5090",
      "nvidia rtx 5090",
      "nvidia 5090",
      "5090",
      "지포스 5090",
      "엔비디아 5090",
      "5090 그래픽카드",
      "5090 gpu",
    ],
  },

  // ===================== RTX 5090 Ti (대비용) =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 5090 ti",
      "rtx5090ti",
      "rtx 5090ti",
      "5090 ti",
      "5090ti",
      "geforce rtx 5090 ti",
      "nvidia 5090 ti",
      "지포스 5090 ti",
      "5090ti 그래픽카드",
    ],
  },

  // ===================== RTX 5090 SUPER (대비용) =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 5090 super",
      "rtx5090super",
      "5090 super",
      "5090super",
      "geforce rtx 5090 super",
      "nvidia 5090 super",
      "지포스 5090 super",
      "5090super 그래픽카드",
    ],
  },

  // ===================== RTX 5080 =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 5080",
      "rtx5080",
      "geforce rtx 5080",
      "geforce 5080",
      "nvidia rtx 5080",
      "nvidia 5080",
      "5080",
      "지포스 5080",
      "엔비디아 5080",
      "5080 그래픽카드",
      "5080 gpu",
    ],
  },

  // ===================== RTX 5080 Ti (대비용) =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 5080 ti",
      "rtx5080ti",
      "rtx 5080ti",
      "5080 ti",
      "5080ti",
      "geforce rtx 5080 ti",
      "nvidia 5080 ti",
      "지포스 5080 ti",
      "5080ti 그래픽카드",
    ],
  },

  // ===================== RTX 5080 SUPER (대비용) =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 5080 super",
      "rtx5080super",
      "5080 super",
      "5080super",
      "geforce rtx 5080 super",
      "nvidia 5080 super",
      "지포스 5080 super",
      "5080super 그래픽카드",
    ],
  },

  // ===================== RTX 5070 =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 5070",
      "rtx5070",
      "geforce rtx 5070",
      "geforce 5070",
      "nvidia rtx 5070",
      "nvidia 5070",
      "5070",
      "지포스 5070",
      "엔비디아 5070",
      "5070 그래픽카드",
      "5070 gpu",
    ],
  },

  // ===================== RTX 5070 Ti =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 5070 ti",
      "rtx5070ti",
      "rtx 5070ti",
      "5070 ti",
      "5070ti",
      "geforce rtx 5070 ti",
      "nvidia 5070 ti",
      "지포스 5070 ti",
      "5070ti 그래픽카드",
    ],
  },

  // ===================== RTX 5070 SUPER (대비용) =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 5070 super",
      "rtx5070super",
      "5070 super",
      "5070super",
      "5070 s",
      "5070s",
      "geforce rtx 5070 super",
      "nvidia 5070 super",
      "지포스 5070 super",
      "5070s 그래픽카드",
    ],
  },

  // ===================== RTX 5070 Ti SUPER (대비용) =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 5070 ti super",
      "rtx5070tisuper",
      "5070 ti super",
      "5070tis",
      "5070 ti s",
      "geforce rtx 5070 ti super",
      "nvidia 5070 ti super",
      "지포스 5070 ti super",
      "5070tis 그래픽카드",
    ],
  },

  // ===================== RTX 5060 =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 5060",
      "rtx5060",
      "geforce rtx 5060",
      "geforce 5060",
      "nvidia rtx 5060",
      "nvidia 5060",
      "5060",
      "지포스 5060",
      "엔비디아 5060",
      "5060 그래픽카드",
      "5060 gpu",
    ],
  },

  // ===================== RTX 5060 Ti =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 5060 ti",
      "rtx5060ti",
      "rtx 5060ti",
      "5060 ti",
      "5060ti",
      "geforce rtx 5060 ti",
      "nvidia 5060 ti",
      "지포스 5060 ti",
      "5060ti 그래픽카드",
    ],
  },

  // ===================== RTX 5060 SUPER (대비용) =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 5060 super",
      "rtx5060super",
      "5060 super",
      "5060super",
      "5060 s",
      "5060s",
      "geforce rtx 5060 super",
      "nvidia 5060 super",
      "지포스 5060 super",
      "5060s 그래픽카드",
    ],
  },

  // ===================== RTX 5050 (대비용/보급형 가정) =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 5050",
      "rtx5050",
      "geforce rtx 5050",
      "geforce 5050",
      "nvidia rtx 5050",
      "nvidia 5050",
      "5050",
      "지포스 5050",
      "엔비디아 5050",
      "5050 그래픽카드",
      "5050 gpu",
    ],
  },

  // ===================== GPU : NVIDIA RTX 40 =====================
  {
    mappingType: "equivalent",
    synonyms: ["rtx 4090", "4090", "geforce 4090", "nvidia 4090", "rtx4090"],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 4080 super",
      "4080 super",
      "4080s",
      "rtx4080 super",
      "rtx 4080s",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: ["rtx 4080", "4080", "geforce 4080", "nvidia 4080", "rtx4080"],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 4070 ti super",
      "4070 ti super",
      "4070tis",
      "4070 ti s",
      "rtx4070 ti super",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 4070 ti",
      "4070ti",
      "4070 ti",
      "rtx4070ti",
      "nvidia 4070ti",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 4070 super",
      "4070 super",
      "4070s",
      "rtx4070 super",
      "4070 s",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: ["rtx 4070", "4070", "geforce 4070", "nvidia 4070", "rtx4070"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["rtx 4060 ti", "4060 ti", "4060ti", "rtx4060ti", "4060 ti gpu"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["rtx 4060", "4060", "rtx4060", "geforce 4060", "nvidia 4060"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["rtx 4050", "4050", "rtx4050", "geforce 4050", "nvidia 4050"],
  },

  // ===================== GPU : NVIDIA RTX 30 =====================
  {
    mappingType: "equivalent",
    synonyms: ["rtx 3090", "3090", "rtx3090", "geforce 3090", "nvidia 3090"],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 3080 ti",
      "3080 ti",
      "3080ti",
      "rtx3080ti",
      "geforce 3080ti",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: ["rtx 3080", "3080", "rtx3080", "geforce 3080", "nvidia 3080"],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 3070 ti",
      "3070 ti",
      "3070ti",
      "rtx3070ti",
      "geforce 3070ti",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: ["rtx 3070", "3070", "rtx3070", "geforce 3070", "nvidia 3070"],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "rtx 3060 ti",
      "3060 ti",
      "3060ti",
      "rtx3060ti",
      "geforce 3060ti",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: ["rtx 3060", "3060", "rtx3060", "geforce 3060", "nvidia 3060"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["rtx 3050", "3050", "rtx3050", "geforce 3050", "nvidia 3050"],
  },

  // ===================== GPU : NVIDIA GTX (인기 모델) =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "gtx 1660 super",
      "1660 super",
      "1660s",
      "gtx1660 super",
      "gtx 1660s",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: ["gtx 1660 ti", "1660 ti", "1660ti", "gtx1660ti", "gtx 1660ti"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["gtx 1660", "1660", "gtx1660", "nvidia 1660", "geforce 1660"],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "gtx 1650 super",
      "1650 super",
      "1650s",
      "gtx1650 super",
      "gtx 1650s",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: ["gtx 1650", "1650", "gtx1650", "nvidia 1650", "geforce 1650"],
  },

  // ===================== GPU : AMD RX 7000 =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "rx 7900 xtx",
      "7900xtx",
      "radeon 7900 xtx",
      "amd 7900 xtx",
      "rx7900xtx",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "rx 7900 xt",
      "7900xt",
      "radeon 7900 xt",
      "amd 7900 xt",
      "rx7900xt",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "rx 7800 xt",
      "7800xt",
      "radeon 7800 xt",
      "amd 7800 xt",
      "rx7800xt",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "rx 7700 xt",
      "7700xt",
      "radeon 7700 xt",
      "amd 7700 xt",
      "rx7700xt",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "rx 7600 xt",
      "7600xt",
      "radeon 7600 xt",
      "amd 7600 xt",
      "rx7600xt",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: ["rx 7600", "7600", "radeon 7600", "amd 7600 gpu", "rx7600"],
  },

  // ===================== GPU : AMD RX 6000 =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "rx 6950 xt",
      "6950xt",
      "radeon 6950 xt",
      "amd 6950 xt",
      "rx6950xt",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "rx 6900 xt",
      "6900xt",
      "radeon 6900 xt",
      "amd 6900 xt",
      "rx6900xt",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "rx 6800 xt",
      "6800xt",
      "radeon 6800 xt",
      "amd 6800 xt",
      "rx6800xt",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "rx 6700 xt",
      "6700xt",
      "radeon 6700 xt",
      "amd 6700 xt",
      "rx6700xt",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "rx 6600 xt",
      "6600xt",
      "radeon 6600 xt",
      "amd 6600 xt",
      "rx6600xt",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: ["rx 6600", "6600", "radeon 6600", "amd 6600 gpu", "rx6600"],
  },

  // ===================== CPU : Intel Core (13/14세대 대표) =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "i9 14900k",
      "14900k",
      "core i9 14900k",
      "intel 14900k",
      "i9-14900k",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "i7 14700k",
      "14700k",
      "core i7 14700k",
      "intel 14700k",
      "i7-14700k",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "i5 14600k",
      "14600k",
      "core i5 14600k",
      "intel 14600k",
      "i5-14600k",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "i9 13900k",
      "13900k",
      "core i9 13900k",
      "intel 13900k",
      "i9-13900k",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "i7 13700k",
      "13700k",
      "core i7 13700k",
      "intel 13700k",
      "i7-13700k",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "i5 13600k",
      "13600k",
      "core i5 13600k",
      "intel 13600k",
      "i5-13600k",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "i5 13400f",
      "13400f",
      "core i5 13400f",
      "intel 13400f",
      "i5-13400f",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "i3 13100f",
      "13100f",
      "core i3 13100f",
      "intel 13100f",
      "i3-13100f",
    ],
  },

  // ===================== CPU : AMD Ryzen 7000 / X3D =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "ryzen 9 7950x3d",
      "7950x3d",
      "r9 7950x3d",
      "amd 7950x3d",
      "ryzen7950x3d",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: ["ryzen 9 7900x", "7900x", "r9 7900x", "amd 7900x", "ryzen7900x"],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "ryzen 7 7800x3d",
      "7800x3d",
      "r7 7800x3d",
      "amd 7800x3d",
      "ryzen7800x3d",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: ["ryzen 7 7700x", "7700x", "r7 7700x", "amd 7700x", "ryzen7700x"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["ryzen 7 7700", "7700", "r7 7700", "amd 7700", "ryzen7700"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["ryzen 5 7600x", "7600x", "r5 7600x", "amd 7600x", "ryzen7600x"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["ryzen 5 7600", "7600", "r5 7600", "amd 7600", "ryzen7600"],
  },

  // ===================== CPU : AMD Ryzen 5000 (가성비 대표) =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "ryzen 7 5800x3d",
      "5800x3d",
      "r7 5800x3d",
      "amd 5800x3d",
      "ryzen5800x3d",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: ["ryzen 7 5800x", "5800x", "r7 5800x", "amd 5800x", "ryzen5800x"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["ryzen 5 5600x", "5600x", "r5 5600x", "amd 5600x", "ryzen5600x"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["ryzen 5 5600", "5600", "r5 5600", "amd 5600", "ryzen5600"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["ryzen 5 5500", "5500", "r5 5500", "amd 5500", "ryzen5500"],
  },

  // ===================== RAM : DDR5 용량/구성 =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "ddr5 64gb",
      "64gb ddr5",
      "ddr5 32x2",
      "32gbx2 ddr5",
      "ddr5 32gb x2",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "ddr5 32gb",
      "32gb ddr5",
      "ddr5 16x2",
      "16gbx2 ddr5",
      "ddr5 16gb x2",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "ddr5 16gb",
      "16gb ddr5",
      "ddr5 8x2",
      "8gbx2 ddr5",
      "ddr5 8gb x2",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: ["ddr5 8gb", "8gb ddr5", "ddr5 8g", "8g ddr5", "ddr5 8 g"],
  },

  // ===================== RAM : DDR4 용량/구성 =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "ddr4 64gb",
      "64gb ddr4",
      "ddr4 32x2",
      "32gbx2 ddr4",
      "ddr4 32gb x2",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "ddr4 32gb",
      "32gb ddr4",
      "ddr4 16x2",
      "16gbx2 ddr4",
      "ddr4 16gb x2",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "ddr4 16gb",
      "16gb ddr4",
      "ddr4 8x2",
      "8gbx2 ddr4",
      "ddr4 8gb x2",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: ["ddr4 8gb", "8gb ddr4", "ddr4 8g", "8g ddr4", "ddr4 8 g"],
  },

  // ===================== Storage : NVMe(용량/폼팩터) =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "nvme 512gb",
      "m.2 512gb",
      "m2 512gb",
      "m.2 nvme 512gb",
      "ssd nvme 512gb",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: ["nvme 1tb", "m.2 1tb", "m2 1tb", "m.2 nvme 1tb", "ssd nvme 1tb"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["nvme 2tb", "m.2 2tb", "m2 2tb", "m.2 nvme 2tb", "ssd nvme 2tb"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["nvme 4tb", "m.2 4tb", "m2 4tb", "m.2 nvme 4tb", "ssd nvme 4tb"],
  },

  // ===================== Storage : PCIe Gen 표기 =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "pcie 4.0 ssd",
      "pcie4 ssd",
      "gen4 ssd",
      "pci-e 4.0 ssd",
      "pcie4.0 nvme",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "pcie 5.0 ssd",
      "pcie5 ssd",
      "gen5 ssd",
      "pci-e 5.0 ssd",
      "pcie5.0 nvme",
    ],
  },

  // ===================== Storage : SATA SSD / 2.5 =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "sata ssd 1tb",
      "2.5 ssd 1tb",
      "2.5인치 ssd 1tb",
      "ssd sata 1tb",
      "sata3 ssd 1tb",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "sata ssd 500gb",
      "2.5 ssd 500gb",
      "2.5인치 ssd 500gb",
      "ssd sata 500gb",
      "sata3 ssd 500gb",
    ],
  },

  // ===================== Storage : HDD =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "hdd 1tb",
      "하드 1tb",
      "하드디스크 1tb",
      "3.5 hdd 1tb",
      "3.5인치 하드 1tb",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "hdd 2tb",
      "하드 2tb",
      "하드디스크 2tb",
      "3.5 hdd 2tb",
      "3.5인치 하드 2tb",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "hdd 4tb",
      "하드 4tb",
      "하드디스크 4tb",
      "3.5 hdd 4tb",
      "3.5인치 하드 4tb",
    ],
  },

  // ===================== Motherboard : AMD 칩셋 =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "a520",
      "amd a520",
      "a520 메인보드",
      "a520 motherboard",
      "a520m",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "b550",
      "amd b550",
      "b550 메인보드",
      "b550 motherboard",
      "b550m",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "x570",
      "amd x570",
      "x570 메인보드",
      "x570 motherboard",
      "x570s",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "b650",
      "amd b650",
      "b650 메인보드",
      "b650 motherboard",
      "b650m",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "x670",
      "amd x670",
      "x670 메인보드",
      "x670 motherboard",
      "x670e",
    ],
  },

  // ===================== Motherboard : Intel 칩셋 =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "h610",
      "intel h610",
      "h610 메인보드",
      "h610 motherboard",
      "h610m",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "b660",
      "intel b660",
      "b660 메인보드",
      "b660 motherboard",
      "b660m",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "z690",
      "intel z690",
      "z690 메인보드",
      "z690 motherboard",
      "z690m",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "b760",
      "intel b760",
      "b760 메인보드",
      "b760 motherboard",
      "b760m",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "z790",
      "intel z790",
      "z790 메인보드",
      "z790 motherboard",
      "z790m",
    ],
  },

  // ===================== Motherboard : 폼팩터 =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "atx",
      "atx 메인보드",
      "atx motherboard",
      "표준 atx",
      "full atx",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: ["m-atx", "matx", "micro atx", "마이크로 atx", "m atx"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["itx", "mini itx", "미니 itx", "mini-itx", "itx 메인보드"],
  },

  // ===================== PSU : 용량(와트) =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "파워 500w",
      "psu 500w",
      "500w psu",
      "500w 파워",
      "파워서플라이 500w",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "파워 600w",
      "psu 600w",
      "600w psu",
      "600w 파워",
      "파워서플라이 600w",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "파워 650w",
      "psu 650w",
      "650w psu",
      "650w 파워",
      "파워서플라이 650w",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "파워 700w",
      "psu 700w",
      "700w psu",
      "700w 파워",
      "파워서플라이 700w",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "파워 750w",
      "psu 750w",
      "750w psu",
      "750w 파워",
      "파워서플라이 750w",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "파워 850w",
      "psu 850w",
      "850w psu",
      "850w 파워",
      "파워서플라이 850w",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "파워 1000w",
      "psu 1000w",
      "1000w psu",
      "1000w 파워",
      "파워서플라이 1000w",
    ],
  },

  // ===================== PSU : 80PLUS 인증 표기 =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "80plus bronze",
      "80+ bronze",
      "브론즈 파워",
      "브론즈 인증",
      "80 플러스 브론즈",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "80plus silver",
      "80+ silver",
      "실버 파워",
      "실버 인증",
      "80 플러스 실버",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "80plus gold",
      "80+ gold",
      "골드 파워",
      "골드 인증",
      "80 플러스 골드",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "80plus platinum",
      "80+ platinum",
      "플래티넘 파워",
      "플래티넘 인증",
      "80 플러스 플래티넘",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "80plus titanium",
      "80+ titanium",
      "티타늄 파워",
      "티타늄 인증",
      "80 플러스 티타늄",
    ],
  },

  // ===================== Case : 케이스 크기 =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "미들타워",
      "미들 타워",
      "mid tower",
      "midtower",
      "middle tower",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: ["빅타워", "빅 타워", "full tower", "fulltower", "big tower"],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "미니타워",
      "미니 타워",
      "mini tower",
      "minitower",
      "small tower",
    ],
  },

  // ===================== Case : 강화유리/메쉬 =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "강화유리 케이스",
      "tempered glass case",
      "tg 케이스",
      "유리 케이스",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "메쉬 케이스",
      "mesh case",
      "통풍 케이스",
      "airflow case",
      "에어플로우 케이스",
    ],
  },

  // ===================== Cooler : 공랭/수랭 =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "공랭 쿨러",
      "cpu 공랭",
      "air cooler",
      "타워쿨러",
      "tower cooler",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: ["수랭 쿨러", "cpu 수랭", "aio", "일체형 수랭", "water cooler"],
  },

  // ===================== AIO 라디에이터 사이즈 =====================
  {
    mappingType: "equivalent",
    synonyms: ["aio 240", "240 aio", "240mm aio", "수냉 240", "240 수랭"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["aio 280", "280 aio", "280mm aio", "수냉 280", "280 수랭"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["aio 360", "360 aio", "360mm aio", "수냉 360", "360 수랭"],
  },

  // ===================== Fan : 사이즈 =====================
  {
    mappingType: "equivalent",
    synonyms: ["120mm 팬", "120 팬", "120mm fan", "12cm 팬", "팬 120"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["140mm 팬", "140 팬", "140mm fan", "14cm 팬", "팬 140"],
  },

  // ===================== Fan : RGB 표기 =====================
  {
    mappingType: "equivalent",
    synonyms: ["argb", "a-rgb", "addressable rgb", "5v argb", "5v 3pin rgb"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["rgb", "12v rgb", "4pin rgb", "non-argb", "12v 4pin rgb"],
  },

  // ===================== Monitor : 해상도 =====================
  {
    mappingType: "equivalent",
    synonyms: ["fhd", "1080p", "1920x1080", "full hd", "풀hd"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["qhd", "1440p", "2560x1440", "quad hd", "2k"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["uhd", "4k", "2160p", "3840x2160", "ultra hd"],
  },

  // ===================== Monitor : 주사율 =====================
  {
    mappingType: "equivalent",
    synonyms: ["144hz", "144 헤르츠", "144hz 모니터", "144 주사율", "144 hZ"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["165hz", "165 헤르츠", "165hz 모니터", "165 주사율", "165 hZ"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["240hz", "240 헤르츠", "240hz 모니터", "240 주사율", "240 hZ"],
  },

  // ===================== Keyboard : 스위치/배열 =====================
  {
    mappingType: "equivalent",
    synonyms: ["텐키리스", "tkl", "87키", "tenkeyless", "87 key"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["풀배열", "full size", "104키", "108키", "full keyboard"],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "적축",
      "red switch",
      "리니어 적축",
      "적축 스위치",
      "cherry red",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "갈축",
      "brown switch",
      "갈축 스위치",
      "cherry brown",
      "택타일 갈축",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "청축",
      "blue switch",
      "청축 스위치",
      "cherry blue",
      "클릭 청축",
    ],
  },

  // ===================== Mouse : 연결 방식 =====================
  {
    mappingType: "equivalent",
    synonyms: [
      "무선 마우스",
      "wireless mouse",
      "2.4g 마우스",
      "2.4ghz 마우스",
      "리시버 마우스",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "유선 마우스",
      "wired mouse",
      "usb 마우스",
      "케이블 마우스",
      "wired gaming mouse",
    ],
  },

  // ===================== Network : 와이파이 표기 =====================
  {
    mappingType: "equivalent",
    synonyms: ["wifi 6", "wi-fi 6", "802.11ax", "와이파이6", "무선 ax"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["wifi 6e", "wi-fi 6e", "802.11ax 6e", "와이파이6e", "6e 무선"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["wifi 7", "wi-fi 7", "802.11be", "와이파이7", "무선 be"],
  },

  // ===================== Misc : 인터페이스/포트 =====================
  {
    mappingType: "equivalent",
    synonyms: ["usb-c", "type-c", "타입c", "type c", "usb type-c"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["usb-a", "type-a", "타입a", "type a", "usb type-a"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["hdmi 2.1", "hdmi2.1", "hdmi 2 1", "hdmi 2_1", "hdmi v2.1"],
  },
  {
    mappingType: "equivalent",
    synonyms: [
      "dp 1.4",
      "displayport 1.4",
      "dp1.4",
      "디스플레이포트 1.4",
      "dp v1.4",
    ],
  },

  // ===================== Windows/OS (가끔 부품몰 검색에 섞임) =====================
  {
    mappingType: "equivalent",
    synonyms: ["윈도우 11", "windows 11", "win11", "w11", "윈11"],
  },
  {
    mappingType: "equivalent",
    synonyms: ["윈도우 10", "windows 10", "win10", "w10", "윈10"],
  },
  // 1) 낚시대
  {
    mappingType: "equivalent",
    synonyms: [
      "낚시대",
      "낚싯대",
      "낚시 로드",
      "fishing rod",
      "rod",
      "낚시봉",
      "로드대",
    ],
  },

  {
    mappingType: "equivalent",
    synonyms: [
      "배쓰낚시대",
      "배쓰 낚시대",
      "배스낚시대",
      "배스 낚시대",
      "배스대",
      "배쓰대",
      "bass rod",
      "bass fishing rod",
    ],
  },

  // 2) 베이트대(캐스팅대)
  {
    mappingType: "equivalent",
    synonyms: [
      "베이트대",
      "베이트 낚시대",
      "베이트 로드",
      "베이트캐스팅대",
      "베이트캐스팅",
      "캐스팅대",
      "bait rod",
      "baitcasting rod",
      "casting rod",
    ],
  },

  // 3) 스피닝대
  {
    mappingType: "equivalent",
    synonyms: [
      "스피닝대",
      "스피닝 낚시대",
      "스피닝 로드",
      "스핀대",
      "스피닝",
      "spinning rod",
      "spinning",
      "스피닝낚시대",
    ],
  },

  // 4) 릴(공통)
  {
    mappingType: "equivalent",
    synonyms: [
      "릴",
      "낚시릴",
      "리일",
      "릴기",
      "낚시용 릴",
      "reel",
      "fishing reel",
      "릴 장비",
    ],
  },

  // 5) 스피닝릴
  {
    mappingType: "equivalent",
    synonyms: [
      "스피닝릴",
      "스핀릴",
      "스피닝 릴",
      "spinning reel",
      "스피닝릴기",
      "스피닝릴세트",
      "스핀 릴",
      "스피닝 리일",
    ],
  },

  // 6) 베이트릴
  {
    mappingType: "equivalent",
    synonyms: [
      "베이트릴",
      "베이트 릴",
      "캐스팅릴",
      "바다 베이트릴",
      "bait reel",
      "baitcasting reel",
      "캐스팅 릴",
      "베이트 리일",
    ],
  },

  // 7) 전동릴
  {
    mappingType: "equivalent",
    synonyms: [
      "전동릴",
      "전동 릴",
      "전동낚시릴",
      "전동릴기",
      "electric reel",
      "power reel",
      "전동리일",
      "전동릴세트",
    ],
  },

  // 8) 원줄(메인라인)
  {
    mappingType: "equivalent",
    synonyms: [
      "원줄",
      "메인라인",
      "메인 라인",
      "낚싯줄",
      "낚시줄",
      "라인",
      "main line",
      "fishing line",
    ],
  },

  // 9) 합사(PE)
  {
    mappingType: "equivalent",
    synonyms: [
      "합사",
      "합사줄",
      "PE라인",
      "PE 라인",
      "브레이드",
      "브레이드 라인",
      "braided line",
      "braid line",
    ],
  },

  // 10) 카본/플로로카본 라인
  {
    mappingType: "equivalent",
    synonyms: [
      "카본줄",
      "카본라인",
      "플로로카본",
      "플루오로카본",
      "fluorocarbon",
      "fluorocarbon line",
      "FC라인",
      "FC 라인",
    ],
  },

  // 11) 목줄/리더(쇼크리더)
  {
    mappingType: "equivalent",
    synonyms: [
      "목줄",
      "리더",
      "리더라인",
      "리더 줄",
      "쇼크리더",
      "쇼크 리더",
      "shock leader",
      "leader line",
    ],
  },

  // 12) 바늘/훅
  {
    mappingType: "equivalent",
    synonyms: [
      "바늘",
      "낚시바늘",
      "훅",
      "후크",
      "hook",
      "낚시 훅",
      "바늘세트",
      "훅세트",
    ],
  },

  // 13) 찌(플로트)
  {
    mappingType: "equivalent",
    synonyms: [
      "찌",
      "낚시찌",
      "찌낚시",
      "찌 채비",
      "float",
      "fishing float",
      "구멍찌",
      "전자찌",
    ],
  },

  // 14) 봉돌(싱커)
  {
    mappingType: "equivalent",
    synonyms: [
      "봉돌",
      "싱커",
      "추",
      "봉돌추",
      "lead weight",
      "납봉돌",
      "봉돌 세트",
    ],
  },

  // 15) 도래(스위벨)
  {
    mappingType: "equivalent",
    synonyms: [
      "도래",
      "스위벨",
      "회전도래",
      "삼각도래",
      "swivel",
      "fishing swivel",
      "도래 세트",
      "스위벨 세트",
    ],
  },

  // 16) 스냅(클립)
  {
    mappingType: "equivalent",
    synonyms: [
      "스냅",
      "스냅도래",
      "클립",
      "카라비너 스냅",
      "snap",
      "snap swivel",
      "퀵스냅",
      "스냅링",
    ],
  },

  // 17) 루어(인조미끼)
  {
    mappingType: "equivalent",
    synonyms: [
      "루어",
      "인조미끼",
      "인공미끼",
      "가짜미끼",
      "lure",
      "fishing lure",
      "루어미끼",
      "하드베이트",
    ],
  },

  // 18) 웜(소프트베이트)
  {
    mappingType: "equivalent",
    synonyms: [
      "웜",
      "소프트베이트",
      "소프트 베이트",
      "웜미끼",
      "worm",
      "soft bait",
      "소프트루어",
      "소프트 루어",
    ],
  },

  // 19) 미노우
  {
    mappingType: "equivalent",
    synonyms: [
      "미노우",
      "미노",
      "미노우 루어",
      "minnow",
      "미노우베이트",
      "하드미노우",
      "싱킹 미노우",
      "플로팅 미노우",
    ],
  },

  // 20) 크랭크베이트
  {
    mappingType: "equivalent",
    synonyms: [
      "크랭크베이트",
      "크랭크",
      "crankbait",
      "크랭크 루어",
      "딥크랭크",
      "섈로우 크랭크",
      "크랭크미끼",
      "크랭크베이트 루어",
    ],
  },

  // 21) 스피너베이트
  {
    mappingType: "equivalent",
    synonyms: [
      "스피너베이트",
      "스피너",
      "spinnerbait",
      "스피너 루어",
      "블레이드베이트",
      "블레이드 루어",
      "스피너 미끼",
      "스피너베이트 루어",
    ],
  },

  // 22) 스푼
  {
    mappingType: "equivalent",
    synonyms: [
      "스푼",
      "스푼루어",
      "spoon",
      "spoon lure",
      "메탈스푼",
      "송어스푼",
      "스푼 미끼",
      "스푼 베이트",
    ],
  },

  // 23) 메탈지그(지그)
  {
    mappingType: "equivalent",
    synonyms: [
      "메탈지그",
      "지그",
      "지깅",
      "metal jig",
      "jig",
      "jigging",
      "슬로우지그",
      "지그헤드",
    ],
  },

  // 24) 에기(오징어 루어)
  {
    mappingType: "equivalent",
    synonyms: [
      "에기",
      "오징어에기",
      "에깅",
      "squid jig",
      "egi",
      "에기 루어",
      "에기 미끼",
      "에기세트",
    ],
  },

  // 25) 다운샷 리그
  {
    mappingType: "equivalent",
    synonyms: [
      "다운샷",
      "다운샷리그",
      "다운샷 채비",
      "downshot",
      "drop shot",
      "드롭샷",
      "드롭샷리그",
      "드롭샷 채비",
    ],
  },

  // 26) 텍사스 리그
  {
    mappingType: "equivalent",
    synonyms: [
      "텍사스리그",
      "텍사스 리그",
      "텍사스 채비",
      "texas rig",
      "텍사스",
      "오프셋훅",
      "오프셋 훅",
      "텍사스 세팅",
    ],
  },

  // 27) 노싱커 / 무게 없는 채비
  {
    mappingType: "equivalent",
    synonyms: [
      "노싱커",
      "노싱커리그",
      "노 싱커",
      "무봉돌",
      "무게없는 채비",
      "프리리그",
      "free rig",
    ],
  },

  // 28) 원투낚시(캐스팅)
  {
    mappingType: "equivalent",
    synonyms: [
      "원투낚시",
      "원투",
      "원투대",
      "원투채비",
      "서프캐스팅",
      "surf casting",
      "원거리 캐스팅",
      "캐스팅낚시",
    ],
  },
  {
    mappingType: "equivalent",
    synonyms: ["낚싯", "낚시"],
  },
  // ...
];

const normalizeSynTerm = (s) => {
  let x = String(s ?? "")
    .trim()
    .toLowerCase();
  if (!x) return "";

  // ⚠️ Atlas Search synonyms에서 자주 터지는 패턴을 안전 치환
  // - 슬래시/하이픈/점 + 한 글자 토큰 조합은 analyzer에 따라 graph token 생성 -> 실패 가능
  x = x
    // 케이스
    .replace(/\bt\/g\b/g, "tg")
    // RGB
    .replace(/\ba-rgb\b/g, "argb")
    // 네트워크/인터페이스
    .replace(/\bwi-fi\b/g, "wifi")
    .replace(/\bpci-e\b/g, "pcie")
    // 폼팩터
    .replace(/\bm-atx\b/g, "matx")
    .replace(/\bmini-itx\b/g, "mini itx")
    // 스토리지 표기
    .replace(/\bm\.2\b/g, "m2");

  // 공백 정리
  x = x.replace(/\s+/g, " ").trim();

  return x;
};

const cleanToken = (s) => {
  const t = normalizeSynTerm(s);
  if (!t) return null;

  // ✅ 더 안전하게 가고 싶으면 공백 제거(옵션)
  // return t.replace(/\s+/g, "");

  return t;
};

// --------------------------
// 2) 문서 검증 + 정리
// --------------------------
const cleanDoc = (doc) => {
  if (!doc || typeof doc !== "object") return null;

  const mappingType = doc.mappingType;
  if (!["equivalent", "explicit"].includes(mappingType)) return null;

  // synonyms 검증/정리
  if (!Array.isArray(doc.synonyms)) return null;

  const synonyms = doc.synonyms.map(cleanToken).filter(Boolean);
  const uniqSynonyms = [...new Set(synonyms)];

  if (uniqSynonyms.length < 1) return null;
  if (mappingType === "equivalent" && uniqSynonyms.length < 2) return null;

  // explicit일 때 input도 필수
  if (mappingType === "explicit") {
    if (!Array.isArray(doc.input)) return null;
    const input = doc.input.map(cleanToken).filter(Boolean);
    const uniqInput = [...new Set(input)];
    if (uniqInput.length < 1) return null;

    return {
      mappingType: "explicit",
      input: uniqInput,
      synonyms: uniqSynonyms,
    };
  }

  // equivalent
  return {
    mappingType: "equivalent",
    synonyms: uniqSynonyms,
  };
};

// --------------------------
// 3) 배치 삽입 유틸 (너무 큰 insertMany 방지)
// --------------------------
const chunkArray = (arr, size) => {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
};

// --------------------------
// 4) 메인 시드 함수
// --------------------------
const seedDB = async () => {
  try {
    await dbConnect();
    console.log("🔥 MongoDB Connected!");

    const collection = mongoose.connection.collection("search_synonyms");

    // 데이터 정리/검증
    const cleaned = [];
    const rejected = [];

    synonymsData.forEach((d, i) => {
      const cd = cleanDoc(d);
      if (cd) cleaned.push(cd);
      else rejected.push({ index: i, doc: d });
    });

    console.log(`📦 원본: ${synonymsData.length}개`);
    console.log(`✅ 정리 후: ${cleaned.length}개`);
    console.log(`🚫 거절: ${rejected.length}개`);

    // 거절 샘플 출력(많으면 10개만)
    if (rejected.length) {
      console.log("🚫 rejected sample (up to 10):");
      rejected.slice(0, 10).forEach((r) => {
        console.log(`- index ${r.index}:`, r.doc);
      });
    }

    // 기존 데이터 삭제
    await collection.deleteMany({});
    console.log("🗑️ 기존 동의어 삭제 완료");

    // 배치 삽입
    const BATCH_SIZE = 1000; // 필요 시 조절
    const batches = chunkArray(cleaned, BATCH_SIZE);

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      if (batch.length === 0) continue;
      await collection.insertMany(batch, { ordered: true });
      console.log(
        `✅ batch ${i + 1}/${batches.length} inserted: ${batch.length}개`,
      );
    }

    console.log(`🎉 총 ${cleaned.length}개 동의어 입력 완료!`);

    process.exit(0);
  } catch (error) {
    console.error("❌ 에러 발생:", error);
    process.exit(1);
  }
};

seedDB();
