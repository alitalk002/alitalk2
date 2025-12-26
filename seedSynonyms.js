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

  // ...
];

const seedDB = async () => {
  try {
    // 1. DB 연결

    await dbConnect();

    console.log("🔥 MongoDB Connected!");

    // 2. 컬렉션 접근 (스키마 없이 바로 접근)
    const collection = mongoose.connection.collection("search_synonyms");

    // 3. 기존 데이터 초기화 (중복 방지)
    await collection.deleteMany({});
    console.log("🗑️ 기존 동의어 삭제 완료");

    // 4. 데이터 삽입
    await collection.insertMany(synonymsData);
    console.log(`✅ 동의어 ${synonymsData.length}개 입력 완료!`);

    process.exit();
  } catch (error) {
    console.error("❌ 에러 발생:", error);
    process.exit(1);
  }
};

seedDB();
