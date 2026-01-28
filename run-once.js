// run-once utility (Node.js / Mongoose)
import mongoose from "mongoose";
import ProductDetail from "./models/ProductDetail.js"; // 사용하신 파일 경로에 맞춰주세요
import dbConnect from "./utils/dbConnect.js";

const categoryList = [
  { categoryName: "이슬람 패션", vol: 1000 },
  { categoryName: "주얼리 및 엑세서리", vol: 2000 },
  { categoryName: "머리 연장 & 가발", vol: 2000 },
  { categoryName: "회계 용품", vol: 3000 },
  { categoryName: "클래식 장난감", vol: 1000 },
  { categoryName: "당신의 순서에 추가 급여", vol: 1000000000 },
  { categoryName: "조명", vol: 300 },
  { categoryName: "전원 도구 부품 및 액세서리", vol: 1000 },
  { categoryName: "네일아트 & 도구", vol: 3000 },
  { categoryName: "남성 시계", vol: 3000 },
  { categoryName: "웨딩 및 이벤트", vol: 1000 },
  { categoryName: "활성 구성 요소", vol: 1000 },
  { categoryName: "워키 토키 액세서리 및 부품", vol: 2000 },
  { categoryName: "도구 세트", vol: 1000 },
  { categoryName: "아기 기념품", vol: 10000 },
  { categoryName: "내장 부품", vol: 1000 },
  { categoryName: "전자 장난감", vol: 1500 },
  { categoryName: "참신하고 웃긴 장난감", vol: 1500 },
  { categoryName: "패션 주얼리", vol: 10000 },
  { categoryName: "회화 용품", vol: 1000 },
  { categoryName: "무대 & 댄스 의상", vol: 5000 },
  { categoryName: "헤어 액세서리", vol: 1000 },
  { categoryName: "욕실용품", vol: 500 },
  { categoryName: "자동차 전자 제품", vol: 1000 },
  { categoryName: "외장 부품", vol: 1000 },
  { categoryName: "신에너지 차량 부품 및 액세서리", vol: 2000 },
  { categoryName: "뷰티 & 헬스", vol: 500 },
  { categoryName: "작업장 안전 용품", vol: 1000 },
  { categoryName: "어머니 & 아이", vol: 2000 },
  { categoryName: "가전제품 부품", vol: 300 },
  { categoryName: "수공예품 및 바느질", vol: 1000 },
  { categoryName: "박제 동물 & 견면 벨벳", vol: 1000 },
  { categoryName: "코스프레 액세서리", vol: 1000 },
  { categoryName: "가족 지능 시스템", vol: 1000 },
  { categoryName: "펜, 연필 & 쓰기 공급", vol: 1000 },
  { categoryName: "수공구", vol: 1000 },
  { categoryName: "플레이 차량 및 모델", vol: 1000 },
  { categoryName: "배관", vol: 1000 },
  { categoryName: "문, 창문", vol: 1000 },
  { categoryName: "오토바이 장비 및 부품", vol: 5000 },
  { categoryName: "가방 부속품", vol: 2000 },
  { categoryName: "DIY 액세서리", vol: 1000 },
  { categoryName: "기타 차량 부품 및 액세서리", vol: 3000 },
  { categoryName: "출입 통제", vol: 3000 },
  { categoryName: "정원 도구", vol: 2000 },
  { categoryName: "보안 경보", vol: 1000 },
  { categoryName: "메이크업", vol: 500 },
];

// [이슬람 패션 vol 1000,
//  주얼리 및 엑세서리 vol 1000,
//  머리 연장 & 가발 vol 2000]
//  회계 용품 vol 3000]
// 클래식 장난감 vol 1000]
// 당신의 순서에 추가 급여 vol 100000000]
// 조명 vol 500]
// 전원 도구 부품 및 액세서리 vol 1000]
// 네일아트 & 도구 vol 3000]
// 남성 시계 vol 3000]
// 웨딩 및 이벤트 vol 1000]
// 활성 구성 요소 vol 1000]
// 워키 토키 액세서리 및 부품 vol 2000]
// 도구 세트 vol 1000]
// 아기 기념품 vol 10000]
// 내장 부품 vol 1000]
// 전자 장난감 vol 1500]
// 참신하고 웃긴 장난감 vol 1500]
// 패션 주얼리 vol 10000]
// 회화 용품 vol 1000]
// 무대 & 댄스 의상 vol 5000]
// 헤어 액세서리 vol 1000]
// 벨트 vol 1000]

async function removeProducts() {
  for (let category of categoryList) {
    const categoryName = category.categoryName;

    const filter = {
      $or: [
        { c1n: categoryName },
        { c2n: categoryName },
        { c3n: categoryName },
      ],
      vol: { $lt: category.vol },
    };

    const res = await ProductDetail.deleteMany(filter);
    console.log("삭제 결과:", {
      categoryName: categoryName,
      acknowledged: res.acknowledged,
      deletedCount: res.deletedCount,
    });
  }
}

// 샘플 실행 (연결/종료 포함)
(async () => {
  await dbConnect();
  try {
    await removeProducts();
  } finally {
    await mongoose.disconnect();
  }
})();
