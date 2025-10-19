// run-once utility (Node.js / Mongoose)
import mongoose from "mongoose";
import ProductDetail from "./models/ProductDetail.js"; // 사용하신 파일 경로에 맞춰주세요
import dbConnect from "./utils/dbConnect.js";

async function removeSalePrice1000OnDates() {
  // 대상 날짜들 (접두사 매칭: 'YYYY-MM-DD')
  const filter = { vol: { $lt: 160 } };
  const res = await ProductDetail.deleteMany(filter);
  console.log("res:", res);
}

// 샘플 실행 (연결/종료 포함)
(async () => {
  await dbConnect();
  try {
    await removeSalePrice1000OnDates();
  } finally {
    await mongoose.disconnect();
  }
})();
