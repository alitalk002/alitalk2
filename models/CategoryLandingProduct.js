import mongoose from "mongoose";
import ProductDetail from "./ProductDetail.js";

const OffItemSchema = new mongoose.Schema(
  {
    product: { type: String, ref: "ProductDetail", required: true }, // populate용
    sId: { type: String }, // 외부 상품ID를 함께 둘 수도
    c: { type: String }, // 예: 색상/코드
    sp: { type: String }, // JSON 문자열이 아니라 "객체/배열"로 저장 추천
  },
  { _id: false }
);

const CategoryLandingProductSchema = new mongoose.Schema(
  {
    categoryName: String,
    rnList: [
      {
        type: String,
        ref: "ProductDetail",
      },
    ],
    volList: [
      {
        type: String,
        ref: "ProductDetail",
      },
    ],
    psList: [
      {
        type: String,
        ref: "ProductDetail",
      },
    ],
    offList: [OffItemSchema],
  },
  { versionKey: false }
);

const CategoryLandingProduct =
  mongoose.models.CategoryLandingProduct ||
  mongoose.model("CategoryLandingProduct", CategoryLandingProductSchema); // ← 공백 제거

export default CategoryLandingProduct;
