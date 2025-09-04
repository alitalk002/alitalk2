import mongoose from "mongoose";

// 배열 원소 스키마 (_id 제거)

// 루트 스키마 (여기에 versionKey 등 옵션)
const ProductCategoriesSchema = new mongoose.Schema(
  {
    cId: { type: String, required: true, index: true }, // cId를 곧바로 _id로
    cn: { type: String, required: true, index: true },
  },
  { versionKey: false }
);

// category_id의 전역 유니크 보장 (멀티키 유니크 인덱스)
// - 문서 간 중복 금지
// - 같은 문서(list 내부) 중복도 금지
ProductCategoriesSchema.statics.findByCId = function (cid) {
  return this.findOne({ cId: String(cid) });
};
ProductCategoriesSchema.index({ cn: 1 });

const ProductCategories =
  mongoose.models.ProductCategories ||
  mongoose.model("ProductCategories", ProductCategoriesSchema); // ← 공백 제거

export default ProductCategories;
