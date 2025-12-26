// models/ProductDetail.js
import mongoose from "mongoose";
import ProductCategories from "./ProductCategories.js";
import Int32 from "mongoose-int32";
import { kstISO } from "../utils/kstISO.js";
import {
  normalizeCForCompare,
  normalizeSpForCompare,
} from "../utils/normalize.js";

mongoose.Schema.Types.Int32 = Int32;

// ── 비교용 정규화 유틸 (파일 상단 util 위치에 둬도 됨)
const _strip = (s) => String(s ?? "").replace(/[{}\[\]\(\)\"\s]/g, "");
const _SP_MAP = { 색깔: "색상" }; // 라벨 동의어
const _normC = (c) => _strip(c);
const _normSp = (spStr) => {
  try {
    let arr = JSON.parse(spStr);
    if (!Array.isArray(arr)) arr = [arr];
    const mapped = arr.map((o) => {
      const out = {};
      for (const [k, v] of Object.entries(o || {})) {
        const nk = _SP_MAP[k] || k;
        out[nk] = v;
      }
      // 키 정렬
      return Object.fromEntries(
        Object.entries(out).sort(([a], [b]) => (a > b ? 1 : -1))
      );
    });
    return _strip(JSON.stringify(mapped));
  } catch {
    return _strip(spStr);
  }
};

// 숫자 문자열 안전 정규화 (쉼표/통화기호 제거)
const toNumber = (v) =>
  v == null ? undefined : Number(String(v).replace(/[^\d.-]/g, "")).valueOf();

// ─────────────────────────────────────────────────────────────────────────────
// 날짜별 가격 포인트 (저장 키: p, s, t)
const PricePointSchema = new mongoose.Schema(
  {
    s: {
      type: mongoose.Schema.Types.Int32,
      min: 0,
      alias: "sale_price_with_tax",
      set: toNumber,
    },
    // t: {
    //   type: Date,
    //   default: () => kstISO(),
    //   alias: "collected_at",
    // },
  },
  { _id: false }
);

// ─────────────────────────────────────────────────────────────────────────────
/**
 * SKU 1개 (저장 키: sid, c, link, sp, cur, pd)
 * - pd: 날짜키(예: 2025-09-01T00:00:00.000Z)를 Key로 갖는 Map
 *       값은 PricePointSchema(서브도큐먼트)
 */
const SkuInfoItemSchema = new mongoose.Schema(
  {
    // sId: { type: String, required: true, alias: "sku_id" },
    c: { type: String, default: "", alias: "color", set: normalizeCForCompare },
    sp: {
      type: String,
      default: "",
      alias: "sku_properties",
    },
    spKey: { type: String, set: normalizeSpForCompare },
    // cur: { type: String, default: "KRW", alias: "currency" },
    pd: {
      type: Map,
      of: PricePointSchema,
      alias: "price_by_date",
      default: () => ({}),
    },
  },
  {
    _id: false,
    toJSON: { virtuals: true }, // JSON 변환 시 alias 노출
    toObject: { virtuals: true },
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// SKU 컨테이너 (저장 키: sil, alias: sku_info_list)
const SkuInfoSchema = new mongoose.Schema(
  {
    sil: {
      type: [SkuInfoItemSchema],
      alias: "sku_info_list",
      default: [],
      // validate: {
      //   validator(arr) {
      //     const keys = (arr || [])
      //       .filter((x) => x && x.sId != null)
      //       .map((x) => `${x.sId}||${_normC(x.c)}||${_normSp(x.sp)}`);
      //     return keys.length === new Set(keys).size;
      //   },
      //   message: "sku_info_list 중 (sId,c,sp) 조합이 중복되었습니다.",
      // },
    },
  },
  { _id: false, toJSON: { virtuals: true }, toObject: { virtuals: true } }
);
// normalizeCForCompare;
// normalizeSpForCompare;
// ─────────────────────────────────────────────────────────────────────────────
// 상품 1문서 = 1상품
const ProductDetailSchema = new mongoose.Schema(
  {
    _id: { type: String, alias: "productId", required: true },

    vol: {
      type: mongoose.Schema.Types.Int32,
      required: true,
      alias: "volume",
      set: toNumber,
      min: 0,
    },

    pl: { type: String, required: true, alias: "promotion_link" },

    // 카테고리는 기존 키명을 유지(외부 의존 코드 최소화)
    // 필요 시 c1/c2/c3 같은 짧은 키로 별도 alias 설계도 가능
    cId1: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "ProductCategories",
      required: true,
      alias: "category_id_1",
      index: true,
    },
    cId2: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "ProductCategories",
      alias: "category_id_2",
      index: true,
    },
    c1n: {
      type: "string",
      required: true,
      alias: "category_id_1_name",
    },
    c2n: {
      type: "string",
      required: true,
      alias: "category_id_2_name",
    },
    c3n: {
      type: "string",
      required: false,
      alias: "category_id_3_name",
    },

    tt: { type: String, required: true, alias: "title" },

    ps: {
      type: Number,
      required: true,
      alias: "product_score",
      set: toNumber,
      min: 0,
    },
    rn: {
      type: mongoose.Schema.Types.Int32,
      required: true,
      alias: "review_number",
      set: toNumber,
      min: 0,
    },

    il: { type: String, required: true, alias: "image_link" },
    ail: { type: [String], default: [], alias: "additional_image_links" },

    // sku_info 객체 내부에 실 저장키 'sil' 배열을 두고, alias로 'sku_info_list' 제공
    sku_info: { type: SkuInfoSchema, default: () => ({}) },
  },
  {
    versionKey: false,
    timestamps: false,
    id: false,
    toJSON: { virtuals: true },
    toObject: { virtuals: true },
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// 조회 가속 인덱스
ProductDetailSchema.index({ cId1: 1 }); // ObjectId ref
ProductDetailSchema.index({ cId2: 1 });

const ProductDetail =
  mongoose.models.ProductDetail ||
  mongoose.model("ProductDetail", ProductDetailSchema);

export default ProductDetail;
