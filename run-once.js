// run-once utility (Node.js / Mongoose)
import mongoose from "mongoose";
import ProductDetail from "./models/ProductDetail.js"; // 사용하신 파일 경로에 맞춰주세요
import dbConnect from "./utils/dbConnect.js";

async function removeSalePrice1000OnDates() {
  // 대상 날짜들 (접두사 매칭: 'YYYY-MM-DD')
  const datePrefixes = ["2025-09-11", "2025-09-12", "2025-09-13", "2025-09-14"];
  const TARGET_ID = "1005008810647750";
  // sale_price_with_tax == 1000 제거
  const SALE_PRICE_TO_REMOVE = 1000;

  // 집계 파이프라인 업데이트 (MongoDB 4.2+)
  // - 각 SKU(sku_info.sil[*])의 pd(Map)를 object<->array 변환하여 필터링 후 재구성
  // - 키가 위 날짜 접두사 중 하나이고, 값.v.s == 1000인 항목만 제거
  const res = await ProductDetail.updateMany(
    {}, // 필요 시 특정 상품/카테고리로 범위 제한 가능
    [
      {
        $set: {
          "sku_info.sil": {
            $map: {
              input: "$sku_info.sil",
              as: "sku",
              in: {
                $mergeObjects: [
                  "$$sku",
                  {
                    pd: {
                      $arrayToObject: {
                        $filter: {
                          input: {
                            $objectToArray: { $ifNull: ["$$sku.pd", {}] },
                          },
                          as: "pp",
                          cond: {
                            // NOT( (key in targetDates) AND (value.s == 1000) )
                            $not: {
                              $and: [
                                {
                                  $in: [
                                    {
                                      // 'YYYY-MM-DD' 접두사 추출
                                      $substrBytes: ["$$pp.k", 0, 10],
                                    },
                                    datePrefixes,
                                  ],
                                },
                                { $eq: ["$$pp.v.s", SALE_PRICE_TO_REMOVE] },
                              ],
                            },
                          },
                        },
                      },
                    },
                  },
                ],
              },
            },
          },
        },
      },
    ]
  );

  console.log(`matched: ${res.matchedCount}, modified: ${res.modifiedCount}`);
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
