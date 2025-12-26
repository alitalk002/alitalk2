// scripts/purgeOldPricePoints.js
import mongoose from "mongoose";
import ProductDetail from "./models/ProductDetail.js";
import dbConnect from "./utils/dbConnect.js";

async function run() {
  await dbConnect();

  // 기준 시각: 60일 전 pd값 전부 삭제
  const now = new Date();
  const cutoff = new Date(now);
  cutoff.setDate(cutoff.getDate() - 61);

  const res = await ProductDetail.updateMany({}, [
    {
      $set: {
        "sku_info.sil": {
          $let: {
            vars: {
              cleaned: {
                $map: {
                  input: { $ifNull: ["$sku_info.sil", []] },
                  as: "s",
                  in: {
                    $mergeObjects: [
                      "$$s",
                      {
                        pd: {
                          $arrayToObject: {
                            $filter: {
                              input: {
                                $objectToArray: { $ifNull: ["$$s.pd", {}] },
                              },
                              as: "pp",
                              cond: {
                                $gte: [
                                  {
                                    $dateFromString: {
                                      dateString: "$$pp.k",
                                      onError: new Date(0),
                                      onNull: new Date(0),
                                    },
                                  },
                                  cutoff,
                                ],
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
            in: {
              // 정리 후 pd가 비어버린 SKU는 제거
              $filter: {
                input: "$$cleaned",
                as: "s",
                cond: {
                  $gt: [
                    {
                      $size: {
                        $objectToArray: { $ifNull: ["$$s.pd", {}] },
                      },
                    },
                    0,
                  ],
                },
              },
            },
          },
        },
      },
    },
  ]);

  console.log(
    `matched: ${res.matchedCount ?? res.n}, modified: ${
      res.modifiedCount ?? res.nModified
    }`
  );

  await mongoose.disconnect();
}

run().catch((e) => {
  console.error(e);
  process.exit(1);
});
