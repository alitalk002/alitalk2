// scripts/purgeOldPricePoints.js
import mongoose from "mongoose";
import ProductDetail from "./models/ProductDetail.js";
import dbConnect from "./utils/dbConnect.js";

async function run() {
  await dbConnect();

  // 기준 시각: 한 달 전
  const now = new Date();
  const monthAgo = new Date(now);
  monthAgo.setMonth(monthAgo.getMonth() - 1);

  const res = await ProductDetail.updateMany(
    {}, // 필요 시 조건 변경
    [
      {
        $set: {
          "sku_info.sil": {
            $filter: {
              input: { $ifNull: ["$sku_info.sil", []] },
              as: "s",
              cond: {
                // 이 SKU의 pd 중 monthAgo 이상인 포인트가 1개 이상이면 유지
                $gt: [
                  {
                    $size: {
                      $filter: {
                        input: {
                          $objectToArray: { $ifNull: ["$$s.pd", {}] },
                        },
                        as: "pp",
                        cond: {
                          $gte: [
                            { $toDate: { $ifNull: ["$$pp.v.t", new Date(0)] } },
                            monthAgo,
                          ],
                        },
                      },
                    },
                  },
                  0,
                ],
              },
            },
          },
        },
      },
    ]
  );

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
