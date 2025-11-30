// scripts/removeTForOne.js
import ProductDetail from "./models/ProductDetail.js"; // 경로는 프로젝트 구조에 맞게 조정
import dbConnect from "./utils/dbConnect.js";

const TEST_PRODUCT_ID = "1005008078761095"; // 👉 실제 _id 넣기

async function main() {
  await dbConnect();

  // 변경 전 확인용
  // const before = await ProductDetail.findById(TEST_PRODUCT_ID).lean();
  // console.log("=== BEFORE ===");
  // console.dir(before?.sku_info?.sil, { depth: 5 });

  // // aggregation pipeline update로 sku_info.sil[].pd 내부의 t만 제거
  // const res = await ProductDetail.updateOne({ _id: TEST_PRODUCT_ID }, [
  //   {
  //     $set: {
  //       "sku_info.sil": {
  //         $map: {
  //           input: "$sku_info.sil",
  //           as: "sku",
  //           in: {
  //             $mergeObjects: [
  //               "$$sku",
  //               {
  //                 pd: {
  //                   $arrayToObject: {
  //                     $map: {
  //                       input: {
  //                         // Map(pd)를 [ {k, v}, ... ] 배열로 변환
  //                         $objectToArray: {
  //                           $ifNull: ["$$sku.pd", {}],
  //                         },
  //                       },
  //                       as: "pp",
  //                       in: {
  //                         k: "$$pp.k", // 날짜 키 그대로 유지
  //                         v: {
  //                           // 값 객체에서 s만 남기고 t 제거
  //                           s: "$$pp.v.s",
  //                         },
  //                       },
  //                     },
  //                   },
  //                 },
  //               },
  //             ],
  //           },
  //         },
  //       },
  //     },
  //   },
  // ]);

  // console.log("Matched:", res.matchedCount, "Modified:", res.modifiedCount);

  // const after = await ProductDetail.findById(TEST_PRODUCT_ID).lean();
  // console.log("=== AFTER ===");
  // console.dir(after?.sku_info?.sil, { depth: 5 });

  // console.log("MongoDB disconnected");

  await ProductDetail.updateMany(
    {}, // 전체
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
                        $map: {
                          input: {
                            $objectToArray: {
                              $ifNull: ["$$sku.pd", {}],
                            },
                          },
                          as: "pp",
                          in: {
                            k: "$$pp.k",
                            v: {
                              s: "$$pp.v.s",
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
  console.log("MongoDB disconnected");
}

main()
  .then(() => {
    process.exit(0);
  })
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
