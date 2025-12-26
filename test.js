import ProductDetail from "./models/ProductDetail.js"; // 경로에 맞게 수정
import dbConnect from "./utils/dbConnect.js";

async function getTotalSkuCount() {
  await dbConnect();
  const result = await ProductDetail.aggregate([
    {
      // 1단계: 각 문서별로 'sil' 배열의 길이를 계산하여 'count' 필드로 임시 생성
      $project: {
        count: {
          $size: {
            // 배열이 null이거나 없을 경우를 대비해 빈 배열로 처리
            $ifNull: ["$sku_info.sil", []],
          },
        },
      },
    },
    {
      // 2단계: 위에서 구한 각 문서의 'count'를 모두 더함
      $group: {
        _id: null, // 전체 문서를 하나의 그룹으로 묶음
        totalSkuCount: { $sum: "$count" },
      },
    },
  ]);

  // 결과가 있으면 그 값을, 없으면 0을 반환
  return result.length > 0 ? result[0].totalSkuCount : 0;
}

// 실행 예시
getTotalSkuCount().then((count) => {
  console.log(`전체 SKU 개수: ${count}`);
});
