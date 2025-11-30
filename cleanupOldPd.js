// cleanupOldPd.js
import mongoose from "mongoose";
import ProductDetail from "./models/ProductDetail.js"; // <= 경로 수정 필수!
import dbConnect from "./utils/dbConnect.js";
import { getProductDetailsById } from "./getProductDetailById.js";
import { withRetry } from "./utils/withRetry.js";

// --- MongoDB 연결 URI 설정 ---

// --- 기준 일수(오늘 기준 65일 이전 것 삭제) ---
const THRESHOLD_DAYS = 65;
let productId;
const isPlList = [];

const tryCatch = async (fn) => {
  try {
    return { ok: true, value: await fn() };
  } catch (e) {
    return { ok: false, error: e };
  }
};

async function main() {
  await dbConnect();

  const now = Date.now();
  const THRESHOLD_MS = THRESHOLD_DAYS * 24 * 60 * 60 * 1000;

  // pd가 존재하는 문서들만 대상으로 커서 생성
  const cursor = ProductDetail.find({}).cursor();

  let docCount = 0;
  let updatedDocs = 0;
  let removedCount = 0;

  for (let doc = await cursor.next(); doc != null; doc = await cursor.next()) {
    productId = doc._id;
    docCount += 1;
    let changed = false;

    // 간혹 pl 값이 없는것들이 몇개있음

    if (!doc.pl && doc.pl === "") {
      console.log("doc:", doc);
      try {
        const pdRes = await tryCatch(() =>
          withRetry(() => getProductDetailsById(doc._id), {
            retries: 2,
            base: 800,
            max: 10000,
          })
        );
        console.log("pdRes:", pdRes);
        doc.pl = pdRes.items[0]._raw.promotion_link;
        if (!pdRes) {
          continue;
        }
      } catch (err) {
        isPlList.push(productId);
        console.log(err);
        continue;
      }
    }

    const skuList = doc.sku_info?.sil || [];

    for (const sku of skuList) {
      if (!sku.pd) continue;

      // Mongoose Map은 for..of 로 순회 가능 ( [key, value] )
      const entriesToDelete = [];

      for (const [dateKey, pricePoint] of sku.pd.entries()) {
        let basisDate = null;

        // 1) 먼저 key (예: "2025-09-01T00:00:00.000Z")를 Date로 파싱
        const dFromKey = new Date(dateKey);
        if (!Number.isNaN(dFromKey.getTime())) {
          basisDate = dFromKey;
        } else if (pricePoint && pricePoint.t) {
          // 2) key 파싱 실패 시, 서브 도큐먼트의 t 필드(collected_at) 사용
          const dFromT = new Date(pricePoint.t);
          if (!Number.isNaN(dFromT.getTime())) {
            basisDate = dFromT;
          }
        }

        if (!basisDate) continue;

        const diffMs = now - basisDate.getTime();

        // 👉 "오늘 기준 70일이 지난 것" (70일 이상 지난 것만 삭제)
        if (diffMs >= THRESHOLD_MS) {
          entriesToDelete.push(dateKey);
        }
      }

      if (entriesToDelete.length > 0) {
        for (const k of entriesToDelete) {
          sku.pd.delete(k); // Map에서 해당 key 삭제
          removedCount += 1;
        }
        changed = true;
      }
    }

    if (changed) {
      await doc.save();
      updatedDocs += 1;
      console.log(
        `[업데이트] productId=${doc._id} 의 오래된 pd 항목들 제거 완료`
      );
    }
  }

  console.log("=================================");
  console.log("총 조회 문서 수:", docCount);
  console.log("실제 업데이트된 문서 수:", updatedDocs);
  console.log("삭제된 pd 항목 개수:", removedCount);
  console.log("=================================");

  await mongoose.disconnect();
  console.log("MongoDB 연결 종료");
}

// 실행
main()
  .then(() => {
    console.log("정리 작업 완료 ✅");
    console.log("plList:", isPlList);
    process.exit(0);
  })
  .catch((err) => {
    console.error("에러 발생 ❗", err);
    console.log("productId", productId);
    process.exit(1);
  });
