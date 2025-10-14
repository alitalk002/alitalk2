// scripts/backfill-sku-keys.mjs
// node >=18, "type": "module" 권장
import mongoose from "mongoose";

// ✨ 여기서 설정하세요
const CONFIG = {
  // 처리할 문서 _id 배열. 비우면 전체 처리.
  TEST_IDS: [],
  // true면 cKey/spKey가 비어있는 SKU만 채움 (기존 로직 유지)
  ONLY_MISSING: false,
  // true면 실제 쓰기 없이 변경사항만 로그
  DRY_RUN: false,
  // 벌크 배치 크기
  BATCH_SIZE: 500,
  // 빈 문자열('') 또는 미존재(normalize 결과가 비어있음)면 SET 하지 않음
  SKIP_EMPTY_KEYS: true,
};

import ProductDetail from "./models/ProductDetail.js";
import {
  normalizeCForCompare,
  normalizeSpForCompare,
} from "./utils/normalize.js";
import dbConnect from "./utils/dbConnect.js";

async function backfillSkuKeys() {
  await dbConnect();
  //   let res = await ProductDetail.find({ cId1: "68b7286ae5b56421d2712568" })
  //     .populate("cId1", "cId cn")
  //     .populate("cId2", "cId cn")
  //     .lean({ virtuals: true });

  //   for (let id of res) {
  //     CONFIG.TEST_IDS.push(id._id);
  //   }

  const projection = {
    _id: 1,
    "sku_info.sil.c": 1,
    "sku_info.sil.sp": 1,
    "sku_info.sil.cKey": 1,
    "sku_info.sil.spKey": 1,
  };

  // ID 필터 (단일/다중/없음)
  let idFilter = {};
  if (Array.isArray(CONFIG.TEST_IDS) && CONFIG.TEST_IDS.length > 0) {
    idFilter =
      CONFIG.TEST_IDS.length === 1
        ? { _id: CONFIG.TEST_IDS[0] }
        : { _id: { $in: CONFIG.TEST_IDS } };
  }

  // ONLY_MISSING 조건: cKey/spKey가 비어있는 SKU가 존재하는 문서만
  const onlyMissingFilter = CONFIG.ONLY_MISSING
    ? {
        "sku_info.sil": {
          $elemMatch: {
            $or: [{ cKey: { $exists: false } }, { spKey: { $exists: false } }],
          },
        },
      }
    : {};

  const filter = { ...idFilter, ...onlyMissingFilter };

  console.log(
    `Running backfill with filter: ${JSON.stringify(filter)} ${
      CONFIG.DRY_RUN ? "(DRY-RUN)" : ""
    }`
  );

  const cursor = ProductDetail.find(filter, projection).lean().cursor();

  let ops = [];
  let seen = 0;
  let updatedDocs = 0;
  let updatedItems = 0;

  for await (const doc of cursor) {
    seen++;
    const sil = doc?.sku_info?.sil || [];
    if (!Array.isArray(sil) || sil.length === 0) continue;

    const setPayload = {};
    let changedCountForDoc = 0;

    sil.forEach((item, idx) => {
      const c = item?.c ?? "";
      const sp = item?.sp ?? "";

      // 정규화 결과를 문자열로 안전 변환 + 트림
      const newSpKeyRaw = normalizeSpForCompare(sp);
      const newSpKey = (newSpKeyRaw == null ? "" : String(newSpKeyRaw)).trim();

      const pathS = `sku_info.sil.${idx}.spKey`;

      // 👇 핵심 규칙: 결과가 빈문자('')면 SET 하지 않음

      const canSetS =
        (!CONFIG.SKIP_EMPTY_KEYS || newSpKey.length > 0) &&
        (!CONFIG.ONLY_MISSING || item?.spKey == null || item?.spKey === "") &&
        item?.spKey !== newSpKey;

      if (canSetS) {
        setPayload[pathS] = newSpKey;
        changedCountForDoc++;
        updatedItems++;
      }
    });

    if (changedCountForDoc > 0) {
      if (CONFIG.DRY_RUN) {
        console.log(`[DRY] _id=${doc._id} $set:`, setPayload);
      } else {
        ops.push({
          updateOne: {
            filter: { _id: doc._id },
            update: { $set: setPayload },
            upsert: false,
          },
        });
        updatedDocs++;
      }
    }

    if (!CONFIG.DRY_RUN && ops.length >= CONFIG.BATCH_SIZE) {
      await ProductDetail.bulkWrite(ops, { ordered: false });
      console.log(
        `progress: seen=${seen}, updatedDocs=${updatedDocs}, updatedItems=${updatedItems}`
      );
      ops = [];
    }
  }

  if (!CONFIG.DRY_RUN && ops.length > 0) {
    await ProductDetail.bulkWrite(ops, { ordered: false });
  }

  if (seen === 0) {
    console.log("No documents matched the filter. 🤔");
  }

  console.log(
    `Done ✅ seen=${seen}, updatedDocs=${updatedDocs}, updatedItems=${updatedItems} ${
      CONFIG.DRY_RUN ? "(DRY-RUN, no writes)" : ""
    }`
  );

  await mongoose.disconnect();
}

// 실행
backfillSkuKeys()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error("Error ❗", err);
    process.exit(1);
  });
