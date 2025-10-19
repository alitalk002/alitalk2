// scripts/migrate-normalize-c-client-side.js
import mongoose from "mongoose";
import ProductDetail from "./models/ProductDetail.js";
import { normalizeCForCompare } from "./utils/normalize.js";
import dbConnect from "./utils/dbConnect.js";

// 안전한 bulkWrite 커밋 함수 (작은 청크로 폴백)
async function commitBulk(ops, { ordered = false } = {}) {
  if (!ops.length) return { matchedCount: 0, modifiedCount: 0 };

  try {
    const res = await ProductDetail.bulkWrite(ops, { ordered });
    // 필요한 정보만 출력 (큰 객체 dump 금지)
    console.log(
      `bulk ok - matched:${res.matchedCount ?? 0} modified:${
        res.modifiedCount ?? 0
      }`
    );
    return res;
  } catch (e) {
    // offset 에러 등 발생 시 더 작은 청크로 나눠 재시도
    console.warn("bulk failed, retrying with smaller chunks...", e?.message);
    if (ops.length === 1) throw e; // 더 못 쪼갬

    const mid = Math.floor(ops.length / 2);
    const left = ops.slice(0, mid);
    const right = ops.slice(mid);
    const r1 = await commitBulk(left, { ordered });
    const r2 = await commitBulk(right, { ordered });
    return {
      matchedCount: (r1.matchedCount ?? 0) + (r2.matchedCount ?? 0),
      modifiedCount: (r1.modifiedCount ?? 0) + (r2.modifiedCount ?? 0),
    };
  }
}

async function main() {
  await dbConnect();

  // lean() + 최소 필드만
  const cursor = ProductDetail.find({}, { "sku_info.sil": 1 }).lean().cursor();

  const BATCH_LIMIT = 100; // 1000 -> 100 으로 축소
  let ops = [];
  let scanned = 0;
  let changedDocs = 0;

  for await (const doc of cursor) {
    scanned++;
    const orig = Array.isArray(doc?.sku_info?.sil) ? doc.sku_info.sil : [];
    if (!orig.length) continue;

    let touched = false;

    // orig는 lean이므로 plain object
    const nextSil = orig.map((it) => {
      const before = it?.c ?? "";
      const after = normalizeCForCompare(before);
      if (before !== after) touched = true;
      return { ...it, c: after };
    });

    if (!touched) continue;

    changedDocs++;
    // 디버깅 로깅은 최소화 (id 정도만)
    console.log("update id:", doc._id);

    ops.push({
      updateOne: {
        filter: { _id: doc._id },
        update: { $set: { "sku_info.sil": nextSil } },
      },
    });

    if (ops.length >= BATCH_LIMIT) {
      await commitBulk(ops, { ordered: false });
      ops = [];
    }
  }

  if (ops.length) {
    await commitBulk(ops, { ordered: false });
  }

  console.log(`Scanned: ${scanned}, Changed docs: ${changedDocs}`);
  await mongoose.disconnect();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
