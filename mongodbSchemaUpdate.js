// scripts/strip_fields_st_link_p.js
import mongoose from "mongoose";
import dotenv from "dotenv";
import dbConnect from "./utils/dbConnect.js";
dotenv.config();

const collName = process.env.COLL || "productdetails";
const BATCH = Number(process.env.BATCH || 500);

function stripFields(doc) {
  const set = {};
  const unset = {};

  // st 제거
  if (doc.st !== undefined) unset.st = "";

  // sku_info.sil[*].link 제거 + pd.*.p 제거
  const sil = doc?.sku_info?.sil;
  if (Array.isArray(sil)) {
    let changed = false;
    const newSil = sil.map((it) => {
      if (!it || typeof it !== "object") return it;
      const clone = { ...it };
      let ch = false;

      if (clone.link !== undefined) {
        delete clone.link;
        ch = true;
      }

      if (
        clone.pd &&
        typeof clone.pd === "object" &&
        !Array.isArray(clone.pd)
      ) {
        const newPd = {};
        let pdCh = false;
        for (const [k, v] of Object.entries(clone.pd)) {
          if (v && typeof v === "object" && !Array.isArray(v)) {
            const { p, ...rest } = v; // p만 제거
            if ("p" in v) pdCh = true;
            newPd[k] = rest;
          } else {
            newPd[k] = v;
          }
        }
        if (pdCh) {
          clone.pd = newPd;
          ch = true;
        }
      }

      if (ch) changed = true;
      return ch ? clone : it;
    });

    if (changed) set["sku_info.sil"] = newSil;
  }

  return {
    set,
    unset,
    changed: !!(Object.keys(set).length || Object.keys(unset).length),
  };
}

async function run() {
  // ✅ dbConnect 내부에서 mongoose.connect 수행한다고 가정
  await dbConnect();

  // ✅ 여기서는 mongoose.connection 사용 (conn 아님!)
  const db = mongoose.connection.db;
  const col = db.collection(collName);

  const cursor = col.find({}, { projection: { _id: 1, sku_info: 1, st: 1 } });

  let ops = [];
  let scanned = 0;
  let modified = 0;

  while (await cursor.hasNext()) {
    const d = await cursor.next();
    scanned++;

    const { set, unset, changed } = stripFields(d);
    if (!changed) continue;

    modified++;
    const update = {};
    if (Object.keys(set).length) update.$set = set;
    if (Object.keys(unset).length) update.$unset = unset;

    ops.push({ updateOne: { filter: { _id: d._id }, update } });

    if (ops.length >= BATCH) {
      await col.bulkWrite(ops, { ordered: false });
      ops = [];
    }
  }

  if (ops.length) {
    await col.bulkWrite(ops, { ordered: false });
  }

  console.log(`Done. scanned=${scanned}, modified=${modified}`);
  await mongoose.disconnect();
}

run().catch((e) => {
  console.error(e);
  process.exit(1);
});
