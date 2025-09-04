import data from "./categorieList_kr_all_v1.json" assert { type: "json" };
import ProductCategories from "./models/productCategories.js";
import dbConnect from "./utils/dbConnect.js";

async function inputData() {
  await dbConnect();

  for (const item of data) {
    const catId = String(item?.cId ?? "").trim();
    const name = String(item?.cn ?? "").trim();

    // 필수값 검증
    if (!catId || !name) continue;

    await ProductCategories.updateOne(
      { cId: catId }, // upsert 기준 키는 cId만!
      { $set: { cn: name } }, // 이름은 최신으로 갱신
      { upsert: true }
    );
  }

  console.log("done");
}

inputData();
