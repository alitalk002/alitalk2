// utils/dateKeyKST.js
export function dateKeyKST(d = new Date()) {
  const kst = new Date(d.getTime() + 9 * 60 * 60 * 1000); // +9h
  const y = kst.getUTCFullYear();
  const m = String(kst.getUTCMonth() + 1).padStart(2, "0");
  const day = String(kst.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`; // "YYYY-MM-DD"
}
