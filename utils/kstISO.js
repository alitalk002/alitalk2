export const kstISO = (d = new Date()) =>
  new Date(d.getTime() + 9 * 60 * 60 * 1000)
    .toISOString()
    .replace("Z", "+09:00");
