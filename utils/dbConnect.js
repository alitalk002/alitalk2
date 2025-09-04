import mongoose from "mongoose";
import "dotenv/config";

// const MONGODB_URI = "mongodb://127.0.0.1:27017/AliPrice"; // 로컬
const MONGODB_URI = process.env.MONGODB_URL; // 원격
if (!MONGODB_URI) {
  console.error("MONGODB_URI is not defined");
  throw new Error(
    "Please define the MONGODB_URI environment variable inside app.yaml"
  );
} else {
  // console.log("MONGODB_URI:", MONGODB_URI); // 환경 변수 로깅
}

/**
 * Global is used here to maintain a cached connection across hot reloads
 * in development. This prevents connections growing exponentially
 * during API Route usage.
 */
let cached = global.mongoose;

if (!cached) {
  cached = global.mongoose = { conn: null, promise: null };
}

async function dbConnect() {
  if (cached.conn) {
    return cached.conn;
  }

  if (!cached.promise) {
    const opts = {
      bufferCommands: false, // 이 옵션은 Mongoose가 몽고DB 커맨드를 버퍼링하지 않도록 합니다.
    };

    cached.promise = mongoose.connect(MONGODB_URI, opts).then((mongoose) => {
      return mongoose;
    });
  }

  cached.conn = await cached.promise;

  console.log("DBCONNECT:");

  return cached.conn;
}

export default dbConnect;
