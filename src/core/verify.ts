import crypto from "node:crypto";
import fs from "node:fs";

export async function sha256File(filePath: string): Promise<string> {
  const hash = crypto.createHash("sha256");
  const stream = fs.createReadStream(filePath);
  await new Promise<void>((resolve, reject) => {
    stream.on("data", (chunk) => hash.update(chunk));
    stream.on("end", resolve);
    stream.on("error", reject);
  });
  return hash.digest("hex");
}

export async function verifySha256(filePath: string, expected: string): Promise<void> {
  const actual = await sha256File(filePath);
  if (actual.toLowerCase() !== expected.toLowerCase()) {
    throw new Error(`SHA256 mismatch for ${filePath}`);
  }
}