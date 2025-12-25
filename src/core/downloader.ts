import fs from "node:fs";
import path from "node:path";
import { pipeline } from "node:stream/promises";

export async function downloadToFile(url: string, destPath: string): Promise<void> {
  const res = await fetch(url);
  if (!res.ok || !res.body) {
    throw new Error(`Download failed: ${res.status} ${res.statusText}`);
  }

  await fs.promises.mkdir(path.dirname(destPath), { recursive: true });
  const file = fs.createWriteStream(destPath);
  await pipeline(res.body as any, file);
}