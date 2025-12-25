import path from "node:path";
import os from "node:os";
import bcrypt from "bcryptjs";
import fs from "node:fs/promises";

import { NodeManager } from "./manager.js";
import { createApp } from "./server/app.js";
import { ChainIndexer } from "./indexer.js";

async function loadDotEnv(filePath: string) {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    for (const line of raw.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) {
        continue;
      }
      const eq = trimmed.indexOf("=");
      if (eq === -1) {
        continue;
      }
      const key = trimmed.slice(0, eq).trim();
      let value = trimmed.slice(eq + 1).trim();
      if (
        (value.startsWith("\"") && value.endsWith("\"")) ||
        (value.startsWith("'") && value.endsWith("'"))
      ) {
        value = value.slice(1, -1);
      }
      if (process.env[key] === undefined) {
        process.env[key] = value;
      }
    }
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== "ENOENT") {
      throw err;
    }
  }
}

await loadDotEnv(path.join(process.cwd(), ".env"));

const manager = new NodeManager();
const storage = manager.getStorage();

await manager.init();

const adminUser = process.env.NEKO_EXPLORER_ADMIN_USER;
const adminPass = process.env.NEKO_EXPLORER_ADMIN_PASS;
if (!(await storage.adminExists())) {
  if (!adminUser || !adminPass) {
    throw new Error(
      "NEKO_EXPLORER_ADMIN_USER and NEKO_EXPLORER_ADMIN_PASS must be set to bootstrap admin"
    );
  }
  const hash = await bcrypt.hash(adminPass, 10);
  await storage.createAdmin(adminUser, hash, true);
}

const baseDir = process.env.NEKO_EXPLORER_BASEDIR ?? path.join(os.homedir(), ".neko-explorer");
await fs.mkdir(baseDir, { recursive: true });
const app = createApp(manager, storage, {
  sessionDbPath: baseDir
});

const indexer = new ChainIndexer(manager, storage, {
  pollMs: Number(process.env.NEKO_EXPLORER_INDEX_POLL_MS ?? 8000),
  batchSize: Number(process.env.NEKO_EXPLORER_INDEX_BATCH ?? 50)
});
indexer.start();

const port = Number(process.env.PORT ?? 4200);
app.listen(port, () => {
  console.log(`Neko Explorer listening on http://localhost:${port}`);
});
