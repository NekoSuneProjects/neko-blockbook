import fs from "node:fs/promises";
import path from "node:path";
import type { ChainPlugin, NodeConfig } from "../types.js";

export async function writeConfig(chain: ChainPlugin, node: NodeConfig): Promise<void> {
  const content = `${chain.config(node).trim()}\n`;
  await fs.mkdir(node.datadir, { recursive: true });
  const confPath = path.join(node.datadir, `${chain.id}.conf`);
  await fs.writeFile(confPath, content, { mode: 0o600 });
}