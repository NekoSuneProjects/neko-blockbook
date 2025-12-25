import path from "node:path";
import os from "node:os";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import fsSync from "node:fs";

import { SequelizeStorage } from "./db/storage.js";
import { getBasePlatform, getPlatformKey } from "./core/platform.js";
import { downloadToFile } from "./core/downloader.js";
import { verifySha256 } from "./core/verify.js";
import { extractArchive } from "./core/extract.js";
import { writeConfig } from "./core/configWriter.js";
import { startNodeProcess } from "./core/processManager.js";
import { rpcCall } from "./core/rpc.js";
import { builtinChains } from "./chains/registry.js";
import type { ChainPlugin, NodeConfig, NodeCreateInput, RpcTarget } from "./types.js";

export interface NodeManagerOptions {
  baseDir?: string;
  storagePath?: string;
  chains?: ChainPlugin[];
}

export class NodeManager {
  private baseDir: string;
  private storage: SequelizeStorage;
  private chains: Map<string, ChainPlugin>;

  constructor(options: NodeManagerOptions = {}) {
    const baseDir = options.baseDir ?? this.resolveBaseDir();
    const storagePath = options.storagePath ?? path.join(baseDir, "db.sqlite");

    this.baseDir = baseDir;
    this.storage = new SequelizeStorage(storagePath);
    this.chains = new Map();

    for (const chain of builtinChains) {
      this.chains.set(chain.id, chain);
    }

    for (const chain of options.chains ?? []) {
      this.chains.set(chain.id, chain);
    }
  }

  private resolveBaseDir(): string {
    if (process.env.NEKO_EXPLORER_BASEDIR) {
      return process.env.NEKO_EXPLORER_BASEDIR;
    }

    if (this.isDocker()) {
      throw new Error(
        "Running in Docker without NEKO_EXPLORER_BASEDIR. Set it to a mounted volume path."
      );
    }

    return path.join(os.homedir(), ".neko-explorer");
  }

  private isDocker(): boolean {
    if (this.pathExists("/.dockerenv")) return true;
    try {
      const cgroup = fsSync.readFileSync("/proc/1/cgroup", "utf8");
      return cgroup.includes("docker") || cgroup.includes("containerd");
    } catch {
      return false;
    }
  }

  private pathExists(target: string): boolean {
    try {
      fsSync.accessSync(target);
      return true;
    } catch {
      return false;
    }
  }

  async init(): Promise<void> {
    await this.storage.init();
  }

  getStorage(): SequelizeStorage {
    return this.storage;
  }

  listChains(): ChainPlugin[] {
    return Array.from(this.chains.values());
  }

  getChain(chainId: string): ChainPlugin {
    const chain = this.chains.get(chainId);
    if (!chain) {
      throw new Error(`Unknown chain: ${chainId}`);
    }
    return chain;
  }

  async installCore(chainId: string, version: string): Promise<string> {
    const chain = this.getChain(chainId);
    const platformKey = getPlatformKey();
    const release = chain.releases[version]?.[platformKey];
    if (!release) {
      throw new Error(`No release for ${chainId} ${version} (${platformKey})`);
    }

    const coreDir = path.join(this.baseDir, "cores", chainId, version);
    const archivePath = path.join(coreDir, `core-${platformKey}.${release.archive}`);
    const extractDir = path.join(coreDir, "bin");

    await fs.mkdir(coreDir, { recursive: true });
    await downloadToFile(release.url, archivePath);
    if (!this.shouldSkipVerify()) {
      await verifySha256(archivePath, release.sha256);
    }
    await extractArchive(archivePath, extractDir, release.archive);

    return extractDir;
  }

  async createNode(input: NodeCreateInput): Promise<NodeConfig> {
    const chain = this.getChain(input.chain);
    if (await this.storage.nodeIdInUse(input.id)) {
      throw new Error(`Node ID already exists: ${input.id}`);
    }
    const coreVersion = input.coreVersion ?? this.getLatestVersion(chain);
    const { p2pPort, rpcPort } = await this.allocatePorts(
      input.p2pPort,
      input.rpcPort,
      chain
    );

    const node: NodeConfig = {
      id: input.id,
      chain: input.chain,
      datadir: path.join(this.baseDir, "nodes", input.id),
      p2pPort,
      rpcPort,
      rpcUser: this.randomToken(12),
      rpcPassword: this.randomToken(24),
      snapshotUrl: input.snapshotUrl,
      coreVersion,
      createdAt: new Date().toISOString()
    };

    await this.installCore(chain.id, coreVersion);
    await writeConfig(chain, node);

    await this.storage.addNode(node);
    await this.start(node.id);
    await this.waitForRpc(node.id);
    return node;
  }

  async start(id: string): Promise<void> {
    const node = await this.storage.getNode(id);
    const chain = this.getChain(node.chain);
    const daemonPath = await this.resolveDaemonPath(chain, node);
    const confPath = path.join(node.datadir, `${chain.id}.conf`);
    startNodeProcess({ daemonPath, datadir: node.datadir, confPath });
  }

  async stop(id: string): Promise<void> {
    const node = await this.storage.getNode(id);
    const chain = this.getChain(node.chain);
    await rpcCall(this.nodeToRpcTarget(node), chain.rpc.stop);
  }

  async restart(id: string): Promise<void> {
    await this.stop(id);
    await this.start(id);
  }

  async resync(id: string): Promise<void> {
    const node = await this.storage.getNode(id);
    await this.stop(id);
    await fs.rm(path.join(node.datadir, "blocks"), { recursive: true, force: true });
    await fs.rm(path.join(node.datadir, "chainstate"), { recursive: true, force: true });
    await this.start(id);
  }

  async deleteNode(id: string): Promise<void> {
    const node = await this.storage.getNode(id);
    try {
      await this.stop(id);
    } catch {
      // ignore stop errors
    }
    await fs.rm(node.datadir, { recursive: true, force: true });
    await this.storage.removeNode(id);
  }

  async getBlockCount(id: string): Promise<number> {
    const node = await this.storage.getNode(id);
    const chain = this.getChain(node.chain);
    return rpcCall(this.nodeToRpcTarget(node), chain.rpc.blockCount);
  }

  async getNode(id: string): Promise<NodeConfig> {
    return this.storage.getNode(id);
  }

  async listNodes(): Promise<NodeConfig[]> {
    return this.storage.listNodes();
  }

  async getBestNodeByChain(chain: string): Promise<NodeConfig> {
    const nodes = await this.storage.listNodesByChain(chain);
    if (!nodes.length) {
      throw new Error(`No nodes available for chain: ${chain}`);
    }

    let best = nodes[0];
    let bestHeight = -1;
    for (const node of nodes) {
      try {
        const height = await rpcCall<number>(this.nodeToRpcTarget(node), "getblockcount");
        if (height > bestHeight) {
          bestHeight = height;
          best = node;
        }
      } catch {
        continue;
      }
    }

    return best;
  }

  private nodeToRpcTarget(node: NodeConfig): RpcTarget {
    return {
      id: `node-${node.id}`,
      chain: node.chain,
      name: node.id,
      url: `http://127.0.0.1:${node.rpcPort}/`,
      username: node.rpcUser,
      password: node.rpcPassword
    };
  }

  private randomToken(bytes: number): string {
    return crypto.randomBytes(bytes).toString("hex");
  }

  private async resolveDaemonPath(
    chain: ChainPlugin,
    node: NodeConfig
  ): Promise<string> {
    if (node.daemonPath) {
      return node.daemonPath;
    }

    if (!node.coreVersion) {
      throw new Error(`Node ${node.id} missing coreVersion`);
    }

    const platformKey = getPlatformKey();
    const coreDir = path.join(this.baseDir, "cores", chain.id, node.coreVersion, "bin");
    const daemonName = chain.daemon[getBasePlatform(platformKey)];
    if (!daemonName) {
      throw new Error(`No daemon name for ${chain.id} on ${platformKey}`);
    }

    let found = await this.findBinary(coreDir, daemonName);
    if (!found) {
      await this.installCore(chain.id, node.coreVersion);
      found = await this.findBinary(coreDir, daemonName);
    }
    if (!found) {
      throw new Error(`Daemon not found: ${daemonName}`);
    }

    node.daemonPath = found;
    await this.storage.updateNode(node.id, { daemonPath: found });
    return found;
  }

  private async findBinary(rootDir: string, fileName: string): Promise<string | null> {
    const entries = await fs.readdir(rootDir, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(rootDir, entry.name);
      if (entry.isDirectory()) {
        const found = await this.findBinary(fullPath, fileName);
        if (found) {
          return found;
        }
      } else if (entry.isFile() && entry.name.toLowerCase() === fileName.toLowerCase()) {
        return fullPath;
      }
    }
    return null;
  }

  private async waitForRpc(nodeId: string): Promise<void> {
    const timeoutMs = 15000;
    const started = Date.now();
    while (Date.now() - started < timeoutMs) {
      try {
        await this.getBlockCount(nodeId);
        return;
      } catch {
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  }

  private async allocatePorts(
    p2pPort: number | undefined,
    rpcPort: number | undefined,
    chain: ChainPlugin
  ): Promise<{ p2pPort: number; rpcPort: number }> {
    const usedPorts = new Set<number>();
    const allNodes = await this.storage.listNodes();
    for (const node of allNodes) {
      usedPorts.add(node.p2pPort);
      usedPorts.add(node.rpcPort);
    }

    const pickPort = (start: number): number => {
      let port = start;
      while (usedPorts.has(port)) {
        port += 1;
      }
      usedPorts.add(port);
      return port;
    };

    const resolvedP2p = p2pPort ?? pickPort(chain.defaultPorts.p2p);
    const resolvedRpc = rpcPort ?? pickPort(chain.defaultPorts.rpc);

    if (resolvedP2p === resolvedRpc) {
      return { p2pPort: resolvedP2p, rpcPort: pickPort(resolvedRpc + 1) };
    }

    return { p2pPort: resolvedP2p, rpcPort: resolvedRpc };
  }

  private getLatestVersion(chain: ChainPlugin): string {
    const versions = Object.keys(chain.releases);
    if (versions.length === 0) {
      throw new Error(`No releases configured for ${chain.id}`);
    }
    return versions.sort(this.compareSemver).pop() as string;
  }

  private compareSemver(a: string, b: string): number {
    const parse = (value: string) => value.split(".").map((part) => Number(part) || 0);
    const aParts = parse(a);
    const bParts = parse(b);
    const length = Math.max(aParts.length, bParts.length);
    for (let i = 0; i < length; i += 1) {
      const diff = (aParts[i] ?? 0) - (bParts[i] ?? 0);
      if (diff !== 0) return diff;
    }
    return 0;
  }

  private shouldSkipVerify(): boolean {
    return process.env.SKIP_VERIFY === "1";
  }
}
