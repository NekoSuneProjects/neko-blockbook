import type { NodeManager } from "./manager.js";
import type { SequelizeStorage } from "./db/storage.js";
import type { RpcTarget } from "./types.js";
import { rpcCallWithFallback } from "./core/rpc.js";

interface IndexerOptions {
  pollMs?: number;
  batchSize?: number;
}

export class ChainIndexer {
  private manager: NodeManager;
  private storage: SequelizeStorage;
  private pollMs: number;
  private batchSize: number;
  private running: Map<string, boolean>;

  constructor(manager: NodeManager, storage: SequelizeStorage, options: IndexerOptions = {}) {
    this.manager = manager;
    this.storage = storage;
    this.pollMs = options.pollMs ?? 8000;
    this.batchSize = options.batchSize ?? 50;
    this.running = new Map();
  }

  start(): void {
    setInterval(() => {
      for (const chain of this.manager.listChains()) {
        void this.syncChain(chain.id);
      }
    }, this.pollMs);
  }

  private async syncChain(chainId: string): Promise<void> {
    if (this.running.get(chainId)) return;
    this.running.set(chainId, true);
    try {
      try {
        const targets = await this.getRpcTargets(chainId);
        if (targets.length === 0) return;

        const bestHeight = Number((await rpcCallWithFallback<number>(targets, "getblockcount")).result);
        const stored = await this.storage.getChainSync(chainId);
        let lastHeight = stored?.lastHeight ?? -1;

        if (lastHeight >= 0 && stored?.lastBlockHash) {
          try {
            const currentHash = await rpcCallWithFallback<string>(targets, "getblockhash", [
              lastHeight
            ]);
            if (currentHash.result !== stored.lastBlockHash) {
              lastHeight = -1;
            }
          } catch {
            lastHeight = -1;
          }
        }

        const targetHeight = Math.min(bestHeight, lastHeight + this.batchSize);
        for (let height = lastHeight + 1; height <= targetHeight; height += 1) {
          const hash = (await rpcCallWithFallback<string>(targets, "getblockhash", [height]))
            .result;
          const block = await this.fetchBlockVerbose(targets, hash);
          await this.storage.saveBlock(chainId, hash, block?.height ?? height, block);
          await this.indexBlock(chainId, block);
          await this.storage.upsertChainSync(chainId, height, hash);
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.error(`[indexer:${chainId}] ${message}`);
      }
    } finally {
      this.running.set(chainId, false);
    }
  }

  private async indexBlock(chainId: string, block: any): Promise<void> {
    const txs = Array.isArray(block?.tx) ? block.tx : [];
    for (const tx of txs) {
      if (tx?.txid) {
        await this.storage.saveTx(chainId, tx.txid, tx);
      }
      const touched = new Set<string>();
      const outputs = Array.isArray(tx?.vout) ? tx.vout : [];
      for (const out of outputs) {
        const amount = toSatsFromOutput(out);
        const addresses = extractAddresses(out?.scriptPubKey);
        for (const address of addresses) {
          touched.add(address);
          await this.storage.applyAddressDelta(chainId, address, amount, 0n, amount);
        }
      }

      const inputs = Array.isArray(tx?.vin) ? tx.vin : [];
      for (const input of inputs) {
        if (input?.coinbase) continue;
        const prevTxid = input?.txid;
        const voutIndex = input?.vout;
        if (!prevTxid && prevTxid !== 0) continue;
        if (voutIndex === undefined || voutIndex === null) continue;
        const prev = await this.fetchPrevOut(chainId, prevTxid);
        const prevOut = Array.isArray(prev?.vout) ? prev.vout[voutIndex] : null;
        if (!prevOut) continue;
        const amount = toSatsFromOutput(prevOut);
        const addresses = extractAddresses(prevOut?.scriptPubKey);
        for (const address of addresses) {
          touched.add(address);
          await this.storage.applyAddressDelta(chainId, address, 0n, amount, -amount);
        }
      }

      for (const address of touched) {
        await this.storage.addAddressTx(chainId, address, tx.txid, block?.height ?? null);
        await this.storage.incrementAddressTxCount(chainId, address);
      }
    }
  }

  private async fetchPrevOut(chainId: string, txid: string): Promise<any> {
    const targets = await this.getRpcTargets(chainId);
    if (targets.length === 0) {
      throw new Error(`No RPC endpoints for chain: ${chainId}`);
    }
    return this.fetchTransaction(targets, txid);
  }

  private async fetchBlockVerbose(targets: RpcTarget[], blockHash: string): Promise<any> {
    try {
      return (await rpcCallWithFallback<any>(targets, "getblock", [blockHash, 2])).result;
    } catch (error) {
      const message = error instanceof Error ? error.message.toLowerCase() : "";
      if (!message.includes("boolean")) {
        throw error;
      }
    }

    const fallback = (await rpcCallWithFallback<any>(targets, "getblock", [blockHash, true])).result;
    const txs = Array.isArray(fallback?.tx) ? fallback.tx : [];
    if (!txs.length || typeof txs[0] !== "string") {
      return fallback;
    }

    const resolved = [];
    for (const txid of txs) {
      try {
        const tx = await this.fetchTransaction(targets, txid);
        resolved.push(tx);
      } catch {
        resolved.push({ txid });
      }
    }
    return { ...fallback, tx: resolved };
  }

  private async getRpcTargets(chainId: string): Promise<RpcTarget[]> {
    const endpoints = (await this.storage.listRpcEndpoints(chainId))
      .filter((entry) => entry.enabled)
      .map((entry) => ({
        id: `rpc-${entry.id}`,
        chain: entry.chain,
        name: entry.name,
        url: entry.url,
        username: entry.username,
        password: entry.password
      }));

    const nodes = (await this.storage.listNodesByChain(chainId)).map((node) => ({
      id: `node-${node.id}`,
      chain: node.chain,
      name: `node:${node.id}`,
      url: `http://127.0.0.1:${node.rpcPort}/`,
      username: node.rpcUser,
      password: node.rpcPassword
    }));

    return [...endpoints, ...nodes];
  }

  private async fetchTransaction(targets: RpcTarget[], txid: string): Promise<any> {
    try {
      return (await rpcCallWithFallback<any>(targets, "getrawtransaction", [txid, true])).result;
    } catch (error) {
      const message = error instanceof Error ? error.message : "";
      const allowFallback =
        message.includes("No such mempool") || message.includes("gettransaction");
      if (!allowFallback) {
        throw error;
      }
    }

    try {
      const tx = await rpcCallWithFallback<any>(targets, "gettransaction", [txid]);
      if (tx.result?.decoded) {
        return tx.result.decoded;
      }

      if (tx.result?.hex) {
        try {
          const decoded = await rpcCallWithFallback<any>(targets, "decoderawtransaction", [
            tx.result.hex
          ]);
          return decoded.result ?? decoded;
        } catch {
          // fall through
        }
      }

      return { txid, vin: [], vout: [], hex: tx.result?.hex ?? "" };
    } catch {
      return { txid, vin: [], vout: [], hex: "" };
    }
  }
}

function extractAddresses(scriptPubKey: any): string[] {
  if (!scriptPubKey) return [];
  if (Array.isArray(scriptPubKey.addresses)) return scriptPubKey.addresses;
  if (typeof scriptPubKey.address === "string") return [scriptPubKey.address];
  return [];
}

function toSats(value: number | string): bigint {
  if (typeof value === "number") {
    const fixed = value.toFixed(8);
    return parseSatsString(fixed);
  }
  return parseSatsString(value);
}

function parseSatsString(value: string): bigint {
  const [wholeRaw, fracRaw] = value.split(".");
  const whole = wholeRaw.replace(/[^0-9-]/g, "");
  const negative = whole.startsWith("-");
  const wholeAbs = negative ? whole.slice(1) : whole;
  const frac = (fracRaw ?? "").padEnd(8, "0").slice(0, 8);
  const amount = BigInt(wholeAbs || "0") * 100000000n + BigInt(frac || "0");
  return negative ? -amount : amount;
}

function toSatsFromOutput(output: any): bigint {
  if (!output) return 0n;
  const valueSat = output.valueSat ?? output.value_sats ?? output.satoshis;
  if (valueSat !== undefined && valueSat !== null) {
    const satString = String(valueSat).replace(/[^0-9-]/g, "");
    if (satString) {
      return BigInt(satString);
    }
  }
  return toSats(output.value ?? 0);
}
