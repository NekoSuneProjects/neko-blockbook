import type { NodeManager } from "../manager.js";
import type { SequelizeStorage } from "../db/storage.js";
import type { RpcTarget } from "../types.js";
import { rpcCallWithFallback } from "../core/rpc.js";

export class ExplorerService {
  private manager: NodeManager;
  private storage: SequelizeStorage;

  constructor(manager: NodeManager, storage: SequelizeStorage) {
    this.manager = manager;
    this.storage = storage;
  }

  async listChains(): Promise<
    Array<{ id: string; name: string; symbol: string; endpoints: number; nodes: number }>
  > {
    const chains = this.manager.listChains();
    const result = [] as Array<{ id: string; name: string; symbol: string; endpoints: number; nodes: number }>;
    for (const chain of chains) {
      const endpoints = (await this.storage.listRpcEndpoints(chain.id)).filter(
        (entry) => entry.enabled
      ).length;
      const nodes = (await this.storage.listNodesByChain(chain.id)).length;
      result.push({ id: chain.id, name: chain.name, symbol: chain.symbol, endpoints, nodes });
    }
    return result;
  }

  async getBlockCount(chain: string): Promise<{ value: number; target: RpcTarget }> {
    return this.rpc(chain, "getblockcount");
  }

  async getBestBlockHash(chain: string): Promise<{ value: string; target: RpcTarget }> {
    return this.rpc(chain, "getbestblockhash");
  }

  async getMempool(chain: string): Promise<{ value: string[]; target: RpcTarget }> {
    const { result, target } = await this.rpc<string[]>(chain, "getrawmempool");
    await this.storage.saveMempool(chain, result);
    return { value: result, target };
  }

  async getBlock(chain: string, id: string): Promise<{ data: any; source: string }> {
    const height = Number(id);
    if (!Number.isNaN(height)) {
      const cached = await this.storage.getCachedBlockByHeight(chain, height);
      if (cached) return { data: cached, source: "cache" };
    } else {
      const cached = await this.storage.getCachedBlockByHash(chain, id);
      if (cached) return { data: cached, source: "cache" };
    }
    throw new Error("Block not in cache");
  }

  async getBlockDetails(
    chain: string,
    id: string,
    page: number,
    itemsOnPage: number
  ): Promise<{
    block: Record<string, any>;
    txs: Array<Record<string, any>>;
    page: number;
    totalPages: number;
    itemsOnPage: number;
  }> {
    const height = Number(id);
    const blockHash = Number.isNaN(height) ? id : null;
    let block: any = null;
    if (!Number.isNaN(height)) {
      block = await this.storage.getCachedBlockByHeight(chain, height);
    } else if (blockHash) {
      block = await this.storage.getCachedBlockByHash(chain, blockHash);
    }
    if (!block) {
      throw new Error("Block not in cache");
    }
    const totalTxs = Array.isArray(block?.tx) ? block.tx.length : 0;
    const totalPages = Math.max(1, Math.ceil(totalTxs / itemsOnPage));
    const offset = Math.max(0, (page - 1) * itemsOnPage);
    const txs = (block?.tx ?? []).slice(offset, offset + itemsOnPage);

    let resolvedTxs = txs;
    if (txs.length && typeof txs[0] === "string") {
      const fetched = [];
      for (const txid of txs) {
        const cachedTx = await this.storage.getCachedTx(chain, txid);
        if (cachedTx && !this.isEmptyTx(cachedTx)) {
          fetched.push(cachedTx);
        } else {
          fetched.push({ txid, vin: [], vout: [] });
        }
      }
      resolvedTxs = fetched;
    }

    const txSummaries = resolvedTxs.map((tx: any) => {
      const vout = Array.isArray(tx?.vout) ? tx.vout : [];
      const valueOut = vout.reduce(
        (sum: number, out: any) => sum + this.getOutputValue(out),
        0
      );
      return {
        txid: tx?.txid ?? "",
        vinCount: Array.isArray(tx?.vin) ? tx.vin.length : 0,
        vout,
        isCoinbase: Array.isArray(tx?.vin) && tx.vin.some((vin: any) => vin?.coinbase),
        valueOut: valueOut.toFixed(8),
        confirmations: block?.confirmations ?? 0,
        time: block?.time ?? null
      };
    });

    return {
      block: {
        hash: block?.hash ?? blockHash ?? "",
        height: block?.height ?? null,
        confirmations: block?.confirmations ?? 0,
        time: block?.time ?? null,
        size: block?.size ?? 0,
        txCount: totalTxs,
        version: block?.version ?? 0,
        merkleroot: block?.merkleroot ?? "",
        nonce: block?.nonce ?? 0,
        bits: block?.bits ?? "",
        difficulty: block?.difficulty ?? "",
        previousblockhash: block?.previousblockhash ?? null,
        nextblockhash: block?.nextblockhash ?? null
      },
      txs: txSummaries,
      page,
      totalPages,
      itemsOnPage
    };
  }

  async getTx(chain: string, txid: string): Promise<{ data: any; source: string }> {
    const cached = await this.storage.getCachedTx(chain, txid);
    if (cached && !this.isEmptyTx(cached)) return { data: cached, source: "cache" };
    throw new Error("Transaction not in cache");
  }

  async getTxDetails(chain: string, txid: string): Promise<{ data: any; source: string }> {
    const cached = await this.storage.getCachedTx(chain, txid);
    if (!cached || this.isEmptyTx(cached)) {
      throw new Error("Transaction not in cache");
    }
    const baseTx = cached;
    const source = "cache";

    let blockheight: number | null = null;
    if (baseTx?.blockhash) {
      const cachedBlock = await this.storage.getCachedBlockByHash(chain, baseTx.blockhash);
      blockheight = cachedBlock?.height ?? null;
    }

    const vin = [];
    let valueIn = 0;
    for (const input of baseTx?.vin ?? []) {
      if (input?.coinbase) {
        vin.push({
          txid: "",
          vout: 0,
          sequence: input.sequence ?? 0,
          n: input.n ?? 0,
          scriptSig: input.scriptSig ?? {},
          addresses: null,
          value: ""
        });
        continue;
      }

      const prevTxid = input?.txid;
      const voutIndex = input?.vout;
      let addresses = null;
      let value = "";
      if (prevTxid !== undefined && voutIndex !== undefined) {
        const prev = await this.storage.getCachedTx(chain, prevTxid);
        const prevOut = Array.isArray(prev?.vout) ? prev.vout[voutIndex] : null;
        if (prevOut) {
          const addrs = prevOut?.scriptPubKey?.addresses;
          addresses = Array.isArray(addrs) ? addrs : null;
          const prevValue = this.getOutputValue(prevOut);
          value = prevValue ? prevValue.toFixed(8) : "";
          valueIn += prevValue;
        }
      }

      vin.push({
        txid: prevTxid ?? "",
        vout: voutIndex ?? 0,
        sequence: input.sequence ?? 0,
        n: input.n ?? 0,
        scriptSig: input.scriptSig ?? {},
        addresses,
        value
      });
    }

    const vout = [];
    let valueOut = 0;
    for (const output of baseTx?.vout ?? []) {
      const outValue = this.getOutputValue(output);
      valueOut += outValue;
      const scriptPubKey = output?.scriptPubKey ?? {};
      const addresses = Array.isArray(scriptPubKey?.addresses)
        ? scriptPubKey.addresses
        : scriptPubKey?.address
          ? [scriptPubKey.address]
          : [];
      vout.push({
        value: outValue.toFixed(8),
        n: output?.n ?? 0,
        scriptPubKey: {
          hex: scriptPubKey?.hex ?? "",
          addresses: addresses.length ? addresses : []
        },
        spent: false
      });
    }

    const fees = baseTx?.vin?.some((input: any) => input?.coinbase)
      ? 0
      : Math.max(0, valueIn - valueOut);

    const payload = {
      txid: baseTx?.txid ?? txid,
      version: baseTx?.version ?? 0,
      vin,
      vout,
      blockhash: baseTx?.blockhash ?? null,
      blockheight,
      confirmations: baseTx?.confirmations ?? 0,
      time: baseTx?.time ?? null,
      blocktime: baseTx?.blocktime ?? null,
      valueOut: valueOut.toFixed(8),
      size: baseTx?.size ?? 0,
      valueIn: valueIn.toFixed(8),
      fees: fees.toFixed(8),
      hex: baseTx?.hex ?? ""
    };

    return { data: payload, source };
  }

  async getAddress(chain: string, address: string): Promise<{ data: any; source: string }> {
    const stats = await this.storage.getAddressStats(chain, address);
    const txids = await this.storage.listAddressTxs(chain, address, 1000, 0);
    if (stats || txids.length) {
      const payload = {
        address,
        amount: Number(this.formatSats(stats?.balanceSats ?? "0")),
        confirmations: 0,
        txids
      };
      await this.storage.saveAddress(chain, address, payload);
      return { data: payload, source: "cache" };
    }

    const cached = await this.storage.getCachedAddress(chain, address);
    if (cached) return { data: cached, source: "cache" };
    throw new Error("Address not in cache");
  }

  async search(chain: string, query: string): Promise<{ kind: string; data: any; source: string }> {
    const trimmed = query.trim();
    if (!trimmed) {
      throw new Error("Query required");
    }

    const height = Number(trimmed);
    if (!Number.isNaN(height)) {
      const cached = await this.storage.getCachedBlockByHeight(chain, height);
      if (cached) {
        return { kind: "block", data: cached, source: "cache" };
      }
      throw new Error("Block not in cache");
    }

    if (/^[0-9a-fA-F]{64}$/.test(trimmed)) {
      const cachedBlock = await this.storage.getCachedBlockByHash(chain, trimmed);
      if (cachedBlock) {
        return { kind: "block", data: cachedBlock, source: "cache" };
      }
      const cachedTx = await this.storage.getCachedTx(chain, trimmed);
      if (cachedTx && !this.isEmptyTx(cachedTx)) {
        return { kind: "tx", data: cachedTx, source: "cache" };
      }
      throw new Error("Transaction not in cache");
    }

    const stats = await this.storage.getAddressStats(chain, trimmed);
    if (stats) {
      const data = {
        addrStr: trimmed,
        balance: this.formatSats(stats.balanceSats ?? "0"),
        totalReceived: this.formatSats(stats.totalReceivedSats ?? "0"),
        totalSent: this.formatSats(stats.totalSentSats ?? "0"),
        txApperances: stats.txCount ?? 0
      };
      return { kind: "address", data, source: "cache" };
    }
    const cachedAddress = await this.storage.getCachedAddress(chain, trimmed);
    if (cachedAddress) {
      return { kind: "address", data: cachedAddress, source: "cache" };
    }
    throw new Error("Address not in cache");
  }

  async getStatus(chain: string): Promise<{
    blockbook: Record<string, any>;
    backend: Record<string, any>;
    masternodes: Record<string, any>;
    nodes: Record<string, any>;
  }> {
    const targets = await this.getRpcTargets(chain);
    if (targets.length === 0) {
      throw new Error(`No RPC endpoints for chain: ${chain}`);
    }

    const { result: chainInfo, target } = await rpcCallWithFallback<any>(
      targets,
      "getblockchaininfo"
    );
    const { result: netInfo } = await rpcCallWithFallback<any>(
      targets,
      "getnetworkinfo"
    );

    let mempoolInfo: any = null;
    try {
      const mempoolResult = await rpcCallWithFallback<any>(targets, "getmempoolinfo");
      mempoolInfo = mempoolResult.result;
    } catch {
      mempoolInfo = null;
    }

    const bestBlockHash =
      chainInfo?.bestblockhash ??
      (await rpcCallWithFallback<string>(targets, "getbestblockhash")).result;

    let lastBlockTime: string | null = null;
    try {
      const header = await rpcCallWithFallback<any>(targets, "getblockheader", [bestBlockHash]);
      if (header.result?.time) {
        lastBlockTime = new Date(header.result.time * 1000).toISOString();
      }
    } catch {
      lastBlockTime = null;
    }

    const blocks = Number(chainInfo?.blocks ?? 0);
    const headers = Number(chainInfo?.headers ?? blocks);
    const verificationProgress = Number(chainInfo?.verificationprogress ?? 0);
    const sync = await this.storage.getChainSync(chain);
    const cachedHeight = sync?.lastHeight ?? 0;
    const cachedBlock = cachedHeight >= 0
      ? await this.storage.getCachedBlockByHeight(chain, cachedHeight)
      : null;
    const cachedBlockTime = cachedBlock?.time
      ? new Date(cachedBlock.time * 1000).toISOString()
      : null;
    const cachedMempool = await this.storage.getLatestMempool(chain);

    const mempoolSize = typeof mempoolInfo?.size === "number" ? mempoolInfo.size : null;
    const cachedMempoolSize = cachedMempool?.txids?.length ?? null;
    const inSyncBlocks = blocks > 0 && cachedHeight >= blocks;
    const inSyncMempool =
      inSyncBlocks &&
      mempoolSize !== null &&
      cachedMempoolSize !== null &&
      mempoolSize === cachedMempoolSize;
    const inSync = inSyncBlocks && inSyncMempool && verificationProgress >= 0.999;
    const backendStatus = inSync ? "synced" : "syncing";

    const masternodes = await this.getMasternodeStats(chain);
    const nodes = await this.getNodeStatus(chain);

    return {
      blockbook: {
        coin: this.manager.getChain(chain).name,
        host: process.env.NEKO_EXPLORER_HOST ?? `${this.manager.getChain(chain).symbol}-Blockbook`,
        version: process.env.NEKO_EXPLORER_VERSION ?? "devel",
        gitCommit: process.env.NEKO_EXPLORER_GIT_COMMIT ?? "local",
        buildTime: process.env.NEKO_EXPLORER_BUILD_TIME ?? null,
        syncMode: true,
        initialSync: !inSync,
        inSync,
        status: backendStatus,
        bestHeight: cachedHeight,
        lastBlockTime: cachedBlockTime,
        inSyncMempool,
        lastMempoolTime: cachedMempool?.fetchedAt ?? null,
        mempoolSize: cachedMempool?.txids?.length ?? 0,
        decimals: 8,
        dbSize: Number(chainInfo?.size_on_disk ?? 0),
        hasFiatRates: false,
        currentFiatRatesTime: null,
        historicalFiatRatesTime: null,
        about:
          "Neko Explorer - blockchain indexer. Do not use for any other purpose."
      },
      backend: {
        chain: chainInfo?.chain ?? "main",
        blocks,
        headers,
        bestBlockHash,
        difficulty: String(chainInfo?.difficulty ?? ""),
        sizeOnDisk: Number(chainInfo?.size_on_disk ?? 0),
        version: String(netInfo?.version ?? ""),
        subversion: String(netInfo?.subversion ?? ""),
        protocolVersion: String(netInfo?.protocolversion ?? ""),
        timeOffset: Number(netInfo?.timeoffset ?? 0),
        lastBlockTime,
        status: backendStatus,
        source: target.name
      },
      masternodes,
      nodes
    };
  }

  private async getMasternodeStats(chain: string): Promise<{
    supported: boolean;
    total: number | null;
    enabled: number | null;
    list: Array<Record<string, any>>;
  }> {
    const targets = await this.getRpcTargets(chain);
    if (targets.length === 0) {
      return { supported: false, total: null, enabled: null, list: [] };
    }

    let total: number | null = null;
    let enabled: number | null = null;

    try {
      const res = await rpcCallWithFallback<any>(targets, "masternode", ["count"]);
      if (typeof res.result === "number") {
        total = res.result;
      } else if (res.result && typeof res.result === "object") {
        total = Number(res.result.total ?? res.result.enabled ?? 0);
        enabled = Number(res.result.enabled ?? res.result.total ?? 0);
      }
    } catch {
      try {
        const res = await rpcCallWithFallback<any>(targets, "getmasternodecount");
        if (typeof res.result === "number") {
          total = res.result;
        } else if (res.result && typeof res.result === "object") {
          total = Number(res.result.total ?? res.result.enabled ?? 0);
          enabled = Number(res.result.enabled ?? res.result.total ?? 0);
        }
      } catch {
        return { supported: false, total: null, enabled: null, list: [] };
      }
    }

    let list: Array<Record<string, any>> = [];
    try {
      const res = await rpcCallWithFallback<any>(targets, "masternodelist", ["full"]);
      if (res.result && typeof res.result === "object" && !Array.isArray(res.result)) {
        list = Object.entries(res.result).slice(0, 50).map(([key, value]) => ({
          key,
          info: value
        }));
      } else if (Array.isArray(res.result)) {
        list = res.result.slice(0, 50);
      }
    } catch {
      list = [];
    }

    return { supported: true, total, enabled, list };
  }

  private async getNodeStatus(chain: string): Promise<{
    total: number;
    online: number;
    syncing: number;
    offline: number;
    list: Array<Record<string, any>>;
  }> {
    const nodes = await this.storage.listNodesByChain(chain);
    const statusList = await Promise.all(
      nodes.map(async (node) => {
        const target: RpcTarget = {
          id: `node-${node.id}`,
          chain: node.chain,
          name: node.id,
          url: `http://127.0.0.1:${node.rpcPort}/`,
          username: node.rpcUser,
          password: node.rpcPassword
        };

        try {
          const height = await rpcCallWithFallback<number>([target], "getblockcount");
          const info = await rpcCallWithFallback<any>([target], "getblockchaininfo");
          const verification = Number(info.result?.verificationprogress ?? 0);
          const syncing = Boolean(info.result?.initialblockdownload) || verification < 0.999;
          return {
            id: node.id,
            height: Number(height.result ?? 0),
            verificationProgress: verification,
            status: syncing ? "syncing" : "online"
          };
        } catch {
          return { id: node.id, height: null, verificationProgress: null, status: "offline" };
        }
      })
    );

    return {
      total: statusList.length,
      online: statusList.filter((n) => n.status === "online").length,
      syncing: statusList.filter((n) => n.status === "syncing").length,
      offline: statusList.filter((n) => n.status === "offline").length,
      list: statusList
    };
  }

  private async rpc<T>(
    chain: string,
    method: string,
    params: unknown[] = []
  ): Promise<{ result: T; target: RpcTarget } & { value: T }> {
    const targets = await this.getRpcTargets(chain);
    if (targets.length === 0) {
      throw new Error(`No RPC endpoints for chain: ${chain}`);
    }
    const { result, target } = await rpcCallWithFallback<T>(targets, method, params);
    return { result, target, value: result };
  }

  private async getBlockVerbose(
    chain: string,
    blockHash: string
  ): Promise<{ block: any; target: RpcTarget }> {
    const targets = await this.getRpcTargets(chain);
    if (targets.length === 0) {
      throw new Error(`No RPC endpoints for chain: ${chain}`);
    }

    try {
      const { result, target } = await rpcCallWithFallback<any>(targets, "getblock", [blockHash, 2]);
      return { block: result, target };
    } catch (error) {
      const message = error instanceof Error ? error.message.toLowerCase() : "";
      if (!message.includes("boolean")) {
        throw error;
      }
    }

    const { result, target } = await rpcCallWithFallback<any>(targets, "getblock", [blockHash, true]);
    return { block: result, target };
  }

  private getOutputValue(output: any): number {
    if (!output) return 0;
    if (typeof output.value === "number") return output.value;
    if (typeof output.value === "string") {
      const parsed = Number(output.value);
      return Number.isFinite(parsed) ? parsed : 0;
    }
    const valueSat = output.valueSat ?? output.value_sats ?? output.satoshis;
    if (valueSat !== undefined && valueSat !== null) {
      const asNumber = Number(valueSat);
      if (Number.isFinite(asNumber)) {
        return asNumber / 100000000;
      }
    }
    return 0;
  }

  private formatSats(value: string): string {
    const negative = value.startsWith("-");
    const raw = negative ? value.slice(1) : value;
    const padded = raw.padStart(9, "0");
    const whole = padded.slice(0, -8) || "0";
    const frac = padded.slice(-8).padStart(8, "0");
    return `${negative ? "-" : ""}${whole}.${frac}`;
  }

  private isEmptyTx(tx: any): boolean {
    if (!tx) return true;
    const vinEmpty = !Array.isArray(tx.vin) || tx.vin.length === 0;
    const voutEmpty = !Array.isArray(tx.vout) || tx.vout.length === 0;
    const hasHex = typeof tx.hex === "string" && tx.hex.length > 0;
    const hasSize = typeof tx.size === "number" && tx.size > 0;
    return vinEmpty && voutEmpty && !hasHex && !hasSize;
  }

  async getWalletAddressSnapshot(
    chain: string,
    address: string,
    page: number,
    itemsOnPage: number
  ): Promise<{
    txids: string[];
    totalReceived: number;
    totalSent: number;
    balance: number;
    totalCount: number;
  }> {
    const scanLimit = Number(process.env.NEKO_EXPLORER_WALLET_TX_SCAN ?? 2000);
    const { result } = await this.rpc<any[]>(chain, "listtransactions", [
      "*",
      scanLimit,
      0,
      true
    ]);

    const seen = new Set<string>();
    const matched = [];
    let totalReceived = 0;
    let totalSent = 0;

    for (const entry of result ?? []) {
      const { txid } = entry ?? {};
      if (!txid) continue;
      let amount = null as number | null;
      let category = entry?.category;
      let hit = false;

      if (entry?.address === address) {
        hit = true;
        amount = typeof entry.amount === "number" ? entry.amount : null;
      } else if (Array.isArray(entry?.details)) {
        const detail = entry.details.find((row: any) => row?.address === address);
        if (detail) {
          hit = true;
          amount = typeof detail.amount === "number" ? detail.amount : null;
          category = detail.category ?? category;
        }
      }

      if (!hit) continue;
      if (!seen.has(txid)) {
        seen.add(txid);
        matched.push(txid);
      }

      if (amount === null) continue;
      if (category === "send" || amount < 0) {
        totalSent += Math.abs(amount);
      } else if (category === "receive" || category === "generate" || category === "immature") {
        totalReceived += Math.max(0, amount);
      } else if (amount > 0) {
        totalReceived += amount;
      }
    }

    const totalCount = matched.length;
    const offset = (page - 1) * itemsOnPage;
    const txids = matched.slice(offset, offset + itemsOnPage);
    const balance = totalReceived - totalSent;

    return { txids, totalReceived, totalSent, balance, totalCount };
  }

  private async fetchTransaction(
    chain: string,
    txid: string
  ): Promise<{ tx: any; source: string }> {
    try {
      const { result, target } = await this.rpc<any>(chain, "getrawtransaction", [txid, true]);
      return { tx: result, source: target.name };
    } catch (error) {
      const message = error instanceof Error ? error.message : "";
      const allowFallback =
        message.includes("No such mempool") || message.includes("gettransaction");
      if (!allowFallback) {
        throw error;
      }
    }

    try {
      const { result, target } = await this.rpc<any>(chain, "gettransaction", [txid]);
      if (result?.decoded) {
        return { tx: result.decoded, source: target.name };
      }

      if (result?.hex) {
        try {
          const decoded = await this.rpc<any>(chain, "decoderawtransaction", [result.hex]);
          return { tx: decoded.result ?? decoded, source: target.name };
        } catch {
          // fall through
        }
      }

      return { tx: { txid, vin: [], vout: [], hex: result?.hex ?? "" }, source: target.name };
    } catch {
      return { tx: { txid, vin: [], vout: [], hex: "" }, source: "unavailable" };
    }
  }

  private async getRpcTargets(chain: string): Promise<RpcTarget[]> {
    const endpoints = (await this.storage.listRpcEndpoints(chain))
      .filter((entry) => entry.enabled)
      .map((entry) => ({
        id: `rpc-${entry.id}`,
        chain: entry.chain,
        name: entry.name,
        url: entry.url,
        username: entry.username,
        password: entry.password
      }));

    const nodes = (await this.storage.listNodesByChain(chain)).map((node) => ({
      id: `node-${node.id}`,
      chain: node.chain,
      name: `node:${node.id}`,
      url: `http://127.0.0.1:${node.rpcPort}/`,
      username: node.rpcUser,
      password: node.rpcPassword
    }));

    return [...endpoints, ...nodes];
  }
}
