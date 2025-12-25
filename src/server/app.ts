import express from "express";
import session, { type Store } from "express-session";
import ConnectSqlite3 from "connect-sqlite3";
import bcrypt from "bcryptjs";
import path from "node:path";

import type { NodeManager } from "../manager.js";
import type { SequelizeStorage } from "../db/storage.js";
import { ExplorerService } from "./explorerService.js";

export interface AppOptions {
  sessionDbPath: string;
}

export function createApp(
  manager: NodeManager,
  storage: SequelizeStorage,
  options: AppOptions
): express.Express {
  const app = express();
  const explorer = new ExplorerService(manager, storage);
  const SQLiteStore = ConnectSqlite3(session);

  app.use(express.json({ limit: "1mb" }));
  app.use(
    session({
      store: new SQLiteStore({ db: "sessions.sqlite", dir: options.sessionDbPath }) as Store,
      secret: process.env.SESSION_SECRET ?? "neko-explorer-secret",
      resave: false,
      saveUninitialized: false,
      cookie: {
        maxAge: 5 * 60 * 60 * 1000
      }
    })
  );

  const assetsDir = path.join(process.cwd(), "src", "assets");
  app.use("/assets", express.static(assetsDir));

  app.get("/", (_req, res) => {
    res.sendFile(path.join(assetsDir, "public-explorer.html"));
  });

  app.get("/block/:id", (_req, res) => {
    res.sendFile(path.join(assetsDir, "block.html"));
  });

  app.get("/tx/:txid", (_req, res) => {
    res.sendFile(path.join(assetsDir, "tx.html"));
  });

  app.get("/address/:address", (_req, res) => {
    res.sendFile(path.join(assetsDir, "address.html"));
  });

  app.get("/admin/login", (_req, res) => {
    res.sendFile(path.join(assetsDir, "admin-login.html"));
  });

  app.get("/admin", (_req, res) => {
    res.sendFile(path.join(assetsDir, "admin.html"));
  });

  app.get("/public/chains", async (_req, res) => {
    try {
      const chains = await explorer.listChains();
      res.json({ ok: true, chains });
    } catch (error) {
      res.json({ ok: false, error: error instanceof Error ? error.message : "Failed" });
    }
  });

  app.get("/api/v1/chains", async (_req, res) => {
    try {
      const chains = await explorer.listChains();
      res.json({ ok: true, chains });
    } catch (error) {
      res.json({ ok: false, error: error instanceof Error ? error.message : "Failed" });
    }
  });

  app.get("/api/v1/:chain", async (req, res) => {
    try {
      const status = await explorer.getStatus(req.params.chain);
      res.json(status);
    } catch (error) {
      let chainMeta = null;
      try {
        chainMeta = manager.getChain(req.params.chain);
      } catch {
        chainMeta = null;
      }
      let cachedHeight = 0;
      let cachedHash = "";
      let cachedTime: string | null = null;
      try {
        const sync = await storage.getChainSync(req.params.chain);
        cachedHeight = Math.max(0, sync?.lastHeight ?? 0);
        cachedHash = sync?.lastBlockHash ?? "";
        if (cachedHeight >= 0) {
          const cachedBlock = await storage.getCachedBlockByHeight(req.params.chain, cachedHeight);
          if (cachedBlock?.hash) cachedHash = cachedBlock.hash;
          if (cachedBlock?.time) {
            cachedTime = new Date(cachedBlock.time * 1000).toISOString();
          }
        }
      } catch {
        // ignore cache failures
      }
      let nodeTotal = 0;
      try {
        const nodes = await storage.listNodesByChain(req.params.chain);
        nodeTotal = nodes.length;
      } catch {
        nodeTotal = 0;
      }
      res.json({
        ok: false,
        error: error instanceof Error ? error.message : "RPC unavailable",
        blockbook: {
          coin: chainMeta?.name ?? req.params.chain,
          host: chainMeta?.symbol ? `${chainMeta.symbol}-Blockbook` : "Blockbook",
          version: process.env.NEKO_EXPLORER_VERSION ?? "devel",
          gitCommit: process.env.NEKO_EXPLORER_GIT_COMMIT ?? "local",
          buildTime: process.env.NEKO_EXPLORER_BUILD_TIME ?? null,
          syncMode: true,
          initialSync: true,
          inSync: false,
          status: "offline",
          bestHeight: cachedHeight,
          lastBlockTime: cachedTime,
          inSyncMempool: false,
          lastMempoolTime: null,
          mempoolSize: 0,
          decimals: 8,
          dbSize: 0,
          hasFiatRates: false,
          currentFiatRatesTime: null,
          historicalFiatRatesTime: null,
          about:
            "Neko Explorer - blockchain indexer. Do not use for any other purpose."
        },
        backend: {
          chain: "main",
          blocks: cachedHeight,
          headers: cachedHeight,
          bestBlockHash: cachedHash,
          difficulty: "",
          sizeOnDisk: 0,
          version: "",
          subversion: "",
          protocolVersion: "",
          timeOffset: 0,
          lastBlockTime: cachedTime,
          status: "offline",
          source: "offline"
        },
        masternodes: { supported: false, total: null, enabled: null, list: [] },
        nodes: {
          total: nodeTotal,
          online: 0,
          syncing: 0,
          offline: nodeTotal,
          list: Array.from({ length: nodeTotal }).map((_, idx) => ({
            id: `node-${idx + 1}`,
            height: null,
            verificationProgress: null,
            status: "offline"
          }))
        }
      });
    }
  });

  app.get("/api/v1/:chain/address/:address", async (req, res) => {
    try {
      const chain = req.params.chain;
      const address = req.params.address;
      const page = Math.max(1, Number(req.query.page ?? 1));
      const itemsOnPage = Math.max(1, Number(req.query.itemsOnPage ?? 1000));
      const stats = await storage.getAddressStats(chain, address);
      const txCount = stats?.txCount ?? 0;
      const totalPages = Math.max(1, Math.ceil(txCount / itemsOnPage));
      const offset = (page - 1) * itemsOnPage;
      let transactions = await storage.listAddressTxs(chain, address, itemsOnPage, offset);
      const sync = await storage.getChainSync(chain);

      if (!txCount && transactions.length === 0) {
        try {
          const wallet = await explorer.getWalletAddressSnapshot(chain, address, page, itemsOnPage);
          if (wallet.totalCount > 0 || wallet.totalReceived !== 0 || wallet.totalSent !== 0) {
            const balanceSats = decimalToSatsString(wallet.balance);
            const receivedSats = decimalToSatsString(wallet.totalReceived);
            const sentSats = decimalToSatsString(wallet.totalSent);
            await storage.upsertAddressStats(
              chain,
              address,
              balanceSats,
              receivedSats,
              sentSats,
              wallet.totalCount
            );
            for (const txid of wallet.txids) {
              await storage.addAddressTx(chain, address, txid, null);
            }
            transactions = wallet.txids;
            await storage.saveAddress(chain, address, {
              addrStr: address,
              balance: formatSats(balanceSats),
              totalReceived: formatSats(receivedSats),
              totalSent: formatSats(sentSats),
              txApperances: wallet.totalCount,
              transactions
            });
            res.json({
              page,
              totalPages: Math.max(1, Math.ceil(wallet.totalCount / itemsOnPage)),
              itemsOnPage,
              indexedHeight: sync?.lastHeight ?? -1,
              addrStr: address,
              balance: formatSats(balanceSats),
              totalReceived: formatSats(receivedSats),
              totalSent: formatSats(sentSats),
              unconfirmedBalance: "0",
              unconfirmedTxApperances: 0,
              txApperances: wallet.totalCount,
              transactions
            });
            return;
          }
        } catch {
          // ignore wallet fallback failures
        }
      }

      res.json({
        page,
        totalPages,
        itemsOnPage,
        indexedHeight: sync?.lastHeight ?? -1,
        addrStr: address,
        balance: formatSats(stats?.balanceSats ?? "0"),
        totalReceived: formatSats(stats?.totalReceivedSats ?? "0"),
        totalSent: formatSats(stats?.totalSentSats ?? "0"),
        unconfirmedBalance: "0",
        unconfirmedTxApperances: 0,
        txApperances: txCount,
        transactions
      });
    } catch (error) {
      res.json({ ok: false, error: error instanceof Error ? error.message : "RPC unavailable" });
    }
  });

  app.get("/api/v1/:chain/block/:id", async (req, res) => {
    try {
      const page = Math.max(1, Number(req.query.page ?? 1));
      const itemsOnPage = Math.max(1, Number(req.query.itemsOnPage ?? 25));
      const data = await explorer.getBlockDetails(req.params.chain, req.params.id, page, itemsOnPage);
      res.json(data);
    } catch (error) {
      res.json({ ok: false, error: error instanceof Error ? error.message : "RPC unavailable" });
    }
  });

  app.get("/api/v1/:chain/tx/:txid", async (req, res) => {
    try {
      const { data } = await explorer.getTxDetails(req.params.chain, req.params.txid);
      res.json(data);
    } catch (error) {
      res.json({ ok: false, error: error instanceof Error ? error.message : "RPC unavailable" });
    }
  });

  app.get("/public/:chain/blockcount", async (req, res) => {
    try {
      const { value, target } = await explorer.getBlockCount(req.params.chain);
      res.json({ ok: true, blockCount: value, source: target.name });
    } catch (error) {
      res.json({ ok: false, error: error instanceof Error ? error.message : "RPC unavailable" });
    }
  });

  app.get("/public/:chain/bestblockhash", async (req, res) => {
    try {
      const { value, target } = await explorer.getBestBlockHash(req.params.chain);
      res.json({ ok: true, bestBlockHash: value, source: target.name });
    } catch (error) {
      res.json({ ok: false, error: error instanceof Error ? error.message : "RPC unavailable" });
    }
  });

  app.get("/public/:chain/mempool", async (req, res) => {
    try {
      const { value, target } = await explorer.getMempool(req.params.chain);
      res.json({ ok: true, txids: value, count: value.length, source: target.name });
    } catch (error) {
      res.json({ ok: false, error: error instanceof Error ? error.message : "RPC unavailable" });
    }
  });

  app.get("/public/:chain/block/:id", async (req, res) => {
    try {
      const { data, source } = await explorer.getBlock(req.params.chain, req.params.id);
      res.json({ ok: true, block: data, source });
    } catch (error) {
      res.json({ ok: false, error: error instanceof Error ? error.message : "RPC unavailable" });
    }
  });

  app.get("/public/:chain/tx/:txid", async (req, res) => {
    try {
      const { data, source } = await explorer.getTx(req.params.chain, req.params.txid);
      res.json({ ok: true, tx: data, source });
    } catch (error) {
      res.json({ ok: false, error: error instanceof Error ? error.message : "RPC unavailable" });
    }
  });

  app.get("/public/:chain/address/:address", async (req, res) => {
    try {
      const { data, source } = await explorer.getAddress(req.params.chain, req.params.address);
      res.json({ ok: true, address: data, source });
    } catch (error) {
      res.json({ ok: false, error: error instanceof Error ? error.message : "RPC unavailable" });
    }
  });

  app.get("/public/:chain/search/:query", async (req, res) => {
    try {
      const result = await explorer.search(req.params.chain, req.params.query);
      res.json({ ok: true, ...result });
    } catch (error) {
      res.json({ ok: false, error: error instanceof Error ? error.message : "RPC unavailable" });
    }
  });

  app.post("/api/admin/login", async (req, res) => {
    const { username, password } = req.body ?? {};
    if (!username || !password) {
      return res.status(400).json({ ok: false, error: "Username and password required" });
    }

    const admin = await storage.getAdminByUsername(username);
    if (!admin) {
      return res.status(401).json({ ok: false, error: "Invalid credentials" });
    }

    const valid = await bcrypt.compare(password, admin.passwordHash);
    if (!valid) {
      return res.status(401).json({ ok: false, error: "Invalid credentials" });
    }

    req.session.adminId = admin.id;
    res.json({ ok: true, mustChangePassword: admin.mustChangePassword });
  });

  app.post("/api/admin/logout", (req, res) => {
    req.session.destroy(() => {
      res.json({ ok: true });
    });
  });

  app.get("/api/admin/me", async (req, res) => {
    if (!req.session.adminId) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }
    const admin = await storage.getAdminById(req.session.adminId);
    if (!admin) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }
    res.json({ ok: true, username: admin.username, mustChangePassword: admin.mustChangePassword });
  });

  app.post("/api/admin/password", async (req, res) => {
    if (!req.session.adminId) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }
    const { currentPassword, newPassword } = req.body ?? {};
    if (!currentPassword || !newPassword) {
      return res.status(400).json({ ok: false, error: "Current and new password required" });
    }

    const admin = await storage.getAdminById(req.session.adminId);
    if (!admin) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }

    const valid = await bcrypt.compare(currentPassword, admin.passwordHash);
    if (!valid) {
      return res.status(401).json({ ok: false, error: "Invalid credentials" });
    }

    const passwordHash = await bcrypt.hash(newPassword, 10);
    await storage.updateAdminPassword(admin.id, passwordHash);
    res.json({ ok: true });
  });

  app.use("/api/admin", async (req, res, next) => {
    if (!req.session.adminId) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }
    const admin = await storage.getAdminById(req.session.adminId);
    if (!admin) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }
    if (
      admin.mustChangePassword &&
      req.path !== "/password" &&
      req.path !== "/me" &&
      req.path !== "/logout"
    ) {
      return res.status(403).json({ ok: false, error: "Password change required" });
    }
    next();
  });

  app.get("/api/admin/rpc-endpoints", async (req, res) => {
    const chain = typeof req.query.chain === "string" ? req.query.chain : undefined;
    const endpoints = await storage.listRpcEndpoints(chain);
    res.json({ ok: true, endpoints });
  });

  app.post("/api/admin/rpc-endpoints", async (req, res) => {
    const { chain, name, url, username, password, priority, enabled } = req.body ?? {};
    if (!chain || !name || !url) {
      return res.status(400).json({ ok: false, error: "chain, name, and url are required" });
    }
    const endpoint = await storage.addRpcEndpoint({
      chain,
      name,
      url,
      username,
      password,
      priority: Number(priority ?? 0),
      enabled: enabled !== false
    });
    res.json({ ok: true, endpoint });
  });

  app.patch("/api/admin/rpc-endpoints/:id", async (req, res) => {
    const id = Number(req.params.id);
    if (Number.isNaN(id)) {
      return res.status(400).json({ ok: false, error: "Invalid endpoint id" });
    }
    await storage.updateRpcEndpoint(id, req.body ?? {});
    res.json({ ok: true });
  });

  app.delete("/api/admin/rpc-endpoints/:id", async (req, res) => {
    const id = Number(req.params.id);
    if (Number.isNaN(id)) {
      return res.status(400).json({ ok: false, error: "Invalid endpoint id" });
    }
    await storage.removeRpcEndpoint(id);
    res.json({ ok: true });
  });

  app.get("/api/admin/nodes", async (_req, res) => {
    const nodes = await manager.listNodes();
    res.json({ ok: true, nodes });
  });

  app.post("/api/admin/nodes", async (req, res) => {
    try {
      const node = await manager.createNode(req.body ?? {});
      res.json({ ok: true, node });
    } catch (error) {
      res.status(400).json({ ok: false, error: error instanceof Error ? error.message : "Failed" });
    }
  });

  app.post("/api/admin/nodes/:id/start", async (req, res) => {
    await manager.start(req.params.id);
    res.json({ ok: true });
  });

  app.post("/api/admin/nodes/:id/stop", async (req, res) => {
    try {
      await manager.stop(req.params.id);
      res.json({ ok: true });
    } catch (error) {
      res.json({ ok: false, error: "Node is offline. Start it first." });
    }
  });

  app.post("/api/admin/nodes/:id/restart", async (req, res) => {
    try {
      await manager.restart(req.params.id);
      res.json({ ok: true });
    } catch (error) {
      res.json({ ok: false, error: "Node is offline. Start it first." });
    }
  });

  app.post("/api/admin/nodes/:id/resync", async (req, res) => {
    try {
      await manager.resync(req.params.id);
      res.json({ ok: true });
    } catch (error) {
      res.json({ ok: false, error: "Node is offline. Start it first." });
    }
  });

  app.delete("/api/admin/nodes/:id", async (req, res) => {
    await manager.deleteNode(req.params.id);
    res.json({ ok: true });
  });

  app.get("/api/admin/nodes/:id/status", async (req, res) => {
    try {
      const blockCount = await manager.getBlockCount(req.params.id);
      res.json({ ok: true, online: true, blockCount });
    } catch (error) {
      res.json({ ok: true, online: false, error: "RPC unavailable" });
    }
  });

  return app;
}

function formatSats(value: string): string {
  const negative = value.startsWith("-");
  const raw = negative ? value.slice(1) : value;
  const padded = raw.padStart(9, "0");
  const whole = padded.slice(0, -8) || "0";
  const frac = padded.slice(-8).padStart(8, "0");
  return `${negative ? "-" : ""}${whole}.${frac}`;
}

function decimalToSatsString(value: number): string {
  if (!Number.isFinite(value) || value === 0) return "0";
  const negative = value < 0;
  const fixed = Math.abs(value).toFixed(8);
  const [whole, frac] = fixed.split(".");
  const combined = `${whole}${(frac ?? "").padEnd(8, "0").slice(0, 8)}`;
  const sats = BigInt(combined || "0");
  return `${negative ? "-" : ""}${sats.toString()}`;
}
