import fs from "node:fs/promises";
import path from "node:path";
import { Sequelize, DataTypes, Model, Op, type ModelCtor } from "sequelize";
import type { NodeConfig, RpcEndpointConfig } from "../types.js";

export interface AdminUserRecord {
  id: number;
  username: string;
  passwordHash: string;
  mustChangePassword: boolean;
}

export interface CachedResult<T> {
  data: T;
  cached: boolean;
}

export class SequelizeStorage {
  private sequelize: Sequelize;
  private AdminUser: ModelCtor<Model<any, any>>;
  private Node: ModelCtor<Model<any, any>>;
  private RpcEndpoint: ModelCtor<Model<any, any>>;
  private CachedBlock: ModelCtor<Model<any, any>>;
  private CachedTx: ModelCtor<Model<any, any>>;
  private CachedAddress: ModelCtor<Model<any, any>>;
  private CachedMempool: ModelCtor<Model<any, any>>;
  private ChainSync: ModelCtor<Model<any, any>>;
  private AddressStat: ModelCtor<Model<any, any>>;
  private AddressTx: ModelCtor<Model<any, any>>;
  private storagePath: string;

  constructor(dbPath: string) {
    this.storagePath = dbPath;
    this.sequelize = new Sequelize({
      dialect: "sqlite",
      storage: dbPath,
      logging: false
    });

    this.AdminUser = this.sequelize.define(
      "AdminUser",
      {
        id: { type: DataTypes.INTEGER, autoIncrement: true, primaryKey: true },
        username: { type: DataTypes.STRING, allowNull: false, unique: true },
        passwordHash: { type: DataTypes.STRING, allowNull: false },
        mustChangePassword: { type: DataTypes.BOOLEAN, allowNull: false, defaultValue: true }
      },
      { tableName: "admin_users" }
    );

    this.Node = this.sequelize.define(
      "Node",
      {
        id: { type: DataTypes.INTEGER, autoIncrement: true, primaryKey: true },
        nodeId: { type: DataTypes.STRING, allowNull: false, unique: true },
        chain: { type: DataTypes.STRING, allowNull: false },
        datadir: { type: DataTypes.STRING, allowNull: false },
        p2pPort: { type: DataTypes.INTEGER, allowNull: false },
        rpcPort: { type: DataTypes.INTEGER, allowNull: false },
        rpcUser: { type: DataTypes.STRING, allowNull: false },
        rpcPassword: { type: DataTypes.STRING, allowNull: false },
        snapshotUrl: { type: DataTypes.STRING, allowNull: true },
        coreVersion: { type: DataTypes.STRING, allowNull: true },
        daemonPath: { type: DataTypes.STRING, allowNull: true }
      },
      {
        tableName: "nodes",
        indexes: [{ fields: ["chain"] }]
      }
    );

    this.RpcEndpoint = this.sequelize.define(
      "RpcEndpoint",
      {
        id: { type: DataTypes.INTEGER, autoIncrement: true, primaryKey: true },
        chain: { type: DataTypes.STRING, allowNull: false },
        name: { type: DataTypes.STRING, allowNull: false },
        url: { type: DataTypes.STRING, allowNull: false },
        username: { type: DataTypes.STRING, allowNull: true },
        password: { type: DataTypes.STRING, allowNull: true },
        priority: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 0 },
        enabled: { type: DataTypes.BOOLEAN, allowNull: false, defaultValue: true }
      },
      {
        tableName: "rpc_endpoints",
        indexes: [{ fields: ["chain", "enabled", "priority"] }]
      }
    );

    this.CachedBlock = this.sequelize.define(
      "CachedBlock",
      {
        id: { type: DataTypes.INTEGER, autoIncrement: true, primaryKey: true },
        chain: { type: DataTypes.STRING, allowNull: false },
        height: { type: DataTypes.INTEGER, allowNull: true },
        hash: { type: DataTypes.STRING, allowNull: false },
        payload: { type: DataTypes.TEXT, allowNull: false },
        fetchedAt: { type: DataTypes.DATE, allowNull: false, defaultValue: DataTypes.NOW }
      },
      {
        tableName: "cached_blocks",
        indexes: [
          { unique: true, fields: ["chain", "hash"] },
          { fields: ["chain", "height"] }
        ]
      }
    );

    this.CachedTx = this.sequelize.define(
      "CachedTx",
      {
        id: { type: DataTypes.INTEGER, autoIncrement: true, primaryKey: true },
        chain: { type: DataTypes.STRING, allowNull: false },
        txid: { type: DataTypes.STRING, allowNull: false },
        payload: { type: DataTypes.TEXT, allowNull: false },
        fetchedAt: { type: DataTypes.DATE, allowNull: false, defaultValue: DataTypes.NOW }
      },
      {
        tableName: "cached_txs",
        indexes: [{ unique: true, fields: ["chain", "txid"] }]
      }
    );

    this.CachedAddress = this.sequelize.define(
      "CachedAddress",
      {
        id: { type: DataTypes.INTEGER, autoIncrement: true, primaryKey: true },
        chain: { type: DataTypes.STRING, allowNull: false },
        address: { type: DataTypes.STRING, allowNull: false },
        payload: { type: DataTypes.TEXT, allowNull: false },
        fetchedAt: { type: DataTypes.DATE, allowNull: false, defaultValue: DataTypes.NOW }
      },
      {
        tableName: "cached_addresses",
        indexes: [{ unique: true, fields: ["chain", "address"] }]
      }
    );

    this.CachedMempool = this.sequelize.define(
      "CachedMempool",
      {
        id: { type: DataTypes.INTEGER, autoIncrement: true, primaryKey: true },
        chain: { type: DataTypes.STRING, allowNull: false },
        payload: { type: DataTypes.TEXT, allowNull: false },
        fetchedAt: { type: DataTypes.DATE, allowNull: false, defaultValue: DataTypes.NOW }
      },
      {
        tableName: "cached_mempool",
        indexes: [{ fields: ["chain", "fetchedAt"] }]
      }
    );

    this.ChainSync = this.sequelize.define(
      "ChainSync",
      {
        id: { type: DataTypes.INTEGER, autoIncrement: true, primaryKey: true },
        chain: { type: DataTypes.STRING, allowNull: false, unique: true },
        lastHeight: { type: DataTypes.INTEGER, allowNull: false, defaultValue: -1 },
        lastBlockHash: { type: DataTypes.STRING, allowNull: true }
      },
      { tableName: "chain_sync" }
    );

    this.AddressStat = this.sequelize.define(
      "AddressStat",
      {
        id: { type: DataTypes.INTEGER, autoIncrement: true, primaryKey: true },
        chain: { type: DataTypes.STRING, allowNull: false },
        address: { type: DataTypes.STRING, allowNull: false },
        balanceSats: { type: DataTypes.TEXT, allowNull: false, defaultValue: "0" },
        totalReceivedSats: { type: DataTypes.TEXT, allowNull: false, defaultValue: "0" },
        totalSentSats: { type: DataTypes.TEXT, allowNull: false, defaultValue: "0" },
        txCount: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 0 }
      },
      {
        tableName: "address_stats",
        indexes: [{ unique: true, fields: ["chain", "address"] }]
      }
    );

    this.AddressTx = this.sequelize.define(
      "AddressTx",
      {
        id: { type: DataTypes.INTEGER, autoIncrement: true, primaryKey: true },
        chain: { type: DataTypes.STRING, allowNull: false },
        address: { type: DataTypes.STRING, allowNull: false },
        txid: { type: DataTypes.STRING, allowNull: false },
        height: { type: DataTypes.INTEGER, allowNull: true }
      },
      {
        tableName: "address_txs",
        indexes: [{ unique: true, fields: ["chain", "address", "txid"] }]
      }
    );
  }

  async init(): Promise<void> {
    await fs.mkdir(path.dirname(this.storagePath), { recursive: true });
    await this.sequelize.authenticate();
    await this.sequelize.sync();
  }

  async adminExists(): Promise<boolean> {
    const count = await (this.AdminUser as any).count();
    return count > 0;
  }

  async createAdmin(
    username: string,
    passwordHash: string,
    mustChangePassword = true
  ): Promise<AdminUserRecord> {
    const row = await (this.AdminUser as any).create({
      username,
      passwordHash,
      mustChangePassword
    });
    return row.get({ plain: true }) as AdminUserRecord;
  }

  async getAdminByUsername(username: string): Promise<AdminUserRecord | null> {
    const user = await (this.AdminUser as any).findOne({ where: { username } });
    return user ? (user.get({ plain: true }) as AdminUserRecord) : null;
  }

  async getAdminById(id: number): Promise<AdminUserRecord | null> {
    const user = await (this.AdminUser as any).findByPk(id);
    return user ? (user.get({ plain: true }) as AdminUserRecord) : null;
  }

  async updateAdminPassword(id: number, passwordHash: string): Promise<void> {
    await (this.AdminUser as any).update(
      { passwordHash, mustChangePassword: false },
      { where: { id } }
    );
  }

  async setAdminMustChangePassword(id: number, mustChangePassword: boolean): Promise<void> {
    await (this.AdminUser as any).update({ mustChangePassword }, { where: { id } });
  }

  async upsertAddressStats(
    chain: string,
    address: string,
    balanceSats: string,
    totalReceivedSats: string,
    totalSentSats: string,
    txCount: number
  ): Promise<void> {
    await (this.AddressStat as any).upsert({
      chain,
      address,
      balanceSats,
      totalReceivedSats,
      totalSentSats,
      txCount
    });
  }

  async listRpcEndpoints(chain?: string): Promise<RpcEndpointConfig[]> {
    const where = chain ? { chain } : undefined;
    const rows = await (this.RpcEndpoint as any).findAll({
      where,
      order: [
        ["priority", "ASC"],
        ["id", "ASC"]
      ]
    });
    return rows.map((row: any) => this.toRpcEndpoint(row));
  }

  async addRpcEndpoint(input: Omit<RpcEndpointConfig, "id">): Promise<RpcEndpointConfig> {
    const row = await (this.RpcEndpoint as any).create({
      chain: input.chain,
      name: input.name,
      url: input.url,
      username: input.username ?? null,
      password: input.password ?? null,
      priority: input.priority ?? 0,
      enabled: input.enabled !== false
    });
    return this.toRpcEndpoint(row);
  }

  async updateRpcEndpoint(id: number, updates: Partial<RpcEndpointConfig>): Promise<void> {
    const payload: Record<string, any> = {};
    const assign = (key: string, value: any) => {
      if (value !== undefined) payload[key] = value;
    };

    assign("chain", updates.chain);
    assign("name", updates.name);
    assign("url", updates.url);
    if (updates.username !== undefined) payload.username = updates.username ?? null;
    if (updates.password !== undefined) payload.password = updates.password ?? null;
    assign("priority", updates.priority);
    assign("enabled", updates.enabled);

    await (this.RpcEndpoint as any).update(payload, { where: { id } });
  }

  async removeRpcEndpoint(id: number): Promise<void> {
    await (this.RpcEndpoint as any).destroy({ where: { id } });
  }

  async listNodes(): Promise<NodeConfig[]> {
    const rows = await (this.Node as any).findAll({ order: [["createdAt", "DESC"]] });
    return rows.map((row: any) => this.toNodeConfig(row));
  }

  async listNodesByChain(chain: string): Promise<NodeConfig[]> {
    const rows = await (this.Node as any).findAll({ where: { chain } });
    return rows.map((row: any) => this.toNodeConfig(row));
  }

  async getNode(nodeId: string): Promise<NodeConfig> {
    const row = await (this.Node as any).findOne({ where: { nodeId } });
    if (!row) {
      throw new Error(`Node not found: ${nodeId}`);
    }
    return this.toNodeConfig(row);
  }

  async addNode(node: NodeConfig): Promise<void> {
    await (this.Node as any).create({
      nodeId: node.id,
      chain: node.chain,
      datadir: node.datadir,
      p2pPort: node.p2pPort,
      rpcPort: node.rpcPort,
      rpcUser: node.rpcUser,
      rpcPassword: node.rpcPassword,
      snapshotUrl: node.snapshotUrl ?? null,
      coreVersion: node.coreVersion ?? null,
      daemonPath: node.daemonPath ?? null
    });
  }

  async updateNode(nodeId: string, updates: Partial<NodeConfig>): Promise<void> {
    const payload: Record<string, any> = {};
    const assign = (key: string, value: any) => {
      if (value !== undefined) payload[key] = value;
    };

    assign("chain", updates.chain);
    assign("datadir", updates.datadir);
    assign("p2pPort", updates.p2pPort);
    assign("rpcPort", updates.rpcPort);
    assign("rpcUser", updates.rpcUser);
    assign("rpcPassword", updates.rpcPassword);
    if (updates.snapshotUrl !== undefined) payload.snapshotUrl = updates.snapshotUrl ?? null;
    if (updates.coreVersion !== undefined) payload.coreVersion = updates.coreVersion ?? null;
    if (updates.daemonPath !== undefined) payload.daemonPath = updates.daemonPath ?? null;

    await (this.Node as any).update(payload, { where: { nodeId } });
  }

  async removeNode(nodeId: string): Promise<void> {
    await (this.Node as any).destroy({ where: { nodeId } });
  }

  async nodeIdInUse(nodeId: string): Promise<boolean> {
    const count = await (this.Node as any).count({ where: { nodeId } });
    return count > 0;
  }

  async portsInUse(ports: number[]): Promise<number[]> {
    const rows = await (this.Node as any).findAll({
      where: {
        [Op.or]: [{ p2pPort: { [Op.in]: ports } }, { rpcPort: { [Op.in]: ports } }]
      },
      attributes: ["p2pPort", "rpcPort"]
    });

    const used = new Set<number>();
    for (const row of rows) {
      used.add(row.get("p2pPort"));
      used.add(row.get("rpcPort"));
    }
    return Array.from(used);
  }

  async getCachedBlockByHash(chain: string, hash: string): Promise<any | null> {
    const row = await (this.CachedBlock as any).findOne({ where: { chain, hash } });
    if (!row) return null;
    return JSON.parse(row.get("payload"));
  }

  async getCachedBlockByHeight(chain: string, height: number): Promise<any | null> {
    const row = await (this.CachedBlock as any).findOne({ where: { chain, height } });
    if (!row) return null;
    return JSON.parse(row.get("payload"));
  }

  async saveBlock(chain: string, hash: string, height: number | null, payload: any): Promise<void> {
    const data = JSON.stringify(payload);
    await (this.CachedBlock as any).upsert({ chain, hash, height, payload: data });
  }

  async getCachedTx(chain: string, txid: string): Promise<any | null> {
    const row = await (this.CachedTx as any).findOne({ where: { chain, txid } });
    if (!row) return null;
    return JSON.parse(row.get("payload"));
  }

  async saveTx(chain: string, txid: string, payload: any): Promise<void> {
    const data = JSON.stringify(payload);
    await (this.CachedTx as any).upsert({ chain, txid, payload: data });
  }

  async getCachedAddress(chain: string, address: string): Promise<any | null> {
    const row = await (this.CachedAddress as any).findOne({ where: { chain, address } });
    if (!row) return null;
    return JSON.parse(row.get("payload"));
  }

  async saveAddress(chain: string, address: string, payload: any): Promise<void> {
    const data = JSON.stringify(payload);
    await (this.CachedAddress as any).upsert({ chain, address, payload: data });
  }

  async saveMempool(chain: string, txids: string[]): Promise<void> {
    const data = JSON.stringify({ txids });
    await (this.CachedMempool as any).create({ chain, payload: data });
  }

  async getLatestMempool(chain: string): Promise<{ txids: string[]; fetchedAt: string } | null> {
    const row = await (this.CachedMempool as any).findOne({
      where: { chain },
      order: [["fetchedAt", "DESC"]]
    });
    if (!row) return null;
    const payload = JSON.parse(row.get("payload"));
    return {
      txids: payload?.txids ?? [],
      fetchedAt: row.get("fetchedAt")?.toISOString?.() ?? new Date(row.get("fetchedAt")).toISOString()
    };
  }

  async getChainSync(chain: string): Promise<{ lastHeight: number; lastBlockHash?: string } | null> {
    const row = await (this.ChainSync as any).findOne({ where: { chain } });
    if (!row) return null;
    return {
      lastHeight: row.get("lastHeight"),
      lastBlockHash: row.get("lastBlockHash") ?? undefined
    };
  }

  async upsertChainSync(chain: string, lastHeight: number, lastBlockHash: string): Promise<void> {
    await (this.ChainSync as any).upsert({ chain, lastHeight, lastBlockHash });
  }

  async applyAddressDelta(
    chain: string,
    address: string,
    totalReceivedDelta: bigint,
    totalSentDelta: bigint,
    balanceDelta: bigint
  ): Promise<void> {
    const row = await (this.AddressStat as any).findOne({ where: { chain, address } });
    if (!row) {
      await (this.AddressStat as any).create({
        chain,
        address,
        balanceSats: balanceDelta.toString(),
        totalReceivedSats: totalReceivedDelta.toString(),
        totalSentSats: totalSentDelta.toString(),
        txCount: 0
      });
      return;
    }

    const balance = BigInt(row.get("balanceSats") || "0") + balanceDelta;
    const totalReceived = BigInt(row.get("totalReceivedSats") || "0") + totalReceivedDelta;
    const totalSent = BigInt(row.get("totalSentSats") || "0") + totalSentDelta;
    await (this.AddressStat as any).update(
      {
        balanceSats: balance.toString(),
        totalReceivedSats: totalReceived.toString(),
        totalSentSats: totalSent.toString()
      },
      { where: { chain, address } }
    );
  }

  async incrementAddressTxCount(chain: string, address: string): Promise<void> {
    const row = await (this.AddressStat as any).findOne({ where: { chain, address } });
    if (!row) {
      await (this.AddressStat as any).create({
        chain,
        address,
        balanceSats: "0",
        totalReceivedSats: "0",
        totalSentSats: "0",
        txCount: 1
      });
      return;
    }
    const txCount = Number(row.get("txCount") || 0) + 1;
    await (this.AddressStat as any).update({ txCount }, { where: { chain, address } });
  }

  async addAddressTx(
    chain: string,
    address: string,
    txid: string,
    height: number | null
  ): Promise<void> {
    await (this.AddressTx as any).upsert({ chain, address, txid, height });
  }

  async getAddressStats(
    chain: string,
    address: string
  ): Promise<{
    balanceSats: string;
    totalReceivedSats: string;
    totalSentSats: string;
    txCount: number;
  } | null> {
    const row = await (this.AddressStat as any).findOne({ where: { chain, address } });
    if (!row) return null;
    return {
      balanceSats: row.get("balanceSats") ?? "0",
      totalReceivedSats: row.get("totalReceivedSats") ?? "0",
      totalSentSats: row.get("totalSentSats") ?? "0",
      txCount: Number(row.get("txCount") ?? 0)
    };
  }

  async countAddressTxs(chain: string, address: string): Promise<number> {
    return (this.AddressTx as any).count({ where: { chain, address } });
  }

  async listAddressTxs(
    chain: string,
    address: string,
    limit: number,
    offset: number
  ): Promise<string[]> {
    const rows = await (this.AddressTx as any).findAll({
      where: { chain, address },
      order: [
        ["height", "DESC"],
        ["id", "DESC"]
      ],
      limit,
      offset
    });
    return rows.map((row: any) => row.get("txid"));
  }

  private toNodeConfig(row: any): NodeConfig {
    return {
      id: row.get("nodeId"),
      chain: row.get("chain"),
      datadir: row.get("datadir"),
      p2pPort: row.get("p2pPort"),
      rpcPort: row.get("rpcPort"),
      rpcUser: row.get("rpcUser"),
      rpcPassword: row.get("rpcPassword"),
      snapshotUrl: row.get("snapshotUrl") ?? undefined,
      coreVersion: row.get("coreVersion") ?? undefined,
      daemonPath: row.get("daemonPath") ?? undefined,
      createdAt: row.get("createdAt").toISOString()
    };
  }

  private toRpcEndpoint(row: any): RpcEndpointConfig {
    return {
      id: row.get("id"),
      chain: row.get("chain"),
      name: row.get("name"),
      url: row.get("url"),
      username: row.get("username") ?? undefined,
      password: row.get("password") ?? undefined,
      priority: row.get("priority"),
      enabled: row.get("enabled")
    };
  }
}
