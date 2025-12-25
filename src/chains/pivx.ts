import type { ChainPlugin } from "../types.js";

export const pivx: ChainPlugin = {
  id: "pivx",
  name: "PIVX",
  symbol: "PIVX",
  daemon: {
    win32: "pivx-qt.exe",
    linux: "pivxd"
  },
  defaultPorts: {
    p2p: 51472,
    rpc: 51473
  },
  releases: {
    "5.6.1": {
      "win32-x64": {
        url: "https://github.com/PIVX-Project/PIVX/releases/download/v5.6.1/pivx-5.6.1-win64.zip",
        sha256: "ae3a7896dee74600665af717fb5785f52be3e2f5cab3a57873020c66bdff54fc",
        archive: "zip"
      },
      "linux-x64": {
        url: "https://github.com/PIVX-Project/PIVX/releases/download/v5.6.1/pivx-5.6.1-x86_64-linux-gnu.tar.gz",
        sha256: "6704625c63ff73da8c57f0fbb1dab6f1e4bd8f62c17467e05f52a64012a0ee2f",
        archive: "tar.gz"
      },
      "linux-arm64": {
        url: "https://github.com/PIVX-Project/PIVX/releases/download/v5.6.1/pivx-5.6.1-aarch64-linux-gnu.tar.gz",
        sha256: "8f1c0243f8a21da6cce51b96c5317c425b9c50e3de2949b2e838ead11426b447",
        archive: "tar.gz"
      },
      "linux-arm": {
        url: "https://github.com/PIVX-Project/PIVX/releases/download/v5.6.1/pivx-5.6.1-arm-linux-gnueabihf.tar.gz",
        sha256: "f865dede694de837aa57f10096dbf99efdef1dda210433d5b33111aa81085074",
        archive: "tar.gz"
      }
    }
  },
  config(node) {
    return `
server=1
daemon=1
listen=1

port=${node.p2pPort}
rpcport=${node.rpcPort}
rpcuser=${node.rpcUser}
rpcpassword=${node.rpcPassword}
rpcallowip=127.0.0.1
rpcbind=127.0.0.1

txindex=1
`.trim();
  },
  rpc: {
    stop: "stop",
    blockCount: "getblockcount"
  }
};
