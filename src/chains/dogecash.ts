import type { ChainPlugin } from "../types.js";

export const dogecash: ChainPlugin = {
  id: "dogecash",
  name: "DogeCash",
  symbol: "DOGEC",
  daemon: {
    win32: "dogecash-qt.exe",
    linux: "dogecashd"
  },
  defaultPorts: {
    p2p: 22556,
    rpc: 22555
  },
  releases: {
    "5.5.1": {
      "win32-x64": {
        url: "https://github.com/dogecash/dogecash/releases/download/5.5.1/dogecash-5.5.1-win64.zip",
        sha256: "5e470328ee68750a2f6e86f75c83cecaeac2ad973f5573b83dcb6e3156a91591",
        archive: "zip"
      },
      "linux-x64": {
        url: "https://github.com/dogecash/dogecash/releases/download/5.5.1/dogecash-5.5.1-x86_64-linux-gnu.tar.gz",
        sha256: "deab49107e8930148e24bb922b3bef6f009e3709d425619ff0044d121d7a02ef",
        archive: "tar.gz"
      },
      "linux-arm": {
        url: "https://github.com/dogecash/dogecash/releases/download/5.5.1/dogecash-5.5.1-arm-linux-gnueabihf.tar.gz",
        sha256: "5c94f91b5e948dddda151bf1379324428d7223abaeb072a406410f6cb155d4b0",
        archive: "tar.gz"
      },
      "linux-arm64": {
        url: "https://github.com/dogecash/dogecash/releases/download/5.5.1/dogecash-5.5.1-aarch64-linux-gnu.tar.gz",
        sha256: "5882c0e2307775e7c257c6728bd4eeef7f0112b7e42181662df4a2ffbd20e10d",
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
