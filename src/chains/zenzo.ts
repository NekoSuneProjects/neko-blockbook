import type { ChainPlugin } from "../types.js";

export const zenzo: ChainPlugin = {
  id: "zenzo",
  name: "Zenzo",
  symbol: "ZNZ",
  daemon: {
    win32: "zenzod.exe",
    linux: "zenzod"
  },
  defaultPorts: {
    p2p: 26210,
    rpc: 26211
  },
  releases: {
    "2.1.0": {
      "win32-x64": {
        url: "https://github.com/ZENZO-Ecosystem/ZENZO-Core/releases/download/v2.1.0/zenzo-2.1.0-win64.zip",
        sha256: "b57a994a236ee915443b0b7d4f19bb338235cd94e2ce9e99baf9d065b7abd6da",
        archive: "zip"
      },
      "linux-x64": {
        url: "https://github.com/ZENZO-Ecosystem/ZENZO-Core/releases/download/v2.1.0/zenzo-2.1.0-x86_64-linux-gnu.tar.gz",
        sha256: "1f3a85d2344bd92255b438a15ed4fd04398b5a78e0ee42133798ff49a554e72d",
        archive: "tar.gz"
      },
      "linux-arm": {
        url: "https://github.com/ZENZO-Ecosystem/ZENZO-Core/releases/download/v2.1.0/zenzo-2.1.0-arm-linux-gnueabihf.tar.gz",
        sha256: "6e6b2fc49bedb04d0230de2880c8e122e6a3ab622243a7732fe595d3dbcec07c",
        archive: "tar.gz"
      },
      "linux-arm64": {
        url: "https://github.com/ZENZO-Ecosystem/ZENZO-Core/releases/download/v2.1.0/zenzo-2.1.0-aarch64-linux-gnu.tar.gz",
        sha256: "f613022307e1af7e95cf91e6940c1ad62f74b1beafcee9645b89ac43e3c4f963",
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

addnode=109.132.146.48
addnode=109.205.181.156
addnode=128.0.222.192
addnode=135.181.183.38
addnode=144.91.100.97
addnode=144.91.106.129
addnode=144.91.107.207
addnode=144.91.88.42
addnode=161.97.127.55
addnode=161.97.132.198
addnode=161.97.166.81
addnode=161.97.78.108
addnode=161.97.79.101
addnode=161.97.87.227
addnode=161.97.97.43
addnode=164.68.102.142
addnode=164.68.102.158
addnode=164.68.102.161
addnode=164.68.102.162
addnode=164.68.120.4
addnode=164.68.124.2
addnode=164.68.96.52
addnode=167.86.109.168
addnode=167.86.110.243
addnode=167.86.111.155
addnode=167.86.122.207
addnode=167.86.124.240
addnode=167.86.124.249
addnode=167.86.67.217
addnode=167.86.67.228
addnode=167.86.89.215
addnode=167.86.91.175
addnode=167.86.95.121
addnode=173.212.204.201
addnode=173.212.204.94
addnode=173.212.208.86
addnode=173.212.209.163
addnode=173.212.223.176
addnode=173.212.234.14
addnode=173.212.236.11
addnode=173.212.239.223
addnode=173.212.243.102
addnode=173.212.251.138
addnode=173.212.252.99
addnode=173.212.253.129
addnode=173.249.23.100
addnode=173.249.25.155
addnode=173.249.40.165
addnode=178.18.244.204
addnode=178.18.251.166
addnode=185.135.137.92
addnode=185.197.251.144
addnode=185.197.251.146
addnode=185.197.251.148
addnode=185.249.225.155
addnode=188.40.92.108
addnode=207.180.204.7
addnode=207.180.205.241
addnode=207.180.253.183
addnode=207.180.254.111
addnode=209.182.216.217
addnode=209.182.218.57
addnode=213.136.75.61
addnode=213.136.85.58
addnode=213.136.88.100
addnode=38.242.150.174
addnode=38.242.151.69
addnode=38.242.198.233
addnode=38.242.204.134
addnode=38.242.214.125
addnode=38.242.214.141
addnode=38.242.217.51
addnode=5.182.33.47
addnode=5.189.135.33
addnode=5.189.158.224
addnode=5.189.171.173
addnode=51.77.116.109
addnode=62.171.131.253
addnode=62.171.144.144
addnode=62.171.145.199
addnode=62.171.147.162
addnode=62.171.154.116
addnode=62.171.158.197
addnode=62.171.158.199
addnode=62.171.164.249
addnode=62.171.181.40
addnode=62.171.182.92
addnode=62.171.187.168
addnode=62.171.190.139
addnode=75.119.139.78
addnode=75.119.153.109
addnode=75.119.153.169
addnode=75.119.153.170
addnode=82.74.118.16
addnode=86.48.3.82
addnode=89.187.90.8
addnode=95.111.230.134
addnode=95.111.236.157

addnode=[2a02:c207:2037:2792::1001]:26210
addnode=[2a02:c207:2037:2792::1002]:26210
addnode=[2a02:c207:2037:2792::1003]:26210
addnode=[2a02:c207:2037:2792::1004]:26210
addnode=[2a02:c207:2037:2792::1005]:26210
addnode=[2a02:c207:2037:2792::1006]:26210
addnode=[2a02:c207:2037:2792::1007]:26210
addnode=[2a02:c207:2037:2792::1008]:26210
addnode=[2a02:c207:2037:2792::1009]:26210
addnode=[2a02:c207:2037:2792::1010]:26210
addnode=[2a02:c207:2037:2792::1011]:26210
addnode=[2a02:c207:2037:2792::1012]:26210
addnode=[2a02:c207:2037:2792::1013]:26210
addnode=[2a02:c207:2037:2792::1014]:26210
addnode=[2a02:c207:2037:2792::1015]:26210
addnode=[2a02:c207:2037:2792::1016]:26210
addnode=[2a02:c207:2037:2792::1017]:26210
addnode=[2a02:c207:2037:2792::1018]:26210
addnode=[2a02:c207:2037:2792::1019]:26210
addnode=[2a02:c207:2037:2792::1020]:26210
addnode=[2a02:c207:2037:2792::1021]:26210
addnode=[2a02:c207:2037:2792::1022]:26210
addnode=[2a02:c207:2037:2792::1023]:26210
addnode=[2a02:c207:2037:2792::1024]:26210
addnode=[2a02:c207:2037:2792::1025]:26210
addnode=[2a02:c207:2037:2792::1026]:26210
addnode=[2a02:c207:2037:2792::1027]:26210
addnode=[2a02:c207:2037:2792::1028]:26210
addnode=[2a02:c207:2037:2792::1029]:26210
addnode=[2a02:c207:2037:2792::1030]:26210

addnode=164.68.120.4:26210
addnode=185.194.217.23:26210
addnode=[2605:a140:2184:6619:267d::6]:26210
addnode=[2605:a142:2073:5573:b564::5]:26210
addnode=161.97.182.56:26210
addnode=[2001:470:1f09:a53:0:400:5:1]:26210
addnode=[2a02:c207:2241:4754:1e33::5]:26210
addnode=178.45.84.15:26210
addnode=[2a02:c202:2191:1859:7888::3]:26210
addnode=[2607:5300:201:3100::935d]:26210
addnode=144.217.84.156:26210

addnode=144.91.80.84
addnode=207.180.246.118
addnode=185.215.165.58
addnode=164.68.102.142
addnode=62.171.183.112
addnode=207.180.246.118
addnode=185.215.165.58
addnode=164.68.102.142
addnode=62.171.183.112
`.trim();
  },
  rpc: {
    stop: "stop",
    blockCount: "getblockcount"
  }
};
