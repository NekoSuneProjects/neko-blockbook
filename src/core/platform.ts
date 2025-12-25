import type { PlatformKey } from "../types.js";

export type BasePlatform = "win32" | "linux" | "darwin";

export function getPlatformKey(): PlatformKey {
  const platform = process.platform;
  const arch = process.arch;

  if (platform === "win32" && arch === "x64") return "win32-x64";
  if (platform === "linux" && arch === "x64") return "linux-x64";
  if (platform === "linux" && arch === "arm") return "linux-arm";
  if (platform === "linux" && arch === "arm64") return "linux-arm64";
  if (platform === "darwin" && arch === "x64") return "darwin-x64";
  if (platform === "darwin" && arch === "arm64") return "darwin-arm64";

  throw new Error(`Unsupported platform: ${platform} ${arch}`);
}

export function getBasePlatform(platformKey: PlatformKey): BasePlatform {
  if (platformKey.startsWith("win32")) return "win32";
  if (platformKey.startsWith("linux")) return "linux";
  return "darwin";
}