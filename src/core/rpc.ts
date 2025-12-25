import type { RpcTarget } from "../types.js";

export async function rpcCall<T = any>(
  target: RpcTarget,
  method: string,
  params: unknown[] = []
): Promise<T> {
  const headers: Record<string, string> = {
    "content-type": "application/json"
  };

  if (target.username || target.password) {
    headers.authorization =
      "Basic " + Buffer.from(`${target.username ?? ""}:${target.password ?? ""}`).toString("base64");
  }

  const res = await fetch(target.url, {
    method: "POST",
    headers,
    body: JSON.stringify({ jsonrpc: "1.0", id: "neko-explorer", method, params })
  });

  if (!res.ok) {
    const bodyText = await res.text();
    let errorMessage = bodyText;
    if (bodyText) {
      try {
        const parsed = JSON.parse(bodyText) as { error?: { message?: string } | string };
        if (parsed?.error && typeof parsed.error === "object" && parsed.error.message) {
          errorMessage = parsed.error.message;
        } else if (typeof parsed?.error === "string") {
          errorMessage = parsed.error;
        }
      } catch {
        // Non-JSON error body.
      }
    }

    const suffix = errorMessage ? ` - ${errorMessage}` : "";
    throw new Error(`RPC ${method} error: ${res.status} ${res.statusText}${suffix}`);
  }

  const json = (await res.json()) as { result: T; error?: { message?: string } };
  if (json.error) {
    throw new Error(json.error.message ?? `RPC ${method} error`);
  }

  return json.result;
}

export async function rpcCallWithFallback<T = any>(
  targets: RpcTarget[],
  method: string,
  params: unknown[] = []
): Promise<{ result: T; target: RpcTarget }> {
  let lastError: Error | null = null;
  for (const target of targets) {
    try {
      const result = await rpcCall<T>(target, method, params);
      return { result, target };
    } catch (error) {
      lastError = error instanceof Error ? error : new Error("RPC error");
    }
  }

  throw lastError ?? new Error("RPC error");
}
