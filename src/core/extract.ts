import fs from "node:fs/promises";
import extract from "extract-zip";
import * as tar from "tar";

export async function extractArchive(
  archivePath: string,
  outDir: string,
  archiveType: "zip" | "tar.gz"
): Promise<void> {
  await fs.mkdir(outDir, { recursive: true });
  if (archiveType === "zip") {
    await extract(archivePath, { dir: outDir });
    return;
  }

  await tar.x({ file: archivePath, cwd: outDir });
}
