import { execa } from "execa";

interface StartNodeInput {
  daemonPath: string;
  datadir: string;
  confPath: string;
}

export function startNodeProcess({ daemonPath, datadir, confPath }: StartNodeInput): void {
  const subprocess = execa(daemonPath, [`-datadir=${datadir}`, `-conf=${confPath}`], {
    windowsHide: true,
    detached: true,
    stdio: "ignore"
  });
  subprocess.unref();
}