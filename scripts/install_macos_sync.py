"""Install/uninstall two per-user macOS LaunchAgents without starting downloads."""

import argparse
import os
from pathlib import Path
import plistlib
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
JOBS = {"stock": ("com.zer0share.sync-stock", 17), "finance": ("com.zer0share.sync-finance", 2)}


def agent_config(group: str, root: Path, uv: Path) -> dict:
    label, hour = JOBS[group]
    log_path = root / "logs" / "launchd" / f"{group}.log"
    return {
        "Label": label,
        "ProgramArguments": [str(root / ".venv" / "bin" / "python"),
                             "-m", "zer0share.macos_sync", group, "--uv", str(uv)],
        "WorkingDirectory": str(root),
        "EnvironmentVariables": {
            "PATH": f"{uv.parent}:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
            "PYTHONUNBUFFERED": "1",
            "TZ": "Asia/Shanghai",
        },
        "StartCalendarInterval": {"Hour": hour, "Minute": 0},
        "RunAtLoad": False,
        "KeepAlive": False,
        "StandardOutPath": str(log_path),
        "StandardErrorPath": str(log_path),
        "ExitTimeOut": 60,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uninstall", action="store_true")
    parser.add_argument("--generate-only", action="store_true", help="只在 logs/launchd/plists 生成配置")
    args = parser.parse_args(argv)
    if sys.platform != "darwin":
        parser.error("仅支持 macOS")
    domain = f"gui/{os.getuid()}"
    agents_dir = Path.home() / "Library" / "LaunchAgents"
    if args.uninstall:
        for label, _ in JOBS.values():
            result = subprocess.run(["launchctl", "print", f"{domain}/{label}"], capture_output=True)
            if result.returncode == 0:
                subprocess.run(["launchctl", "bootout", f"{domain}/{label}"], check=True)
            (agents_dir / f"{label}.plist").unlink(missing_ok=True)
            print(f"Removed {label}")
        return 0
    uv_path = shutil.which("uv")
    if not uv_path or not (ROOT / ".venv" / "bin" / "python").exists():
        parser.error("需要 uv 和项目 .venv，请先运行 uv sync")
    generated = ROOT / "logs" / "launchd" / "plists"
    generated.mkdir(parents=True, exist_ok=True)
    for group, (label, hour) in JOBS.items():
        path = generated / f"{label}.plist"
        path.write_bytes(plistlib.dumps(agent_config(group, ROOT, Path(uv_path))))
        subprocess.run(["plutil", "-lint", str(path)], check=True)
    if args.generate_only:
        print(f"Generated {generated}")
        return 0
    # Do not write user LaunchAgents until their GUI domain is confirmed.
    subprocess.run(["launchctl", "print", domain], check=True, stdout=subprocess.DEVNULL)
    agents_dir.mkdir(parents=True, exist_ok=True)
    for group, (label, hour) in JOBS.items():
        installed = agents_dir / f"{label}.plist"
        result = subprocess.run(["launchctl", "print", f"{domain}/{label}"], capture_output=True)
        if result.returncode == 0:
            raise RuntimeError(f"{label} 已加载；请先卸载再安装，避免中断运行中的同步")
        installed.write_bytes((generated / installed.name).read_bytes())
        installed.chmod(0o644)
        subprocess.run(["launchctl", "bootstrap", domain, str(installed)], check=True)
        subprocess.run(["launchctl", "enable", f"{domain}/{label}"], check=True)
        print(f"Installed {label}: daily {hour:02d}:00; RunAtLoad=false")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
