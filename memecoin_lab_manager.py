import os
import sys
import time
import signal
import subprocess
from pathlib import Path

BASE = Path.home() / "memecoin_lab"
LOG_DIR = BASE / "logs"
PID_DIR = BASE / "pids"

PYTHON = BASE / ".venv" / "bin" / "python"

PROCESSES = {
    "event": "event_tracker_v090.py",
    "price": "price_tracker_v100.py",
    "sequence": "event_sequence_v340.py",
    "regime": "frozen_regime_forward_v620.py",
    "t23": "v2_frozen_prospective_t23.py",
    "t31": "t31_frozen_base_prospective_execution.py",
    "t32": "t32_prospective_shadow_recorder.py",
    "t47": "t47_fastflip_prospective_shadow.py",
}

MONITOR = "master_monitor_v810.py"


def ensure_dirs():
    LOG_DIR.mkdir(exist_ok=True)
    PID_DIR.mkdir(exist_ok=True)


def pid_file(name):
    return PID_DIR / f"{name}.pid"


def log_file(name):
    return LOG_DIR / f"{name}.log"


def read_pid(name):
    p = pid_file(name)

    if not p.exists():
        return None

    try:
        return int(p.read_text().strip())
    except Exception:
        return None


def process_exists(pid):
    if not pid:
        return False

    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def find_process_by_script(script):
    try:
        out = subprocess.check_output(
            ["ps", "-axo", "pid,command"],
            text=True
        )
    except Exception:
        return None

    for line in out.splitlines():
        if script in line and "memecoin_lab_manager.py" not in line:
            try:
                pid = int(line.strip().split(None, 1)[0])
                return pid
            except Exception:
                pass

    return None


def current_pid(name):
    script = PROCESSES[name]

    pid = read_pid(name)

    if process_exists(pid):
        return pid

    pid = find_process_by_script(script)

    if process_exists(pid):
        pid_file(name).write_text(str(pid))
        return pid

    return None


def start_one(name):
    ensure_dirs()

    script = PROCESSES[name]
    script_path = BASE / script

    if not script_path.exists():
        print(f"❌ {name:8} missing: {script}")
        return

    pid = current_pid(name)

    if pid:
        print(f"✅ {name:8} already running | PID={pid}")
        return

    lf = open(log_file(name), "a")

    proc = subprocess.Popen(
        [str(PYTHON), str(script_path)],
        cwd=str(BASE),
        stdout=lf,
        stderr=subprocess.STDOUT,
        start_new_session=True
    )

    pid_file(name).write_text(str(proc.pid))

    print(
        f"🚀 {name:8} started | "
        f"PID={proc.pid} | "
        f"log={log_file(name).name}"
    )


def start_all():
    print("=" * 90)
    print("MEMECOIN LAB — START")
    print("=" * 90)

    for name in PROCESSES:
        start_one(name)

    print()
    print("Done.")


def status():
    print("=" * 100)
    print("MEMECOIN LAB — PROCESS STATUS")
    print("=" * 100)

    running = 0

    for name, script in PROCESSES.items():
        pid = current_pid(name)

        if pid:
            running += 1

            print(
                f"✅ {name:8} "
                f"PID={pid:<7} "
                f"{script}"
            )
        else:
            print(
                f"❌ {name:8} "
                f"{'':11}"
                f"{script}"
            )

    print("-" * 100)
    print(
        f"RUNNING={running}/{len(PROCESSES)}"
    )


def stop_one(name):
    pid = current_pid(name)

    if not pid:
        print(f"○  {name:8} already stopped")
        try:
            pid_file(name).unlink()
        except FileNotFoundError:
            pass
        return

    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    except PermissionError:
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass

    for _ in range(20):
        time.sleep(0.25)

        if not process_exists(pid):
            break

    if process_exists(pid):
        try:
            os.killpg(pid, signal.SIGKILL)
        except Exception:
            try:
                os.kill(pid, signal.SIGKILL)
            except Exception:
                pass

    try:
        pid_file(name).unlink()
    except FileNotFoundError:
        pass

    print(
        f"🛑 {name:8} stopped | PID={pid}"
    )


def stop_all():
    print("=" * 90)
    print("MEMECOIN LAB — STOP")
    print("=" * 90)

    for name in reversed(list(PROCESSES.keys())):
        stop_one(name)


def restart_all():
    stop_all()

    time.sleep(1)

    print()

    start_all()


def monitor():
    monitor_path = BASE / MONITOR

    if not monitor_path.exists():
        print(
            f"❌ Monitor missing: {MONITOR}"
        )
        return

    os.execv(
        str(PYTHON),
        [
            str(PYTHON),
            str(monitor_path)
        ]
    )


def tail(name=None):
    if name:

        if name not in PROCESSES:
            print(
                "Unknown process. Available:",
                ", ".join(PROCESSES)
            )
            return

        path = log_file(name)

        if not path.exists():
            print(
                f"No log yet: {path}"
            )
            return

        os.execvp(
            "tail",
            [
                "tail",
                "-f",
                str(path)
            ]
        )

    else:

        print(
            f"Logs directory: {LOG_DIR}"
        )

        for n in PROCESSES:
            path = log_file(n)

            if path.exists():
                print(
                    f"{n:8} -> {path.name}"
                )


def usage():
    print("""
MEMECOIN LAB MANAGER

Usage:

  python memecoin_lab_manager.py start
  python memecoin_lab_manager.py status
  python memecoin_lab_manager.py monitor
  python memecoin_lab_manager.py restart
  python memecoin_lab_manager.py stop

Logs:

  python memecoin_lab_manager.py logs
  python memecoin_lab_manager.py logs t23
  python memecoin_lab_manager.py logs t31
  python memecoin_lab_manager.py logs t47

Individual process:

  python memecoin_lab_manager.py start t47
  python memecoin_lab_manager.py stop t47
""")


def main():
    ensure_dirs()

    if len(sys.argv) < 2:
        usage()
        return

    cmd = sys.argv[1].lower()

    name = (
        sys.argv[2].lower()
        if len(sys.argv) >= 3
        else None
    )

    if cmd == "start":
        if name:
            if name not in PROCESSES:
                print("Unknown process:", name)
                return
            start_one(name)
        else:
            start_all()

    elif cmd == "status":
        status()

    elif cmd == "stop":
        if name:
            if name not in PROCESSES:
                print("Unknown process:", name)
                return
            stop_one(name)
        else:
            stop_all()

    elif cmd == "restart":
        restart_all()

    elif cmd == "monitor":
        monitor()

    elif cmd in ("log", "logs"):
        tail(name)

    else:
        usage()


if __name__ == "__main__":
    main()
