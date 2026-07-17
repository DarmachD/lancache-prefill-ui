import asyncio
import os
import pty
import select
import signal
import struct
import subprocess
import termios
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel

app = FastAPI(title="LANCache Prefill UI")

TARGET_CONTAINER = os.getenv("TARGET_CONTAINER", "LANCache-Prefill")
PREFILL_DIR = os.getenv("PREFILL_DIR", "/lancacheprefill/SteamPrefill")
PREFILL_USER = os.getenv("PREFILL_USER", "prefill")
PORT = int(os.getenv("PORT", "8080"))

ALLOWED_ACTIONS = {
    "prefill": "./SteamPrefill prefill",
    "select": "./SteamPrefill select-apps",
    "status": "./SteamPrefill status",
    "clear-cache": "./SteamPrefill clear-cache -y",
}

class ActionRequest(BaseModel):
    action: str

def docker_exec_command(command: str, interactive: bool = False) -> list[str]:
    args = ["docker", "exec"]
    if interactive:
        args += ["-it"]
    args += [
        "--user", PREFILL_USER,
        "--workdir", PREFILL_DIR,
        TARGET_CONTAINER,
        "bash", "-lc", command,
    ]
    return args

@app.get("/")
async def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")

@app.get("/api/health")
async def health():
    check = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", TARGET_CONTAINER],
        capture_output=True, text=True, timeout=10
    )
    return {
        "target": TARGET_CONTAINER,
        "running": check.returncode == 0 and check.stdout.strip() == "true",
        "detail": check.stderr.strip() if check.returncode else "",
    }

@app.post("/api/action")
async def action(req: ActionRequest):
    if req.action not in ALLOWED_ACTIONS:
        raise HTTPException(400, "Unsupported action")
    # Non-interactive actions only. Interactive selection/prefill runs in terminal.
    if req.action in {"select", "prefill"}:
        raise HTTPException(400, "Use the browser terminal for this action")
    proc = subprocess.run(
        docker_exec_command(ALLOWED_ACTIONS[req.action]),
        capture_output=True, text=True, timeout=120
    )
    return {
        "ok": proc.returncode == 0,
        "code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }

@app.websocket("/ws/terminal")
async def terminal(ws: WebSocket):
    await ws.accept()
    master_fd, slave_fd = pty.openpty()
    env = os.environ.copy()
    env["TERM"] = "xterm-256color"

    command = docker_exec_command("exec bash", interactive=True)
    proc = subprocess.Popen(
        command,
        stdin=slave_fd,
        stdout=slave_fd,
        stderr=slave_fd,
        env=env,
        close_fds=True,
        start_new_session=True,
    )
    os.close(slave_fd)

    async def pump_output():
        loop = asyncio.get_running_loop()
        while proc.poll() is None:
            ready, _, _ = await loop.run_in_executor(
                None, lambda: select.select([master_fd], [], [], 0.2)
            )
            if ready:
                try:
                    data = os.read(master_fd, 8192)
                    if not data:
                        break
                    await ws.send_bytes(data)
                except OSError:
                    break

    output_task = asyncio.create_task(pump_output())
    try:
        while True:
            message = await ws.receive()
            if "text" in message and message["text"] is not None:
                text = message["text"]
                if text.startswith("__RESIZE__:"):
                    try:
                        cols, rows = map(int, text.split(":")[1:])
                        winsize = struct.pack("HHHH", rows, cols, 0, 0)
                        import fcntl
                        fcntl.ioctl(master_fd, termios.TIOCSWINSZ, winsize)
                    except Exception:
                        pass
                else:
                    os.write(master_fd, text.encode())
            elif "bytes" in message and message["bytes"] is not None:
                os.write(master_fd, message["bytes"])
    except WebSocketDisconnect:
        pass
    finally:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        output_task.cancel()
        os.close(master_fd)
