"""nanobot Web API server."""

from __future__ import annotations

import asyncio
import secrets
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from loguru import logger
from pydantic import BaseModel

from nanobot.agent.loop import AgentLoop
from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus


class ConnectionManager:
    """Track active WebSocket connections by session key."""

    def __init__(self) -> None:
        self._connections: dict[str, list[WebSocket]] = {}

    async def connect(self, key: str, ws: WebSocket) -> None:
        await ws.accept()
        self._connections.setdefault(key, []).append(ws)
        logger.info("WS connected: {} (total {})", key, sum(len(v) for v in self._connections.values()))

    def disconnect(self, key: str, ws: WebSocket) -> None:
        conns = self._connections.get(key, [])
        if ws in conns:
            conns.remove(ws)
        if not conns:
            self._connections.pop(key, None)
        logger.info("WS disconnected: {}", key)

    async def send(self, key: str, data: dict) -> None:
        conns = list(self._connections.get(key, []))
        if not conns:
            logger.warning("Push to {} — no connected clients, message dropped", key)
            return
        for ws in conns:
            try:
                await ws.send_json(data)
                logger.debug("Push delivered to {}", key)
            except Exception as e:
                logger.warning("Push to {} failed: {}", key, e)

    def connected_keys(self) -> list[str]:
        return list(self._connections.keys())


class FileSaveRequest(BaseModel):
    path: str
    content: str


def create_app(
    agent: AgentLoop,
    bus: MessageBus,
    ui_path: Path,
    heartbeat=None,
    password: str | None = None,
    allow_host_paths: bool = False,
) -> FastAPI:
    """Build and return the FastAPI application."""

    manager = ConnectionManager()
    _tokens: set[str] = set()  # active session tokens (in-memory)

    def _check_token(request: Request) -> None:
        """Raise 401 if password is set and request carries no valid token."""
        if not password:
            return
        token = request.headers.get("X-Token") or request.query_params.get("token")
        if not token or token not in _tokens:
            raise HTTPException(status_code=401, detail="Unauthorized")

    def _resolve_path(path: str) -> Path:
        """Resolve a path according to allow_host_paths setting.

        Returns the resolved absolute Path.
        Raises HTTPException 403 if path escapes workspace when not allow_host_paths.
        """
        workspace = agent.context.workspace.resolve()
        if allow_host_paths and path and Path(path).is_absolute():
            target = Path(path).resolve()
        else:
            target = (workspace / path).resolve() if path else workspace
            # Restrict to workspace subtree
            try:
                target.relative_to(workspace)
            except ValueError:
                raise HTTPException(status_code=403, detail="Path outside workspace")
        return target

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        task = asyncio.create_task(_dispatch_outbound())
        logger.info("Outbound dispatcher started")
        yield
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    app = FastAPI(title="nanobot", version="1.0", lifespan=lifespan)

    async def _dispatch_outbound() -> None:
        """Forward bus outbound messages (e.g. cron alerts) to WebSocket clients."""
        while True:
            try:
                msg: OutboundMessage = await bus.consume_outbound()
                if msg.metadata.get("_progress"):
                    continue
                key = f"{msg.channel}:{msg.chat_id}"
                logger.info("Dispatch push -> {} ({} chars)", key, len(msg.content or ""))
                await manager.send(key, {"type": "push", "content": msg.content})
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Outbound dispatch error: {}", e)

    # ------------------------------------------------------------------ UI
    @app.get("/")
    async def serve_ui():
        html = ui_path / "index.html"
        if html.exists():
            return FileResponse(html)
        return JSONResponse({"error": "UI not found"}, status_code=404)

    # ------------------------------------------------------------------ Auth
    @app.post("/v1/auth")
    async def authenticate(request: Request):
        """Validate password and return a session token."""
        if not password:
            # No password configured — return a dummy token
            return {"token": "no-auth", "required": False}
        body = await request.json()
        if body.get("password") == password:
            token = secrets.token_hex(32)
            _tokens.add(token)
            return {"token": token, "required": True}
        raise HTTPException(status_code=403, detail="Invalid password")

    @app.get("/v1/auth/required")
    async def auth_required():
        return {"required": bool(password)}

    # ------------------------------------------------------------------ REST
    @app.get("/v1/health")
    async def health():
        return {"status": "ok"}

    @app.get("/v1/debug/system-prompt")
    async def debug_system_prompt(request: Request):
        _check_token(request)
        prompt = agent.context.build_system_prompt()
        skills = agent.context.skills.list_skills(filter_unavailable=False)
        always = agent.context.skills.get_always_skills()
        return {
            "workspace": str(agent.context.workspace),
            "always_skills": always,
            "all_skills": skills,
            "system_prompt": prompt,
        }

    @app.post("/v1/heartbeat/trigger")
    async def trigger_heartbeat(request: Request):
        _check_token(request)
        if heartbeat is None:
            return JSONResponse({"error": "heartbeat not running"}, status_code=503)
        result = await heartbeat.trigger_now()
        return {"triggered": True, "result": result or "(no active tasks)"}

    @app.get("/v1/sessions")
    async def list_sessions(request: Request):
        _check_token(request)
        return {"sessions": agent.sessions.list_sessions()}

    @app.delete("/v1/sessions/{session_id:path}")
    async def clear_session(session_id: str, request: Request):
        _check_token(request)
        session = agent.sessions.get_or_create(session_id)
        session.clear()
        agent.sessions.save(session)
        agent.sessions.invalidate(session_id)
        # Also remove the JSONL file so it disappears from the list
        path = agent.sessions._get_session_path(session_id)
        if path.exists():
            path.unlink()
        return {"ok": True, "session_id": session_id}

    @app.get("/v1/sessions/{session_id:path}/messages")
    async def get_session_messages(session_id: str, request: Request):
        _check_token(request)
        session = agent.sessions.get_or_create(session_id)
        history = []
        for m in session.messages:
            role = m.get("role")
            content = m.get("content", "")
            if role in ("user", "assistant") and content:
                history.append({"role": role, "content": content})
        return {"messages": history}

    @app.get("/v1/sessions/{session_id:path}/export")
    async def export_session(session_id: str, request: Request, format: str = Query("md")):
        """Export a session as a markdown file download."""
        _check_token(request)
        import tempfile

        session = agent.sessions.get_or_create(session_id)
        lines = []
        for m in session.messages:
            role = m.get("role")
            content = m.get("content", "")
            if not content:
                continue
            if role == "user":
                lines.append(f"**You:** {content}\n")
            elif role == "assistant":
                lines.append(f"**nanobot:** {content}\n")

        md_content = "\n".join(lines)

        # Write to a temp file for FileResponse
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".md", delete=False, encoding="utf-8"
        )
        tmp.write(md_content)
        tmp.flush()
        tmp.close()

        safe_id = session_id.replace("/", "_").replace(":", "_")
        filename = f"session_{safe_id}.md"
        return FileResponse(
            tmp.name,
            media_type="text/markdown",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    # ------------------------------------------------------------------ Files
    @app.get("/v1/files")
    async def list_files(request: Request, path: str = Query("")):
        """Browse the workspace directory tree (or host if allow_host_paths)."""
        _check_token(request)
        workspace = agent.context.workspace.resolve()
        target = _resolve_path(path)

        if not target.exists():
            raise HTTPException(status_code=404, detail="Path not found")

        if target.is_file():
            # Return relative path if within workspace, else absolute
            try:
                rel = str(target.relative_to(workspace))
            except ValueError:
                rel = str(target)
            try:
                content = target.read_text(encoding="utf-8", errors="replace")
            except Exception:
                content = "(binary file)"
            return {"type": "file", "path": rel, "name": target.name, "content": content}

        # Directory listing
        try:
            rel = str(target.relative_to(workspace)) if target != workspace else ""
        except ValueError:
            rel = str(target)

        entries = []
        for entry in sorted(target.iterdir(), key=lambda e: (e.is_file(), e.name.lower())):
            try:
                size = entry.stat().st_size if entry.is_file() else None
            except OSError:
                size = None
            # Path for the entry
            try:
                entry_path = str(entry.relative_to(workspace))
            except ValueError:
                entry_path = str(entry)
            entries.append({
                "name": entry.name,
                "type": "dir" if entry.is_dir() else "file",
                "size": size,
                "path": entry_path,
            })
        return {"type": "dir", "path": rel, "entries": entries}

    @app.put("/v1/files")
    async def save_file(request: Request, body: FileSaveRequest):
        """Save (write) file content to disk."""
        _check_token(request)
        target = _resolve_path(body.path)

        if target.is_dir():
            raise HTTPException(status_code=400, detail="Path is a directory")

        target.parent.mkdir(parents=True, exist_ok=True)
        encoded = body.content.encode("utf-8")
        target.write_bytes(encoded)
        return {"ok": True, "bytes": len(encoded)}

    @app.get("/v1/files/download")
    async def download_file(request: Request, path: str = Query(...)):
        """Download a file as an attachment."""
        _check_token(request)
        target = _resolve_path(path)

        if not target.exists():
            raise HTTPException(status_code=404, detail="File not found")
        if target.is_dir():
            raise HTTPException(status_code=400, detail="Path is a directory")

        return FileResponse(
            target,
            filename=target.name,
            headers={"Content-Disposition": f'attachment; filename="{target.name}"'},
        )

    # ------------------------------------------------------------------ Search
    @app.get("/v1/search/files")
    async def search_files(
        request: Request,
        q: str = Query(...),
        path: str = Query(""),
        max: int = Query(100),
    ):
        """Search filenames (case-insensitive substring match)."""
        _check_token(request)
        root = _resolve_path(path) if path else agent.context.workspace.resolve()

        if not root.exists():
            raise HTTPException(status_code=404, detail="Search root not found")

        results: list[dict] = []
        q_lower = q.lower()

        def _walk(d: Path) -> None:
            if len(results) >= max:
                return
            try:
                entries = list(d.iterdir())
            except PermissionError:
                return
            for entry in entries:
                if len(results) >= max:
                    return
                if q_lower in entry.name.lower():
                    results.append({
                        "name": entry.name,
                        "path": str(entry),
                        "type": "dir" if entry.is_dir() else "file",
                    })
                if entry.is_dir():
                    _walk(entry)

        _walk(root)
        return {"results": results}

    @app.get("/v1/search/content")
    async def search_content(
        request: Request,
        q: str = Query(...),
        path: str = Query(""),
        max: int = Query(50),
    ):
        """Search file contents (grep-like, case-insensitive)."""
        _check_token(request)
        root = _resolve_path(path) if path else agent.context.workspace.resolve()

        if not root.exists():
            raise HTTPException(status_code=404, detail="Search root not found")

        results: list[dict] = []
        q_lower = q.lower()
        MAX_FILE_SIZE = 512 * 1024  # 512 KB

        def _walk(d: Path) -> None:
            if len(results) >= max:
                return
            try:
                entries = list(d.iterdir())
            except PermissionError:
                return
            for entry in entries:
                if len(results) >= max:
                    return
                if entry.is_dir():
                    _walk(entry)
                elif entry.is_file():
                    try:
                        if entry.stat().st_size > MAX_FILE_SIZE:
                            continue
                    except OSError:
                        continue
                    try:
                        text = entry.read_text(encoding="utf-8")
                    except (UnicodeDecodeError, OSError):
                        continue
                    for lineno, line in enumerate(text.splitlines(), 1):
                        if len(results) >= max:
                            return
                        if q_lower in line.lower():
                            results.append({
                                "file": str(entry),
                                "line": lineno,
                                "preview": line.strip()[:200],
                            })

        _walk(root)
        return {"results": results}

    # ---- Diagnostic: push a test message to verify the WS push pipeline ----
    @app.post("/v1/test-push/{session_id}")
    async def test_push(session_id: str, request: Request, content: str = "Test push notification"):
        _check_token(request)
        key = f"web:{session_id}"
        keys = manager.connected_keys()
        logger.info("test-push -> key={}, connected_keys={}", key, keys)
        await manager.send(key, {"type": "push", "content": content})
        return {"ok": True, "key": key, "connected_keys": keys}

    # ------------------------------------------------------------------ WebSocket
    @app.websocket("/v1/ws/{session_id:path}")
    async def ws_endpoint(ws: WebSocket, session_id: str, token: str = Query("")):
        if password and token not in _tokens:
            await ws.close(code=4001)
            return

        channel, chat_id = "web", session_id
        key = f"{channel}:{chat_id}"
        await manager.connect(key, ws)
        try:
            while True:
                data = await ws.receive_json()
                content = (data.get("content") or "").strip()
                if not content:
                    continue

                async def on_progress(text: str, *, tool_hint: bool = False, skill_hint: bool = False) -> None:
                    if skill_hint:
                        msg_type = "skill"
                    elif tool_hint:
                        msg_type = "tool"
                    else:
                        msg_type = "progress"
                    await ws.send_json({"type": msg_type, "content": text})

                msg = InboundMessage(
                    channel=channel,
                    sender_id="web_user",
                    chat_id=chat_id,
                    content=content,
                )
                response = await agent._process_message(
                    msg, session_key=key, on_progress=on_progress
                )
                if response is not None:
                    await ws.send_json({
                        "type": "message",
                        "content": response.content or "",
                    })
        except WebSocketDisconnect:
            pass
        except Exception as e:
            logger.error("WebSocket error for {}: {}", key, e)
        finally:
            manager.disconnect(key, ws)

    return app
