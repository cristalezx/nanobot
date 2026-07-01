"""nanobot Web API server."""

from __future__ import annotations

import asyncio
import json
import re
import secrets
import shutil
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import (
    FastAPI,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import FileResponse, JSONResponse
from loguru import logger
from pydantic import BaseModel

from nanobot.agent.loop import AgentLoop
from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.cron.types import CronSchedule


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


class ShareRequest(BaseModel):
    session_id: str


class ModelSwitchRequest(BaseModel):
    model: str


class SkillSaveRequest(BaseModel):
    content: str


class SkillFileSaveRequest(BaseModel):
    path: str
    content: str


class ReportPublishRequest(BaseModel):
    path: str


class CronCreateRequest(BaseModel):
    name: str
    message: str
    schedule_kind: str = "cron"   # "cron" | "every"
    expr: str | None = None       # for kind="cron", e.g. "0 8 * * 1"
    every_ms: int | None = None   # for kind="every"
    tz: str | None = None
    deliver: bool = True


class CronEnableRequest(BaseModel):
    enabled: bool


class AdminUserUpdate(BaseModel):
    can_publish: bool | None = None


# Skill names map to directory names — restrict to a safe character set.
_SKILL_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]+$")


def create_app(
    agent: AgentLoop,
    bus: MessageBus,
    ui_path: Path,
    heartbeat=None,
    password: str | None = None,
    allow_host_paths: bool = False,
    user_agents: dict[str, dict] | None = None,
    board_dir: Path | None = None,
    users_config_path: Path | None = None,
) -> FastAPI:
    """Build and return the FastAPI application.

    ``user_agents`` enables multi-user mode:
    ``{username: {"password": str, "can_publish": bool, "agent": AgentLoop, "bus": MessageBus}}``
    ``board_dir`` is the shared team board (published/) directory; defaults to
    the primary agent's workspace/published (single-user).
    """

    manager = ConnectionManager()
    _is_multi_user = user_agents is not None
    # token → username; empty string means "single-user / no username"
    _tokens: dict[str, str] = {}
    # Shared team board (published reports); all users read the same one.
    _board_dir = (board_dir or (agent.context.workspace / "published")).resolve()

    # Read-only share links: share_id → raw value.
    # Single-user: raw = session_id.
    # Multi-user:  raw = f"{username}\x00{session_id}".
    # Held in memory for O(1) concurrent reads; persisted to disk for restarts.
    _shares_path = agent.context.workspace.resolve() / ".perrobot_shares.json"
    _shares_lock = asyncio.Lock()
    try:
        _shares: dict[str, str] = json.loads(_shares_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        _shares = {}

    async def _persist_shares() -> bool:
        """Write in-memory shares to disk. Returns False on failure."""
        try:
            _shares_path.write_text(json.dumps(_shares), encoding="utf-8")
            return True
        except OSError as e:
            logger.warning("Failed to persist share links: {}", e)
            return False

    def _session_for_share(share_id: str) -> tuple[AgentLoop, str] | None:
        """Return (agent, session_id) for a share, or None if not found."""
        raw = _shares.get(share_id)
        if raw is None:
            return None
        if "\x00" in raw:
            username, session_id = raw.split("\x00", 1)
            if _is_multi_user and username in user_agents:
                return user_agents[username]["agent"], session_id
            return agent, session_id
        return agent, raw

    def _check_token(request: Request) -> None:
        """Raise 401 if auth is required and request carries no valid token."""
        if not password and not _is_multi_user:
            return
        token = request.headers.get("X-Token") or request.query_params.get("token")
        if not token or token not in _tokens:
            raise HTTPException(status_code=401, detail="Unauthorized")

    def _get_agent(request: Request) -> AgentLoop:
        """Verify auth and return the appropriate agent for this request.

        In single-user mode: performs the existing token check and returns ``agent``.
        In multi-user mode: resolves the token to a username and returns that user's agent.
        """
        token = request.headers.get("X-Token") or request.query_params.get("token")
        if _is_multi_user:
            username = _tokens.get(token or "")
            if not username:
                raise HTTPException(status_code=401, detail="Unauthorized")
            return user_agents[username]["agent"]
        _check_token(request)
        return agent

    def _username_of(request: Request) -> str:
        token = request.headers.get("X-Token") or request.query_params.get("token") or ""
        return _tokens.get(token, "")

    def _can_publish(request: Request) -> bool:
        """Single-user can always publish; multi-user gates on the config flag."""
        if not _is_multi_user:
            return True
        return bool(user_agents.get(_username_of(request), {}).get("can_publish", False))

    def _is_admin(request: Request) -> bool:
        """Admin is a multi-user concept: the config's admin flag."""
        if not _is_multi_user:
            return False
        return bool(user_agents.get(_username_of(request), {}).get("admin", False))

    def _persist_user_field(username: str, field: str, value) -> bool:
        """Write a single user field back to the users config file (best-effort)."""
        if not users_config_path:
            return False
        try:
            import yaml  # type: ignore[import]
            data = yaml.safe_load(users_config_path.read_text(encoding="utf-8")) or {}
            if username in data and isinstance(data[username], dict):
                data[username][field] = value
                users_config_path.write_text(
                    yaml.safe_dump(data, allow_unicode=True, sort_keys=False),
                    encoding="utf-8",
                )
                return True
        except Exception as e:
            logger.warning("Failed to persist user field {}={}: {}", field, value, e)
        return False

    def _resolve_path(path: str, ag: AgentLoop | None = None) -> Path:
        """Resolve a path according to allow_host_paths setting.

        Returns the resolved absolute Path.
        Raises HTTPException 403 if path escapes workspace when not allow_host_paths.
        """
        workspace = (ag or agent).context.workspace.resolve()
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

    def _resolve_skill_dir(name: str, ag: AgentLoop | None = None) -> tuple[Path, str] | None:
        """Resolve a skill's directory. A workspace copy shadows the builtin.

        Returns (directory, source) where source is "workspace" or "builtin",
        or None if no skill with that name exists.
        """
        loader = (ag or agent).context.skills
        ws_dir = loader.workspace_skills / name
        if (ws_dir / "SKILL.md").exists():
            return ws_dir, "workspace"
        bi_dir = loader.builtin_skills / name
        if (bi_dir / "SKILL.md").exists():
            return bi_dir, "builtin"
        return None

    def _resolve_skill_file(base: Path, rel: str) -> Path:
        """Safely join a relative path inside a skill directory.

        Raises HTTPException on traversal attempts or paths that escape ``base``.
        """
        if not rel or rel.startswith("/") or ".." in Path(rel).parts:
            raise HTTPException(status_code=400, detail="Invalid file path")
        base_resolved = base.resolve()
        target = (base_resolved / rel).resolve()
        if target != base_resolved and base_resolved not in target.parents:
            raise HTTPException(status_code=403, detail="Path outside skill directory")
        return target

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        # Start one outbound dispatcher per bus (default + each user in multi-user mode).
        buses: list[tuple[MessageBus, str]] = [(bus, "")]
        if _is_multi_user:
            for uname, ua in user_agents.items():
                buses.append((ua["bus"], uname))
        tasks = [
            asyncio.create_task(_dispatch_outbound(b, prefix))
            for b, prefix in buses
        ]
        logger.info("Outbound dispatcher started ({} bus(es))", len(tasks))
        yield
        for t in tasks:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

    app = FastAPI(title="nanobot", version="1.0", lifespan=lifespan)

    async def _dispatch_outbound(b: MessageBus, prefix: str = "") -> None:
        """Forward bus outbound messages (e.g. cron alerts) to WebSocket clients."""
        while True:
            try:
                msg: OutboundMessage = await b.consume_outbound()
                if msg.metadata.get("_progress"):
                    continue
                # In multi-user mode the WS key is "{username}:web:{chat_id}".
                if prefix:
                    key = f"{prefix}:{msg.channel}:{msg.chat_id}"
                else:
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

    @app.get("/vendor/{filename}")
    async def serve_vendor(filename: str):
        """Serve locally vendored frontend libraries (no token required)."""
        vendor_dir = (ui_path / "vendor").resolve()
        target = (vendor_dir / filename).resolve()
        try:
            target.relative_to(vendor_dir)
        except ValueError:
            raise HTTPException(status_code=403, detail="Forbidden")
        if target.is_file():
            return FileResponse(target)
        raise HTTPException(status_code=404, detail="Not found")

    # ------------------------------------------------------------------ Auth
    @app.post("/v1/auth")
    async def authenticate(request: Request):
        """Validate credentials and return a session token."""
        if not password and not _is_multi_user:
            # No auth configured — return a dummy token
            return {"token": "no-auth", "required": False}
        body = await request.json()
        if _is_multi_user:
            username = body.get("username", "")
            pw = body.get("password", "")
            if username in user_agents and user_agents[username].get("password") == pw:
                token = secrets.token_hex(32)
                _tokens[token] = username
                return {"token": token, "required": True, "username": username}
            raise HTTPException(status_code=403, detail="无效的用户名或密码")
        else:
            if body.get("password") == password:
                token = secrets.token_hex(32)
                _tokens[token] = ""
                return {"token": token, "required": True}
            raise HTTPException(status_code=403, detail="Invalid password")

    @app.get("/v1/auth/required")
    async def auth_required():
        return {"required": bool(password or _is_multi_user), "multi_user": _is_multi_user}

    @app.get("/v1/me")
    async def whoami(request: Request):
        """Return the current user's identity + capabilities (for UI gating)."""
        _get_agent(request)  # verifies auth
        return {
            "username": _username_of(request),
            "can_publish": _can_publish(request),
            "multi_user": _is_multi_user,
            "admin": _is_admin(request),
        }

    # ------------------------------------------------------------------ Admin
    @app.get("/v1/admin/users")
    async def admin_list_users(request: Request):
        """List users and their permissions (admin only, no secrets)."""
        _get_agent(request)  # verify auth
        if not _is_admin(request):
            raise HTTPException(status_code=403, detail="需要管理员权限")
        users = []
        for uname, ua in (user_agents or {}).items():
            try:
                ws = str(ua["agent"].context.workspace)
            except Exception:
                ws = ""
            users.append({
                "username": uname,
                "can_publish": bool(ua.get("can_publish")),
                "admin": bool(ua.get("admin")),
                "workspace": ws,
            })
        users.sort(key=lambda x: x["username"])
        return {"users": users}

    @app.put("/v1/admin/users/{username}")
    async def admin_update_user(username: str, request: Request, body: AdminUserUpdate):
        """Update a user's permissions (admin only). Persists to the users file."""
        _get_agent(request)
        if not _is_admin(request):
            raise HTTPException(status_code=403, detail="需要管理员权限")
        if not user_agents or username not in user_agents:
            raise HTTPException(status_code=404, detail="User not found")
        if body.can_publish is not None:
            user_agents[username]["can_publish"] = bool(body.can_publish)
            _persist_user_field(username, "can_publish", bool(body.can_publish))
        return {"ok": True, "username": username,
                "can_publish": bool(user_agents[username].get("can_publish"))}

    # ------------------------------------------------------------------ REST
    @app.get("/v1/health")
    async def health():
        return {"status": "ok"}

    @app.get("/v1/debug/system-prompt")
    async def debug_system_prompt(request: Request):
        ag = _get_agent(request)
        prompt = ag.context.build_system_prompt()
        skills = ag.context.skills.list_skills(filter_unavailable=False)
        always = ag.context.skills.get_always_skills()
        return {
            "workspace": str(ag.context.workspace),
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
        ag = _get_agent(request)
        return {"sessions": ag.sessions.list_sessions()}

    @app.delete("/v1/sessions/{session_id:path}")
    async def clear_session(session_id: str, request: Request):
        ag = _get_agent(request)
        session = ag.sessions.get_or_create(session_id)
        session.clear()
        ag.sessions.save(session)
        ag.sessions.invalidate(session_id)
        # Also remove the JSONL file so it disappears from the list
        path = ag.sessions._get_session_path(session_id)
        if path.exists():
            path.unlink()
        return {"ok": True, "session_id": session_id}

    @app.get("/v1/sessions/{session_id:path}/messages")
    async def get_session_messages(session_id: str, request: Request):
        ag = _get_agent(request)
        session = ag.sessions.get_or_create(session_id)
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
        ag = _get_agent(request)
        import tempfile

        session = ag.sessions.get_or_create(session_id)
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

    # ------------------------------------------------------------------ Sharing
    # Note: kept under /v1/share (not /v1/sessions/{id:path}/share) so the
    # greedy {session_id:path} on the clear-session route can't capture it.
    @app.post("/v1/share")
    async def create_share(request: Request, body: ShareRequest):
        """Create (or reuse) a read-only share link for a session."""
        ag = _get_agent(request)
        session_id = body.session_id
        # In multi-user mode encode username into the share value.
        username = _tokens.get(
            request.headers.get("X-Token") or request.query_params.get("token") or "", ""
        )
        share_val = f"{username}\x00{session_id}" if _is_multi_user else session_id
        async with _shares_lock:
            for sid, target in _shares.items():
                if target == share_val:
                    return {"share_id": sid, "url": f"/share/{sid}"}
            share_id = secrets.token_urlsafe(16)
            _shares[share_id] = share_val
            if not await _persist_shares():
                _shares.pop(share_id, None)
                raise HTTPException(status_code=500, detail="Failed to persist share link")
        return {"share_id": share_id, "url": f"/share/{share_id}"}

    @app.delete("/v1/share/{share_id}")
    async def revoke_share(share_id: str, request: Request):
        """Revoke a share link by its id."""
        _check_token(request)
        async with _shares_lock:
            existed = share_id in _shares
            _shares.pop(share_id, None)
            await _persist_shares()
        return {"ok": True, "revoked": existed}

    @app.get("/share/{share_id}")
    async def serve_share_page(share_id: str):
        """Serve the read-only share viewer page (no auth)."""
        html = ui_path / "share.html"
        if html.exists():
            return FileResponse(html)
        return JSONResponse({"error": "Share viewer not found"}, status_code=404)

    @app.get("/v1/share/{share_id}/data")
    async def share_data(share_id: str):
        """Return a shared session's messages (no auth — share_id is the secret)."""
        result = _session_for_share(share_id)
        if not result:
            raise HTTPException(status_code=404, detail="Share not found or revoked")
        ag, session_id = result
        session = ag.sessions.get_or_create(session_id)
        history = []
        for m in session.messages:
            role = m.get("role")
            content = m.get("content", "")
            if role in ("user", "assistant") and content and isinstance(content, str):
                history.append({"role": role, "content": content})
        title = session_id.split(":", 1)[-1] if ":" in session_id else session_id
        return {"title": title, "messages": history}

    # ------------------------------------------------------------------ Models
    @app.get("/v1/models")
    async def list_models(request: Request):
        """Return available models from config (no secrets)."""
        ag = _get_agent(request)
        try:
            from nanobot.providers.custom_provider import load_models_config
            cfg = load_models_config()
        except Exception:
            cfg = {}
        models_raw = cfg.get("models", {})
        current = getattr(ag.provider, "default_model", cfg.get("default_model", ""))
        models = [
            {"id": mid, "label": entry.get("label", mid)}
            for mid, entry in models_raw.items()
        ]
        return {"models": models, "current": current}

    @app.post("/v1/model")
    async def switch_model(request: Request, body: ModelSwitchRequest):
        """Hot-switch the LLM model on the running agent provider."""
        ag = _get_agent(request)
        provider = ag.provider
        if not hasattr(provider, "switch_model"):
            raise HTTPException(status_code=400, detail="Provider does not support model switching")
        provider.switch_model(body.model)
        return {"ok": True, "model": body.model}

    # ------------------------------------------------------------------ Skills
    @app.get("/v1/skills")
    async def list_skills_api(request: Request):
        """List all skills (builtin + workspace) with source and availability."""
        ag = _get_agent(request)
        loader = ag.context.skills
        always = set(loader.get_always_skills())
        out = []
        for s in loader.list_skills(filter_unavailable=False):
            name = s["name"]
            meta = loader._get_skill_meta(name)
            available = loader._check_requirements(meta)
            out.append({
                "name": name,
                "description": loader._get_skill_description(name),
                "source": s["source"],
                "available": available,
                "requires": "" if available else loader._get_missing_requirements(meta),
                "always": name in always,
            })
        out.sort(key=lambda x: (x["source"] != "workspace", x["name"].lower()))
        return {"skills": out}

    @app.get("/v1/skills/{name}")
    async def get_skill_api(name: str, request: Request):
        """Return a skill's raw SKILL.md content and whether it is editable."""
        ag = _get_agent(request)
        if not _SKILL_NAME_RE.match(name):
            raise HTTPException(status_code=400, detail="Invalid skill name")
        loader = ag.context.skills
        content = loader.load_skill(name)
        if content is None:
            raise HTTPException(status_code=404, detail="Skill not found")
        # A workspace copy (if present) shadows the builtin and is editable.
        ws_md = loader.workspace_skills / name / "SKILL.md"
        source = "workspace" if ws_md.exists() else "builtin"
        return {"name": name, "content": content, "source": source,
                "editable": source == "workspace"}

    @app.put("/v1/skills/{name}")
    async def save_skill_api(name: str, request: Request, body: SkillSaveRequest):
        """Create or update a workspace skill (writes workspace/skills/<name>/SKILL.md)."""
        ag = _get_agent(request)
        if not _SKILL_NAME_RE.match(name):
            raise HTTPException(status_code=400, detail="Invalid skill name")
        loader = ag.context.skills
        skill_dir = loader.workspace_skills / name
        skill_dir.mkdir(parents=True, exist_ok=True)
        (skill_dir / "SKILL.md").write_text(body.content, encoding="utf-8")
        return {"ok": True, "name": name, "source": "workspace"}

    @app.delete("/v1/skills/{name}")
    async def delete_skill_api(name: str, request: Request):
        """Delete a workspace skill. Builtin skills cannot be deleted."""
        ag = _get_agent(request)
        if not _SKILL_NAME_RE.match(name):
            raise HTTPException(status_code=400, detail="Invalid skill name")
        loader = ag.context.skills
        skill_dir = loader.workspace_skills / name
        if not (skill_dir / "SKILL.md").exists():
            raise HTTPException(status_code=404, detail="No workspace skill to delete")
        shutil.rmtree(skill_dir)
        return {"ok": True, "name": name}

    @app.get("/v1/skills/{name}/files")
    async def list_skill_files_api(name: str, request: Request):
        """List every file inside a skill package (recursive), not just SKILL.md."""
        ag = _get_agent(request)
        if not _SKILL_NAME_RE.match(name):
            raise HTTPException(status_code=400, detail="Invalid skill name")
        resolved = _resolve_skill_dir(name, ag)
        if resolved is None:
            raise HTTPException(status_code=404, detail="Skill not found")
        base, source = resolved
        files = []
        for p in sorted(base.rglob("*")):
            if p.is_file():
                try:
                    size = p.stat().st_size
                except OSError:
                    size = 0
                files.append({"path": p.relative_to(base).as_posix(), "size": size})
        return {"name": name, "source": source,
                "editable": source == "workspace", "files": files}

    @app.get("/v1/skills/{name}/file")
    async def read_skill_file_api(name: str, request: Request, path: str = Query(...)):
        """Return the content of one file within a skill package."""
        ag = _get_agent(request)
        if not _SKILL_NAME_RE.match(name):
            raise HTTPException(status_code=400, detail="Invalid skill name")
        resolved = _resolve_skill_dir(name, ag)
        if resolved is None:
            raise HTTPException(status_code=404, detail="Skill not found")
        base, source = resolved
        target = _resolve_skill_file(base, path)
        if not target.is_file():
            raise HTTPException(status_code=404, detail="File not found")
        raw = target.read_bytes()
        try:
            content = raw.decode("utf-8")
            binary = False
        except UnicodeDecodeError:
            content, binary = "", True
        return {"name": name, "path": path, "content": content, "binary": binary,
                "source": source, "editable": source == "workspace"}

    @app.put("/v1/skills/{name}/file")
    async def write_skill_file_api(name: str, request: Request, body: SkillFileSaveRequest):
        """Create or update one file inside a workspace skill package."""
        ag = _get_agent(request)
        if not _SKILL_NAME_RE.match(name):
            raise HTTPException(status_code=400, detail="Invalid skill name")
        loader = ag.context.skills
        ws_dir = loader.workspace_skills / name
        target = _resolve_skill_file(ws_dir, body.path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body.content, encoding="utf-8")
        return {"ok": True, "name": name, "path": body.path, "source": "workspace"}

    @app.post("/v1/skills/{name}/copy")
    async def copy_skill_api(name: str, request: Request):
        """Copy a builtin skill package (all files) into the workspace for editing."""
        ag = _get_agent(request)
        if not _SKILL_NAME_RE.match(name):
            raise HTTPException(status_code=400, detail="Invalid skill name")
        loader = ag.context.skills
        ws_dir = loader.workspace_skills / name
        if (ws_dir / "SKILL.md").exists():
            return {"ok": True, "name": name, "source": "workspace"}
        bi_dir = loader.builtin_skills / name
        if not (bi_dir / "SKILL.md").exists():
            raise HTTPException(status_code=404, detail="Skill not found")
        ws_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(bi_dir, ws_dir)
        return {"ok": True, "name": name, "source": "workspace"}

    # ------------------------------------------------------------------ Files
    @app.get("/v1/files")
    async def list_files(request: Request, path: str = Query("")):
        """Browse the workspace directory tree (or host if allow_host_paths)."""
        ag = _get_agent(request)
        workspace = ag.context.workspace.resolve()
        target = _resolve_path(path, ag)

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
        ag = _get_agent(request)
        target = _resolve_path(body.path, ag)

        if target.is_dir():
            raise HTTPException(status_code=400, detail="Path is a directory")

        target.parent.mkdir(parents=True, exist_ok=True)
        encoded = body.content.encode("utf-8")
        target.write_bytes(encoded)
        return {"ok": True, "bytes": len(encoded)}

    @app.post("/v1/files/upload")
    async def upload_file(
        request: Request,
        file: UploadFile = File(...),
        path: str = Form(""),
    ):
        """Upload a (binary) file into the workspace.

        `path` is the target *directory* (relative to workspace, or absolute when
        allow_host_paths). The uploaded filename is appended to it. Returns the
        saved path so the UI can reference it in a chat message.
        """
        ag = _get_agent(request)

        # Sanitize the client-supplied filename — strip any directory components.
        raw_name = file.filename or "upload"
        name = Path(raw_name).name or "upload"

        target_dir = _resolve_path(path, ag) if path else ag.context.workspace.resolve()
        if target_dir.exists() and target_dir.is_file():
            raise HTTPException(status_code=400, detail="Target path is a file, not a directory")
        target_dir.mkdir(parents=True, exist_ok=True)

        target = (target_dir / name).resolve()
        # Re-validate the final path stays within the allowed subtree.
        workspace = ag.context.workspace.resolve()
        if not allow_host_paths:
            try:
                target.relative_to(workspace)
            except ValueError:
                raise HTTPException(status_code=403, detail="Path outside workspace")

        # Avoid clobbering an existing file: suffix with -1, -2, ...
        if target.exists():
            stem, suffix = target.stem, target.suffix
            i = 1
            while True:
                candidate = target.with_name(f"{stem}-{i}{suffix}")
                if not candidate.exists():
                    target = candidate
                    break
                i += 1

        size = 0
        with target.open("wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
                size += len(chunk)
        await file.close()

        try:
            rel = str(target.relative_to(workspace))
        except ValueError:
            rel = str(target)
        return {"ok": True, "path": rel, "name": target.name, "bytes": size}

    @app.get("/v1/files/download")
    async def download_file(request: Request, path: str = Query(...)):
        """Download a file as an attachment."""
        ag = _get_agent(request)
        target = _resolve_path(path, ag)

        if not target.exists():
            raise HTTPException(status_code=404, detail="File not found")
        if target.is_dir():
            raise HTTPException(status_code=400, detail="Path is a directory")

        return FileResponse(
            target,
            filename=target.name,
            headers={"Content-Disposition": f'attachment; filename="{target.name}"'},
        )

    # ------------------------------------------------------------------ Reports
    # Reports are just markdown files under the workspace:
    #   reports/    → personal drafts
    #   published/  → published to the team board
    # An optional sibling data file (<stem>.data.* / <stem>.snapshot.*) is the
    # snapshot that powers cheap follow-up questions without re-querying.
    _REPORT_EXTS = (".md", ".markdown")

    def _is_snapshot_sibling(name: str) -> bool:
        n = name.lower()
        return ".data." in n or ".snapshot." in n

    def _ensure_author(text: str, username: str) -> str:
        """Ensure the report frontmatter records who published it."""
        if not username:
            return text
        if text.startswith("---"):
            end = text.find("\n---", 3)
            if end != -1:
                if "author:" not in text[3:end]:
                    return text[:end] + f"\nauthor: {username}" + text[end:]
                return text
        return f"---\nauthor: {username}\n---\n\n{text}"

    def _parse_report_meta(path: Path) -> dict:
        """Best-effort read of a report's frontmatter + snapshot sibling."""
        title = path.stem
        author = ""
        series = ""
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            text = ""
        if text.startswith("---"):
            end = text.find("\n---", 3)
            if end != -1:
                for line in text[3:end].splitlines():
                    if ":" in line:
                        k, _, v = line.partition(":")
                        k = k.strip().lower()
                        v = v.strip().strip('"').strip("'")
                        if k == "title" and v:
                            title = v
                        elif k == "author":
                            author = v
                        elif k == "series":
                            series = v
        if title == path.stem:
            for line in text.splitlines():
                if line.startswith("# "):
                    title = line[2:].strip()
                    break
        snapshot = None
        try:
            for sib in path.parent.glob(path.stem + ".*"):
                if sib != path and _is_snapshot_sibling(sib.name):
                    snapshot = sib.name
                    break
        except OSError:
            pass
        return {"title": title, "author": author, "series": series, "snapshot": snapshot}

    def _report_target(ag: AgentLoop, logical_path: str) -> tuple[Path, Path]:
        """Map a logical report path to (target, base).

        'published/<name>' → shared board_dir; anything else → the user's
        workspace (drafts under reports/).
        """
        if logical_path.startswith("published/"):
            base = _board_dir
            target = (_board_dir / logical_path[len("published/"):]).resolve()
        else:
            base = ag.context.workspace.resolve()
            target = (base / logical_path).resolve()
        try:
            target.relative_to(base)
        except ValueError:
            raise HTTPException(status_code=403, detail="Path outside allowed area")
        return target, base

    @app.get("/v1/reports")
    async def list_reports(request: Request):
        """List drafts (user's reports/) and the shared team board (published/)."""
        ag = _get_agent(request)
        ws = ag.context.workspace.resolve()
        out = []
        sources = [("draft", ws / "reports", ws), ("published", _board_dir, _board_dir)]
        for status, d, base in sources:
            if not d.is_dir():
                continue
            for p in sorted(d.rglob("*")):
                if not (p.is_file() and p.suffix.lower() in _REPORT_EXTS):
                    continue
                meta = _parse_report_meta(p)
                if status == "published":
                    logical = "published/" + p.relative_to(_board_dir).as_posix()
                else:
                    try:
                        logical = str(p.relative_to(ws))
                    except ValueError:
                        logical = str(p)
                try:
                    mtime = p.stat().st_mtime
                except OSError:
                    mtime = 0
                out.append({
                    "path": logical, "name": p.name, "status": status,
                    "title": meta["title"], "author": meta["author"],
                    "series": meta["series"], "snapshot": meta["snapshot"],
                    "updated_at": mtime,
                })
        out.sort(key=lambda x: (x["status"] != "draft", -x["updated_at"]))
        return {"reports": out}

    @app.get("/v1/reports/content")
    async def report_content(request: Request, path: str = Query(...)):
        """Return a report's markdown by logical path (draft or shared board)."""
        ag = _get_agent(request)
        target, _ = _report_target(ag, path)
        if not target.is_file():
            raise HTTPException(status_code=404, detail="Report not found")
        content = target.read_text(encoding="utf-8", errors="replace")
        return {"path": path, "name": target.name, "content": content,
                "editable": not path.startswith("published/")}

    async def _write_upload(dst: Path, up: UploadFile) -> int:
        size = 0
        with dst.open("wb") as out:
            while True:
                chunk = await up.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
                size += len(chunk)
        await up.close()
        return size

    @app.post("/v1/reports/upload")
    async def upload_report(
        request: Request,
        report: UploadFile = File(...),
        snapshot: UploadFile | None = File(None),
    ):
        """Upload an offline report (+ optional snapshot) into reports/ as a draft.

        The snapshot is saved as ``<report-stem>.data.<ext>`` so it's detected as
        the report's sibling and can power reference-and-ask follow-ups.
        """
        ag = _get_agent(request)
        rdir = ag.context.workspace.resolve() / "reports"
        rdir.mkdir(parents=True, exist_ok=True)

        rname = Path(report.filename or "report.md").name or "report.md"
        rtarget = (rdir / rname)
        if rtarget.exists():
            i = 1
            while (rdir / f"{rtarget.stem}-{i}{rtarget.suffix}").exists():
                i += 1
            rtarget = rdir / f"{rtarget.stem}-{i}{rtarget.suffix}"
        await _write_upload(rtarget, report)

        snap_name = None
        if snapshot is not None and snapshot.filename:
            sext = Path(snapshot.filename).suffix or ".json"
            snap_target = rdir / f"{rtarget.stem}.data{sext}"
            await _write_upload(snap_target, snapshot)
            snap_name = snap_target.name

        return {"ok": True, "path": "reports/" + rtarget.name,
                "name": rtarget.name, "snapshot": snap_name}

    @app.post("/v1/reports/publish")
    async def publish_report(request: Request, body: ReportPublishRequest):
        """Publish a draft to the shared team board (requires publish permission)."""
        ag = _get_agent(request)
        if not _can_publish(request):
            raise HTTPException(status_code=403, detail="无发布权限")
        src, _ = _report_target(ag, body.path)
        if not src.is_file() or src.suffix.lower() not in _REPORT_EXTS:
            raise HTTPException(status_code=404, detail="Report not found")
        _board_dir.mkdir(parents=True, exist_ok=True)
        username = _username_of(request)
        # Stamp the publishing author into the report's frontmatter.
        text = src.read_text(encoding="utf-8", errors="replace")
        (_board_dir / src.name).write_text(_ensure_author(text, username), encoding="utf-8")
        # Carry the snapshot sibling(s) so published reports stay interrogable.
        try:
            for sib in src.parent.glob(src.stem + ".*"):
                if sib != src and _is_snapshot_sibling(sib.name):
                    shutil.copy2(sib, _board_dir / sib.name)
        except OSError:
            pass
        return {"ok": True, "path": "published/" + src.name, "author": username}

    # ------------------------------------------------------------------ Cron
    def _cron_to_dict(j) -> dict:
        return {
            "id": j.id, "name": j.name, "enabled": j.enabled,
            "schedule": {
                "kind": j.schedule.kind, "expr": j.schedule.expr,
                "every_ms": j.schedule.every_ms, "at_ms": j.schedule.at_ms,
                "tz": j.schedule.tz,
            },
            "message": j.payload.message, "deliver": j.payload.deliver,
            "last_status": j.state.last_status, "last_error": j.state.last_error,
            "last_run_at_ms": j.state.last_run_at_ms,
            "next_run_at_ms": j.state.next_run_at_ms,
        }

    @app.get("/v1/cron")
    async def list_cron(request: Request):
        ag = _get_agent(request)
        cron = getattr(ag, "cron_service", None)
        if not cron:
            return {"jobs": []}
        return {"jobs": [_cron_to_dict(j) for j in cron.list_jobs(include_disabled=True)]}

    @app.post("/v1/cron")
    async def create_cron(request: Request, body: CronCreateRequest):
        ag = _get_agent(request)
        cron = getattr(ag, "cron_service", None)
        if not cron:
            raise HTTPException(status_code=503, detail="Cron service not available")
        if body.schedule_kind == "every":
            sched = CronSchedule(kind="every", every_ms=body.every_ms)
        else:
            sched = CronSchedule(kind="cron", expr=body.expr, tz=body.tz)
        try:
            job = cron.add_job(
                name=body.name, schedule=sched, message=body.message,
                deliver=body.deliver, channel="web", to="default",
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return {"ok": True, "job": _cron_to_dict(job)}

    @app.delete("/v1/cron/{job_id}")
    async def delete_cron(job_id: str, request: Request):
        ag = _get_agent(request)
        cron = getattr(ag, "cron_service", None)
        if not cron:
            raise HTTPException(status_code=503, detail="Cron service not available")
        return {"ok": cron.remove_job(job_id)}

    @app.post("/v1/cron/{job_id}/enable")
    async def enable_cron(job_id: str, request: Request, body: CronEnableRequest):
        ag = _get_agent(request)
        cron = getattr(ag, "cron_service", None)
        if not cron:
            raise HTTPException(status_code=503, detail="Cron service not available")
        job = cron.enable_job(job_id, body.enabled)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return {"ok": True, "job": _cron_to_dict(job)}

    @app.post("/v1/cron/{job_id}/run")
    async def run_cron(job_id: str, request: Request):
        ag = _get_agent(request)
        cron = getattr(ag, "cron_service", None)
        if not cron:
            raise HTTPException(status_code=503, detail="Cron service not available")
        ok = await cron.run_job(job_id, force=True)
        if not ok:
            raise HTTPException(status_code=404, detail="Job not found")
        return {"ok": True}

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
        if _is_multi_user:
            username = _tokens.get(token)
            if not username:
                await ws.close(code=4001)
                return
            ag = user_agents[username]["agent"]
            # Prefix the WS key with username to isolate users' push notifications.
            key = f"{username}:web:{session_id}"
        elif password and token not in _tokens:
            await ws.close(code=4001)
            return
        else:
            ag = agent
            key = f"web:{session_id}"

        chat_id = session_id
        session_key = f"web:{session_id}"
        await manager.connect(key, ws)

        # Pending human-approval requests, keyed by id. Resolved when the client
        # sends back an {type: approval_response, id, approved} message.
        pending_approvals: dict[str, asyncio.Future] = {}

        async def on_progress(text: str, *, tool_hint: bool = False, skill_hint: bool = False, file_hint: bool = False) -> None:
            if file_hint:
                msg_type = "file_written"
            elif skill_hint:
                msg_type = "skill"
            elif tool_hint:
                msg_type = "tool"
            else:
                msg_type = "progress"
            try:
                await ws.send_json({"type": msg_type, "content": text})
            except Exception:
                pass  # client may have disconnected mid-turn; keep the loop alive

        async def on_token(kind: str, text: str) -> None:
            try:
                if kind == "delta":
                    await ws.send_json({"type": "token", "content": text})
                elif kind == "cancel":
                    await ws.send_json({"type": "token_cancel"})
            except Exception:
                pass  # client disconnected mid-stream; agent loop continues to completion

        async def on_approval(command: str, reason: str) -> bool:
            """Ask the client to approve a dangerous command; block until answered."""
            approval_id = secrets.token_hex(8)
            loop = asyncio.get_event_loop()
            fut: asyncio.Future = loop.create_future()
            pending_approvals[approval_id] = fut
            try:
                await ws.send_json({
                    "type": "approval_request",
                    "id": approval_id,
                    "command": command,
                    "reason": reason,
                })
            except Exception:
                pending_approvals.pop(approval_id, None)
                return False
            try:
                # Generous window; if the user walks away, deny rather than hang.
                return await asyncio.wait_for(fut, timeout=300)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                return False
            finally:
                pending_approvals.pop(approval_id, None)

        async def handle_message(content: str) -> None:
            msg = InboundMessage(
                channel="web", sender_id="web_user", chat_id=chat_id, content=content,
            )
            try:
                response = await ag._process_message(
                    msg, session_key=session_key, on_progress=on_progress,
                    on_token=on_token, on_approval=on_approval,
                )
                if response is not None:
                    await ws.send_json({"type": "message", "content": response.content or ""})
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Message processing error for {}: {}", key, e)
                # Unstick the client (it only clears "busy" on a message/push).
                try:
                    await ws.send_json({"type": "message", "content": f"⚠️ 处理出错：{e}"})
                except Exception:
                    pass

        current_task: asyncio.Task | None = None
        try:
            while True:
                data = await ws.receive_json()

                # Approval replies must be handled even while a turn is running,
                # so processing runs as a background task and the receive loop
                # keeps reading. Demux approval responses here.
                if data.get("type") == "approval_response":
                    fut = pending_approvals.get(data.get("id"))
                    if fut is not None and not fut.done():
                        fut.set_result(bool(data.get("approved")))
                    continue

                content = (data.get("content") or "").strip()
                if not content:
                    continue
                # One in-flight turn per connection (UI gates send while busy).
                if current_task is not None and not current_task.done():
                    await ws.send_json({"type": "progress", "content": "上一条还在处理中，请稍候…"})
                    continue
                current_task = asyncio.create_task(handle_message(content))
        except WebSocketDisconnect:
            pass
        except Exception as e:
            logger.error("WebSocket error for {}: {}", key, e)
        finally:
            # Release any pending approval as "denied" so it doesn't hang, then
            # cancel the in-flight turn — the client is gone, nothing to deliver.
            for fut in pending_approvals.values():
                if not fut.done():
                    fut.set_result(False)
            if current_task is not None and not current_task.done():
                current_task.cancel()
            manager.disconnect(key, ws)

    return app
