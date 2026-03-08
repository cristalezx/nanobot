"""nanobot Web API server."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from loguru import logger

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


def create_app(agent: AgentLoop, bus: MessageBus, ui_path: Path) -> FastAPI:
    """Build and return the FastAPI application."""

    manager = ConnectionManager()

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
                logger.info("Dispatch push → {} ({} chars)", key, len(msg.content or ""))
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

    # ------------------------------------------------------------------ REST
    @app.get("/v1/health")
    async def health():
        return {"status": "ok"}

    @app.get("/v1/debug/system-prompt")
    async def debug_system_prompt():
        """Return the current system prompt and skills list (for debugging)."""
        prompt = agent.context.build_system_prompt()
        skills = agent.context.skills.list_skills(filter_unavailable=False)
        always = agent.context.skills.get_always_skills()
        return {
            "workspace": str(agent.context.workspace),
            "always_skills": always,
            "all_skills": skills,
            "system_prompt": prompt,
        }

    @app.get("/v1/sessions")
    async def list_sessions():
        return {"sessions": agent.sessions.list_sessions()}

    @app.delete("/v1/sessions/{session_id:path}")
    async def clear_session(session_id: str):
        session = agent.sessions.get_or_create(session_id)
        session.clear()
        agent.sessions.save(session)
        agent.sessions.invalidate(session_id)
        return {"ok": True, "session_id": session_id}

    @app.get("/v1/sessions/{session_id:path}/messages")
    async def get_session_messages(session_id: str):
        """Return user/assistant message pairs for chat history display."""
        session = agent.sessions.get_or_create(session_id)
        history = []
        for m in session.messages:
            role = m.get("role")
            content = m.get("content", "")
            if role in ("user", "assistant") and content:
                history.append({"role": role, "content": content})
        return {"messages": history}

    # ---- Diagnostic: push a test message to verify the WS push pipeline ----
    @app.post("/v1/test-push/{session_id}")
    async def test_push(session_id: str, content: str = "🔔 Test push notification"):
        """Bypass cron+LLM entirely; push directly to WebSocket clients."""
        key = f"web:{session_id}"
        keys = manager.connected_keys()
        logger.info("test-push → key={}, connected_keys={}", key, keys)
        await manager.send(key, {"type": "push", "content": content})
        return {"ok": True, "key": key, "connected_keys": keys}

    # ------------------------------------------------------------------ WebSocket
    @app.websocket("/v1/ws/{session_id:path}")
    async def ws_endpoint(ws: WebSocket, session_id: str):
        channel, chat_id = "web", session_id
        key = f"{channel}:{chat_id}"
        await manager.connect(key, ws)
        try:
            while True:
                data = await ws.receive_json()
                content = (data.get("content") or "").strip()
                if not content:
                    continue

                async def on_progress(text: str, *, tool_hint: bool = False) -> None:
                    await ws.send_json({
                        "type": "tool" if tool_hint else "progress",
                        "content": text,
                    })

                msg = InboundMessage(
                    channel=channel,
                    sender_id="web_user",
                    chat_id=chat_id,
                    content=content,
                )
                response = await agent._process_message(
                    msg, session_key=key, on_progress=on_progress
                )
                # response is None when the agent used the message tool —
                # the content is already in bus.outbound and _dispatch_outbound
                # will push it; don't send an empty frame.
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
