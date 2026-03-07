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


class _ConnectionManager:
    """Track active WebSocket connections by session key."""

    def __init__(self) -> None:
        self._connections: dict[str, list[WebSocket]] = {}

    async def connect(self, key: str, ws: WebSocket) -> None:
        await ws.accept()
        self._connections.setdefault(key, []).append(ws)

    def disconnect(self, key: str, ws: WebSocket) -> None:
        conns = self._connections.get(key, [])
        if ws in conns:
            conns.remove(ws)
        if not conns:
            self._connections.pop(key, None)

    async def send(self, key: str, data: dict) -> None:
        for ws in list(self._connections.get(key, [])):
            try:
                await ws.send_json(data)
            except Exception:
                pass


def create_app(agent: AgentLoop, bus: MessageBus, ui_path: Path) -> FastAPI:
    """Build and return the FastAPI application."""

    manager = _ConnectionManager()

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        task = asyncio.create_task(_dispatch_outbound())
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
                # Direct await — no wait_for; Python 3.11 wait_for+Queue.get() can
                # silently drop messages at timeout boundaries (fixed in 3.12).
                msg: OutboundMessage = await bus.consume_outbound()
                # Progress messages are sent inline via the WS on_progress callback;
                # filter them out here to avoid duplicates.
                if msg.metadata.get("_progress"):
                    continue
                key = f"{msg.channel}:{msg.chat_id}"
                logger.debug("Push → {}: {}", key, (msg.content or "")[:80])
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
                # in that case the content is already in bus.outbound and
                # _dispatch_outbound will push it; don't send an empty frame.
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
