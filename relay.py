import asyncio
import re
from typing import Dict, Optional

from fastapi import FastAPI, WebSocket

app = FastAPI(title="Remote Screen Relay")

ROOM_RE = re.compile(r"^[A-Z0-9]{4,16}$")
rooms: Dict[str, Dict[str, object]] = {}


@app.get("/")
@app.get("/health")
async def health():
    return {"status": "ok", "active_rooms": len(rooms)}


async def _forward_worker(room_code: str, target_role: str):
    """Inoltra prima i pacchetti di controllo/audio e poi l'ultimo frame video disponibile."""
    ev_key = f"{target_role}_ev"
    ctrl_key = f"{target_role}_ctrl_q"
    vid_key = f"{target_role}_vid"

    while True:
        entry = rooms.get(room_code)
        if not entry:
            break
        event: asyncio.Event = entry[ev_key]
        await event.wait()
        event.clear()

        while True:
            entry = rooms.get(room_code)
            if not entry:
                return
            target_ws: Optional[WebSocket] = entry.get(target_role)
            if target_ws is None:
                break

            ctrl_q: list = entry[ctrl_key]
            if ctrl_q:
                pkt = ctrl_q.pop(0)
                try:
                    await target_ws.send_bytes(pkt)
                except Exception:
                    break
                continue

            vid_pkt = entry[vid_key]
            if vid_pkt is not None:
                entry[vid_key] = None
                try:
                    await target_ws.send_bytes(vid_pkt)
                except Exception:
                    break
                continue

            break


@app.websocket("/ws/{role}/{room}")
async def ws_relay(ws: WebSocket, role: str, room: str):
    await ws.accept()
    room_code = room.strip().upper()

    if role not in ("guest", "controller") or not ROOM_RE.match(room_code):
        await ws.send_text("ERR:INVALID_PARAMS")
        await ws.close(code=4000)
        return

    if role == "guest":
        existing = rooms.get(room_code)
        if existing and existing.get("guest") is not None:
            await ws.send_text("ERR:ROOM_IN_USE")
            await ws.close(code=4001)
            return

        rooms[room_code] = {
            "guest": ws,
            "controller": None,
            "guest_ctrl_q": [],
            "controller_ctrl_q": [],
            "guest_vid": None,
            "controller_vid": None,
            "guest_ev": asyncio.Event(),
            "controller_ev": asyncio.Event(),
        }
        await ws.send_text("READY")

        fwd_task = asyncio.create_task(_forward_worker(room_code, "guest"))
        try:
            while True:
                msg = await ws.receive()
                if msg.get("type") == "websocket.disconnect":
                    break

                data = msg.get("bytes")
                if data is not None:
                    entry = rooms.get(room_code)
                    if entry and entry.get("controller") is not None and len(data) > 0:
                        msg_type = data[0]
                        if msg_type == 5:  # MSG_FRAME: sovrascrive sempre con l'ultimo frame
                            entry["controller_vid"] = data
                        else:
                            cq: list = entry["controller_ctrl_q"]
                            if len(cq) < 32:
                                cq.append(data)
                        entry["controller_ev"].set()
                    continue

                txt = msg.get("text")
                if txt is not None:
                    if txt == "PING":
                        await ws.send_text("PONG")
                    elif txt == "KICK":
                        ctrl = rooms.get(room_code, {}).get("controller")
                        if ctrl is not None:
                            try:
                                await ctrl.close(code=4000)
                            except Exception:
                                pass
        except Exception:
            pass
        finally:
            fwd_task.cancel()
            entry = rooms.get(room_code)
            if entry and entry.get("guest") is ws:
                ctrl = entry.get("controller")
                rooms.pop(room_code, None)
                if ctrl is not None:
                    try:
                        await ctrl.send_text("PEER_LEFT")
                    except Exception:
                        pass
                    try:
                        await ctrl.close(code=1000)
                    except Exception:
                        pass
        return

    # role == "controller"
    entry = rooms.get(room_code)
    if not entry or entry.get("guest") is None:
        await ws.send_text("ERR:ROOM_NOT_FOUND")
        await ws.close(code=4004)
        return

    if entry.get("controller") is not None:
        await ws.send_text("ERR:ROOM_BUSY")
        await ws.close(code=4009)
        return

    entry["controller"] = ws
    entry["controller_ctrl_q"].clear()
    entry["controller_vid"] = None
    guest_ws: WebSocket = entry["guest"]
    try:
        await guest_ws.send_text("PEER_JOINED")
        await ws.send_text("CONNECTED")
    except Exception:
        entry["controller"] = None
        try:
            await ws.close(code=1011)
        except Exception:
            pass
        return

    fwd_task = asyncio.create_task(_forward_worker(room_code, "controller"))
    try:
        while True:
            msg = await ws.receive()
            if msg.get("type") == "websocket.disconnect":
                break

            data = msg.get("bytes")
            if data is not None:
                entry = rooms.get(room_code)
                if entry and entry.get("guest") is not None and len(data) > 0:
                    if data[0] == 5:
                        entry["guest_vid"] = data
                    else:
                        gq: list = entry["guest_ctrl_q"]
                        if len(gq) < 32:
                            gq.append(data)
                    entry["guest_ev"].set()
                continue

            txt = msg.get("text")
            if txt == "PING":
                await ws.send_text("PONG")
    except Exception:
        pass
    finally:
        fwd_task.cancel()
        entry = rooms.get(room_code)
        if entry and entry.get("controller") is ws:
            entry["controller"] = None
            entry["controller_ctrl_q"].clear()
            entry["controller_vid"] = None
            g = entry.get("guest")
            if g is not None:
                try:
                    await g.send_text("PEER_LEFT")
                except Exception:
                    pass