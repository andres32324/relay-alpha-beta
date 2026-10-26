import asyncio
import aiohttp
from aiohttp import web
import os
import time
import secrets

emitters = {}
listeners = {}
pending   = {}
watchers  = {}
last_info = {}
last_seen = {}


def log(code, msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}][{code}] {msg}", flush=True)


def gen_code():
    for _ in range(10000):
        code = f"{secrets.randbelow(10000):04d}"
        if code not in emitters and code not in listeners and code not in pending:
            return code
    raise RuntimeError("Sin codigos disponibles")


async def safe_str(ws, msg):
    try:
        if not ws.closed:
            await ws.send_str(msg)
            return True
    except Exception as e:
        log("safe_str", f"ERROR '{msg[:30]}': {e}")
    return False


async def safe_bytes(ws, data):
    try:
        if not ws.closed:
            await ws.send_bytes(data)
            return True
    except Exception as e:
        log("safe_bytes", f"ERROR bytes: {e}")
    return False


async def cleanup(code, role, ws):
    log(code, f"Limpiando {role}")
    if role == "BETA":
        if emitters.get(code) is ws:
            emitters.pop(code, None)
            last_seen.pop(code, None)
            alpha = listeners.get(code)
            if alpha is not None and not alpha.closed:
                await safe_str(alpha, "EMITTER_GONE")
            alpha_p = pending.get(code)
            if alpha_p is not None and not alpha_p.closed:
                await safe_str(alpha_p, "EMITTER_GONE")
    elif role == "ALPHA":
        was_l = listeners.get(code) is ws
        was_p = pending.get(code) is ws
        if was_l:
            listeners.pop(code, None)
        if was_p:
            pending.pop(code, None)
        if was_l and listeners.get(code) is None:
            beta = emitters.get(code)
            if beta is not None and not beta.closed:
                await safe_str(beta, "STOP")
    elif role == "WATCHER":
        group = watchers.get(code)
        if group is not None:
            group.discard(ws)
            if not group:
                watchers.pop(code, None)


async def self_ping(app):
    url = os.environ.get("RAILWAY_STATIC_URL", os.environ.get("RENDER_EXTERNAL_URL", ""))
    if not url:
        return
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    url = url.rstrip("/") + "/ping"
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            await asyncio.sleep(240)
            try:
                async with session.get(url) as r:
                    log("SYSTEM", f"Self-ping {r.status}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log("SYSTEM", f"Self-ping error: {e}")


async def handle_ping(request):
    return web.Response(text="Alpha-Beta Relay OK", status=200)


async def handle(request):
    ws = web.WebSocketResponse(heartbeat=15)
    await ws.prepare(request)
    code = None
    role = None

    try:
        try:
            msg = await asyncio.wait_for(ws.receive(), timeout=15)
        except asyncio.TimeoutError:
            await ws.close()
            return ws

        if msg.type != aiohttp.WSMsgType.TEXT:
            await ws.close()
            return ws

        text = msg.data.strip()
        log("???", f"Primer mensaje: [{text}]")

        # ── BETA emisor ───────────────────────────────────────────────────────
        if text == "EMIT" or text.startswith("EMIT:"):
            if text.startswith("EMIT:"):
                code = text[5:].strip()
                if not code.isdigit() or len(code) != 4:
                    try:
                        code = gen_code()
                    except RuntimeError:
                        await safe_str(ws, "ERROR:NO_CODES_AVAILABLE")
                        await ws.close()
                        return ws
            else:
                try:
                    code = gen_code()
                except RuntimeError:
                    await safe_str(ws, "ERROR:NO_CODES_AVAILABLE")
                    await ws.close()
                    return ws

            role = "BETA"
            log(code, "Beta conectado")

            old = emitters.get(code)
            if old is not None and not old.closed:
                try:
                    await old.close()
                except Exception:
                    pass

            emitters[code] = ws
            last_seen[code] = time.time()
            watchers.setdefault(code, set())

            await safe_str(ws, f"CODE:{code}")
            log(code, f"CODE:{code} enviado")

            # Alpha en espera (pending)
            alpha_p = pending.get(code)
            if alpha_p is not None and not alpha_p.closed:
                pending.pop(code, None)
                listeners[code] = alpha_p
                await safe_str(alpha_p, "OK")
                await safe_str(ws, "READY")
                if code in last_info:
                    await safe_str(alpha_p, last_info[code])
                log(code, "Alpha en espera → READY enviado a Beta")
            else:
                # Alpha ya conectado (listener)
                alpha = listeners.get(code)
                if alpha is not None and not alpha.closed:
                    await safe_str(alpha, "OK")
                    await safe_str(ws, "READY")
                    log(code, "Alpha ya conectado → READY enviado a Beta")

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.BINARY:
                    last_seen[code] = time.time()
                    alpha = listeners.get(code)
                    if alpha is not None and not alpha.closed:
                        if not await safe_bytes(alpha, msg.data):
                            listeners.pop(code, None)

                elif msg.type == aiohttp.WSMsgType.TEXT:
                    txt = msg.data.strip()
                    last_seen[code] = time.time()
                    if txt.startswith("INFO|"):
                        last_info[code] = txt
                        alpha = listeners.get(code)
                        if alpha is not None and not alpha.closed:
                            await safe_str(alpha, txt)
                        dead = set()
                        for w in list(watchers.get(code, set())):
                            if w.closed:
                                dead.add(w)
                            elif not await safe_str(w, txt):
                                dead.add(w)
                        watchers.get(code, set()).difference_update(dead)
                    else:
                        alpha = listeners.get(code)
                        if alpha is not None and not alpha.closed:
                            await safe_str(alpha, txt)

                elif msg.type in (
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR
                ):
                    break

        # ── ALPHA receptor ────────────────────────────────────────────────────
        elif text.startswith("JOIN:"):
            code = text[5:].strip()
            if not code.isdigit() or len(code) != 4:
                await safe_str(ws, "ERROR:INVALID_CODE")
                await ws.close()
                return ws

            role = "ALPHA"

            old = listeners.get(code)
            if old is not None and not old.closed:
                try:
                    await old.close()
                except Exception:
                    pass
            old_p = pending.get(code)
            if old_p is not None and not old_p.closed:
                try:
                    await old_p.close()
                except Exception:
                    pass

            beta = emitters.get(code)
            if beta is not None and not beta.closed:
                listeners[code] = ws
                await safe_str(ws, "OK")
                await safe_str(beta, "READY")
                if code in last_info:
                    await safe_str(ws, last_info[code])
                log(code, "Alpha conectado → READY enviado a Beta")
            else:
                pending[code] = ws
                await safe_str(ws, "WAITING")
                log(code, "Alpha esperando a Beta")

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    beta = emitters.get(code)
                    if beta is not None and not beta.closed:
                        await safe_str(beta, msg.data.strip())
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    beta = emitters.get(code)
                    if beta is not None and not beta.closed:
                        await safe_bytes(beta, msg.data)
                elif msg.type in (
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR
                ):
                    break

        # ── WATCHER ───────────────────────────────────────────────────────────
        elif text.startswith("WATCH:"):
            code = text[6:].strip()
            if not code.isdigit() or len(code) != 4:
                await safe_str(ws, "ERROR:INVALID_CODE")
                await ws.close()
                return ws
            role = "WATCHER"
            watchers.setdefault(code, set()).add(ws)
            await safe_str(ws, "WATCHING")
            log(code, "Watcher conectado")
            if code in last_info:
                await safe_str(ws, last_info[code])
            beta = emitters.get(code)
            if beta is not None and not beta.closed:
                await safe_str(beta, "GET_INFO")
            async for msg in ws:
                if msg.type in (
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR
                ):
                    break

        else:
            log("???", f"Desconocido: {text}")
            await ws.close()
            return ws

    except asyncio.CancelledError:
        raise
    except Exception as e:
        log(code or "???", f"Error [{role}]: {e}")
    finally:
        if code and role:
            try:
                await cleanup(code, role, ws)
            except Exception as e:
                log(code, f"Error limpieza: {e}")
        if not ws.closed:
            try:
                await ws.close()
            except Exception:
                pass

    return ws


async def main():
    port = int(os.environ.get("PORT", "8765"))
    app = web.Application()
    app.router.add_get("/", handle)
    app.router.add_get("/ws", handle)
    app.router.add_get("/ping", handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    print(f"Alpha-Beta Relay corriendo en puerto {port}", flush=True)
    asyncio.ensure_future(self_ping(app))
    await asyncio.Future()


asyncio.run(main())
