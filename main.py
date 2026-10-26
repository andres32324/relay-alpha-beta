import asyncio
import aiohttp
from aiohttp import web
import os
import time
import secrets

# ── Estado global ─────────────────────────────────────────────────────────────
emitters = {}
listeners = {}
pending = {}
watchers = {}
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
    raise RuntimeError("No hay codigos disponibles")


# ── safe_str SIN chequeo ws.closed (bug de aiohttp con proxy de Railway) ─────
async def safe_str(ws, msg):
    try:
        if ws:
            await ws.send_str(msg)
            return True
    except Exception as e:
        log("safe_str", f"ERROR '{msg[:30]}': {type(e).__name__}: {e}")
    return False


async def safe_bytes(ws, data):
    try:
        if ws:
            await ws.send_bytes(data)
            return True
    except Exception as e:
        log("safe_bytes", f"ERROR bytes ({len(data)}B): {type(e).__name__}: {e}")
    return False


# ── Envío de CODE: inmediato + reintentos ─────────────────────────────────────
async def send_code(ws, code):
    log(code, f"send_code: intentando enviar CODE:{code} (ws.closed={ws.closed if ws else 'N/A'})")
    ok = await safe_str(ws, f"CODE:{code}")
    if ok:
        log(code, f"CODE:{code} enviado OK")
    else:
        log(code, f"CODE:{code} fallo - iniciando reintentos en background")
    return ok


async def retry_code_task(ws, code):
    for i in range(15):
        await asyncio.sleep(1)
        if code in listeners or code in pending:
            return
        log(code, f"retry_code t+{i+1}s: ws.closed={ws.closed if ws else 'N/A'}")
        ok = await safe_str(ws, f"CODE:{code}")
        if ok:
            log(code, f"CODE:{code} enviado en retry t+{i+1}s")
            return
    log(code, f"retry_code: agotados 15 intentos para CODE:{code}")


# ── Limpieza ──────────────────────────────────────────────────────────────────
async def cleanup(code, role, ws):
    log(code, f"Limpiando {role}")
    if role == "BETA":
        if emitters.get(code) is ws:
            emitters.pop(code, None)
            last_seen.pop(code, None)
            alpha = listeners.get(code)
            if alpha and not alpha.closed:
                await safe_str(alpha, "EMITTER_GONE")
            alpha_p = pending.get(code)
            if alpha_p and not alpha_p.closed:
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
            if beta and not beta.closed:
                await safe_str(beta, "STOP")
    elif role == "WATCHER":
        group = watchers.get(code)
        if group is not None:
            group.discard(ws)
            if not group:
                watchers.pop(code, None)


# ── Self-ping ─────────────────────────────────────────────────────────────────
async def self_ping(app):
    url = os.environ.get("RAILWAY_STATIC_URL", os.environ.get("RENDER_EXTERNAL_URL", ""))
    if not url:
        log("SYSTEM", "Sin URL para self-ping")
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
                    await asyncio.sleep(0)
                    log("SYSTEM", f"Self-ping HTTP {r.status}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log("SYSTEM", f"Self-ping error: {e}")


async def handle_ping(request):
    return web.Response(text="Alpha-Beta Relay OK", status=200)


# ── Handler WebSocket ─────────────────────────────────────────────────────────
async def handle(request):
    ws = web.WebSocketResponse(heartbeat=25)
    await ws.prepare(request)

    code = None
    role = None

    try:
        try:
            msg = await asyncio.wait_for(ws.receive(), timeout=15)
        except asyncio.TimeoutError:
            log("???", "Timeout identificacion")
            await ws.close()
            return ws

        if msg.type != aiohttp.WSMsgType.TEXT:
            await ws.close()
            return ws

        text = msg.data.strip()
        log("???", f"Primer mensaje: [{text}] ws.closed={ws.closed}")

        # ── BETA ──────────────────────────────────────────────────────────────
        if text == "EMIT" or text.startswith("EMIT:"):

            if text.startswith("EMIT:"):
                code = text[5:].strip()
                if not code.isdigit() or len(code) != 4:
                    await safe_str(ws, "ERROR:INVALID_CODE")
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
            log(code, f"Beta conectado ws.closed={ws.closed}")

            old = emitters.get(code)
            if old and not old.closed:
                try:
                    await old.close()
                except Exception:
                    pass

            emitters[code] = ws
            last_seen[code] = time.time()
            watchers.setdefault(code, set())

            await send_code(ws, code)
            asyncio.create_task(retry_code_task(ws, code))

            alpha_pending = pending.get(code)
            if alpha_pending and not alpha_pending.closed:
                pending.pop(code, None)
                listeners[code] = alpha_pending
                await safe_str(alpha_pending, "OK")
                await safe_str(ws, "READY")
                if code in last_info:
                    await safe_str(alpha_pending, last_info[code])
                log(code, "Alpha en espera conectado; READY enviado")
            else:
                alpha = listeners.get(code)
                if alpha and not alpha.closed:
                    await safe_str(alpha, "OK")
                    await safe_str(ws, "READY")
                    log(code, "Alpha existente; READY enviado")

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.BINARY:
                    last_seen[code] = time.time()
                    alpha = listeners.get(code)
                    if alpha and not alpha.closed:
                        ok = await safe_bytes(alpha, msg.data)
                        if not ok and listeners.get(code) is alpha:
                            listeners.pop(code, None)
                            log(code, "Alpha caido durante transmision")

                elif msg.type == aiohttp.WSMsgType.TEXT:
                    txt = msg.data.strip()
                    last_seen[code] = time.time()
                    if txt.startswith("INFO|"):
                        last_info[code] = txt
                        alpha = listeners.get(code)
                        if alpha and not alpha.closed:
                            await safe_str(alpha, txt)
                        dead = set()
                        for watcher in list(watchers.get(code, set())):
                            if watcher.closed:
                                dead.add(watcher)
                            else:
                                if not await safe_str(watcher, txt):
                                    dead.add(watcher)
                        group = watchers.get(code)
                        if group is not None:
                            group.difference_update(dead)
                    elif txt == "GET_INFO":
                        alpha = listeners.get(code)
                        if alpha and not alpha.closed:
                            await safe_str(alpha, txt)
                    else:
                        alpha = listeners.get(code)
                        if alpha and not alpha.closed:
                            await safe_str(alpha, txt)

                elif msg.type in (
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR
                ):
                    break

        # ── ALPHA ─────────────────────────────────────────────────────────────
        elif text.startswith("JOIN:"):
            code = text[5:].strip()
            if not code.isdigit() or len(code) != 4:
                await safe_str(ws, "ERROR:INVALID_CODE")
                await ws.close()
                return ws

            role = "ALPHA"
            old = listeners.get(code)
            if old and not old.closed:
                try:
                    await old.close()
                except Exception:
                    pass
            old_p = pending.get(code)
            if old_p and not old_p.closed:
                try:
                    await old_p.close()
                except Exception:
                    pass

            beta = emitters.get(code)
            if beta and not beta.closed:
                listeners[code] = ws
                await safe_str(ws, "OK")
                await safe_str(beta, "READY")
                if code in last_info:
                    await safe_str(ws, last_info[code])
                log(code, "Alpha conectado; READY enviado a Beta")
            else:
                pending[code] = ws
                await safe_str(ws, "WAITING")
                log(code, "Alpha esperando a Beta")

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    txt = msg.data.strip()
                    beta = emitters.get(code)
                    if beta and not beta.closed:
                        ok = await safe_str(beta, txt)
                        if ok:
                            log(code, f"Alpha->Beta: {txt}")
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    beta = emitters.get(code)
                    if beta and not beta.closed:
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
            if beta and not beta.closed:
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
            await safe_str(ws, "ERROR:UNKNOWN_COMMAND")
            await ws.close()
            return ws

    except asyncio.CancelledError:
        raise
    except Exception as e:
        log(code or "???", f"Error [{role}]: {type(e).__name__}: {e}")
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


# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    port = int(os.environ.get("PORT", "8765"))
    app = web.Application()
    app.router.add_get("/", handle)
    app.router.add_get("/ws", handle)
    app.router.add_get("/ping", handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, host="0.0.0.0", port=port).start()
    print(f"Alpha-Beta Relay corriendo en puerto {port}", flush=True)
    ping_task = asyncio.create_task(self_ping(app))
    try:
        await asyncio.Future()
    finally:
        ping_task.cancel()
        try:
            await ping_task
        except asyncio.CancelledError:
            pass
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
