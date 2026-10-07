import asyncio
import aiohttp
from aiohttp import web
import os
import time

# ── Estado global ─────────────────────────────────────────────────────────────
emitters  = {}   # code → ws de Beta (emisor)
listeners = {}   # code → ws de Alpha (receptor activo)
pending   = {}   # code → ws de Alpha esperando que Beta aparezca
watchers  = {}   # code → set de ws observadores (futuro uso)
last_info = {}   # code → último INFO recibido de Beta
last_seen = {}   # code → timestamp última actividad de Beta

# ── Logger con timestamp ──────────────────────────────────────────────────────
def log(code, msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}][{code}] {msg}", flush=True)

# ── Envío seguro ──────────────────────────────────────────────────────────────
async def safe_str(ws, msg):
    try:
        if not ws.closed:
            await ws.send_str(msg)
            return True
    except Exception as e:
        pass
    return False

async def safe_bytes(ws, data):
    try:
        if not ws.closed:
            await ws.send_bytes(data)
            return True
    except Exception as e:
        pass
    return False

# ── Limpieza garantizada ──────────────────────────────────────────────────────
async def cleanup(code, role, ws):
    log(code, f"Limpiando {role}")

    if role == "BETA":
        if emitters.get(code) is ws:
            emitters.pop(code, None)
            last_seen.pop(code, None)
        # Notificar a Alpha que Beta se fue
        alpha = listeners.get(code)
        if alpha and not alpha.closed:
            await safe_str(alpha, "EMITTER_GONE")
            log(code, "EMITTER_GONE enviado a Alpha")
        # También notificar a Alpha si estaba en pending
        alpha_pending = pending.get(code)
        if alpha_pending and not alpha_pending.closed:
            await safe_str(alpha_pending, "EMITTER_GONE")
            log(code, "EMITTER_GONE enviado a Alpha en espera")

    elif role == "ALPHA":
        if listeners.get(code) is ws:
            listeners.pop(code, None)
        if pending.get(code) is ws:
            pending.pop(code, None)
        # Notificar a Beta que Alpha se fue — parar captura
        beta = emitters.get(code)
        if beta and not beta.closed:
            await safe_str(beta, "STOP")
            log(code, "STOP enviado a Beta — Alpha desconectado")

    elif role == "WATCHER":
        if code in watchers:
            watchers[code].discard(ws)

# ── Ping para mantener Railway despierto ──────────────────────────────────────
async def self_ping(app):
    url = os.environ.get("RAILWAY_STATIC_URL", os.environ.get("RENDER_EXTERNAL_URL", ""))
    if not url:
        return
    if not url.startswith("http"):
        url = "https://" + url
    while True:
        await asyncio.sleep(4 * 60)
        try:
            async with aiohttp.ClientSession() as session:
                await session.get(url + "/ping", timeout=aiohttp.ClientTimeout(total=10))
        except Exception:
            pass

async def handle_ping(request):
    return web.Response(text="Alpha-Beta Relay OK", status=200)

# ── Handler principal ─────────────────────────────────────────────────────────
async def handle(request):
    ws = web.WebSocketResponse(heartbeat=15)
    await ws.prepare(request)

    code = None
    role = None

    try:
        # Primer mensaje — identificación
        try:
            msg = await asyncio.wait_for(ws.receive(), timeout=15)
        except asyncio.TimeoutError:
            await ws.close()
            return ws

        if msg.type != aiohttp.WSMsgType.TEXT:
            await ws.close()
            return ws

        text = msg.data.strip()

        # ── BETA — emisor ─────────────────────────────────────────────────────
        if text.startswith("EMIT:"):
            code = text[5:].strip()
            if not code.isdigit() or len(code) != 4:
                await safe_str(ws, "ERROR:INVALID_CODE")
                await ws.close()
                return ws

            role = "BETA"
            log(code, "Beta conectado")

            # Cerrar Beta anterior si existe
            old = emitters.get(code)
            if old and not old.closed:
                try: await old.close()
                except Exception: pass

            emitters[code] = ws
            last_seen[code] = time.time()
            if code not in watchers:
                watchers[code] = set()

            await safe_str(ws, f"CODE:{code}")
            log(code, f"CODE:{code} enviado a Beta")

            # ── FIX BOOT: Alpha esperando en pending → conectar ahora ─────────
            # Este es el fix del bug principal: cuando Alpha mandó JOIN antes
            # de que Beta arrancara, quedó en pending. Ahora que Beta llegó
            # lo conectamos automáticamente sin que Alpha tenga que reintentar.
            alpha_pending = pending.pop(code, None)
            if alpha_pending and not alpha_pending.closed:
                listeners[code] = alpha_pending
                await safe_str(alpha_pending, "OK")
                await safe_str(ws, "READY")
                if code in last_info:
                    await safe_str(alpha_pending, last_info[code])
                log(code, "Alpha estaba en espera — conectado automáticamente, READY enviado a Beta")
            else:
                # Alpha ya conectado normalmente
                alpha = listeners.get(code)
                if alpha and not alpha.closed:
                    await safe_str(ws, "READY")
                    log(code, "Alpha ya conectado — READY enviado a Beta")

            # Loop principal de Beta
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.BINARY:
                    last_seen[code] = time.time()
                    alpha = listeners.get(code)
                    if alpha and not alpha.closed:
                        ok = await safe_bytes(alpha, msg.data)
                        if not ok:
                            listeners.pop(code, None)
                            log(code, "Alpha caído durante transmisión — removido")

                elif msg.type == aiohttp.WSMsgType.TEXT:
                    txt = msg.data.strip()

                    if txt.startswith("INFO|"):
                        last_info[code] = txt
                        last_seen[code] = time.time()
                        # Reenviar a Alpha
                        alpha = listeners.get(code)
                        if alpha and not alpha.closed:
                            await safe_str(alpha, txt)
                        # Reenviar a watchers
                        dead = set()
                        for w in list(watchers.get(code, set())):
                            if w.closed: dead.add(w)
                            else:
                                ok = await safe_str(w, txt)
                                if not ok: dead.add(w)
                        watchers.get(code, set()).difference_update(dead)

                    elif txt == "GET_INFO":
                        alpha = listeners.get(code)
                        if alpha and not alpha.closed:
                            await safe_str(alpha, txt)

                    else:
                        # Cualquier otro texto → reenviar a Alpha
                        alpha = listeners.get(code)
                        if alpha and not alpha.closed:
                            await safe_str(alpha, txt)

                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                    break

        # ── ALPHA — receptor ──────────────────────────────────────────────────
        elif text.startswith("JOIN:"):
            code = text[5:].strip()
            role = "ALPHA"

            # Cerrar Alpha anterior zombie
            old = listeners.get(code)
            if old and not old.closed:
                try: await old.close()
                except Exception: pass
            old_pending = pending.get(code)
            if old_pending and not old_pending.closed:
                try: await old_pending.close()
                except Exception: pass

            beta = emitters.get(code)

            if beta and not beta.closed:
                # Beta existe — conectar normal
                listeners[code] = ws
                await safe_str(ws, "OK")
                await safe_str(beta, "READY")
                if code in last_info:
                    await safe_str(ws, last_info[code])
                log(code, "Alpha conectado — READY enviado a Beta")
            else:
                # ── FIX BOOT: Beta no existe todavía — Alpha espera ───────────
                # En vez de responder ERROR:INVALID_CODE (que mataba el JOIN
                # para siempre), Alpha queda en pending y cuando Beta llegue
                # el relay los conecta automáticamente.
                pending[code] = ws
                await safe_str(ws, "WAITING")
                log(code, "Alpha en sala de espera — Beta aún no conectado")

            # Loop principal de Alpha
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    txt = msg.data.strip()
                    beta = emitters.get(code)
                    if beta and not beta.closed:
                        await safe_str(beta, txt)
                        log(code, f"Comando Alpha→Beta: {txt}")

                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                    break

        # ── WATCHER — solo info ───────────────────────────────────────────────
        elif text.startswith("WATCH:"):
            code = text[6:].strip()
            role = "WATCHER"

            if code not in watchers:
                watchers[code] = set()
            watchers[code].add(ws)
            await safe_str(ws, "WATCHING")
            log(code, "Watcher conectado")

            if code in last_info:
                await safe_str(ws, last_info[code])

            beta = emitters.get(code)
            if beta and not beta.closed:
                await safe_str(beta, "GET_INFO")

            async for msg in ws:
                if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                    break

        else:
            log("???", f"Mensaje desconocido: {text}")
            await ws.close()
            return ws

    except Exception as e:
        log(code or "???", f"Error [{role}]: {e}")
    finally:
        # Limpieza SIEMPRE garantizada
        if code and role:
            await cleanup(code, role, ws)
        if not ws.closed:
            try: await ws.close()
            except Exception: pass

    return ws

# ── App principal ─────────────────────────────────────────────────────────────
async def main():
    port = int(os.environ.get("PORT", 8765))
    app = web.Application()
    app.router.add_get("/", handle)
    app.router.add_get("/ws", handle)
    app.router.add_get("/ping", handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"Alpha-Beta Relay corriendo en puerto {port}", flush=True)
    asyncio.ensure_future(self_ping(app))
    await asyncio.Future()

asyncio.run(main())
