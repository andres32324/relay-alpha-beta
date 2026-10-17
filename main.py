
import asyncio
import aiohttp
from aiohttp import web
import os
import time
import secrets

# ── Estado global ─────────────────────────────────────────────────────────────
emitters = {}    # code -> WebSocket de Beta (emisor)
listeners = {}   # code -> WebSocket de Alpha (receptor activo)
pending = {}     # code -> WebSocket de Alpha esperando a Beta
watchers = {}    # code -> set de WebSockets observadores
last_info = {}   # code -> último INFO recibido de Beta
last_seen = {}   # code -> timestamp de última actividad de Beta


# ── Logger con timestamp ──────────────────────────────────────────────────────
def log(code, msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}][{code}] {msg}", flush=True)


# ── Generación de códigos de cuatro dígitos ───────────────────────────────────
def gen_code():
    for _ in range(10000):
        code = f"{secrets.randbelow(10000):04d}"

        if (
            code not in emitters
            and code not in listeners
            and code not in pending
        ):
            return code

    raise RuntimeError("No hay códigos disponibles")


# ── Envío seguro de mensajes ──────────────────────────────────────────────────
async def safe_str(ws, msg):
    try:
        if ws and not ws.closed:
            await ws.send_str(msg)
            return True
    except Exception:
        pass
    return False


async def safe_bytes(ws, data):
    try:
        if ws and not ws.closed:
            await ws.send_bytes(data)
            return True
    except Exception:
        pass
    return False


# ── Limpieza de conexiones ────────────────────────────────────────────────────
async def cleanup(code, role, ws):
    log(code, f"Limpiando {role}")

    if role == "BETA":
        # Solo la conexión Beta actual puede limpiar su sala.
        is_current = emitters.get(code) is ws

        if is_current:
            emitters.pop(code, None)
            last_seen.pop(code, None)

            # Informar al Alpha activo.
            alpha = listeners.get(code)
            if alpha and not alpha.closed:
                await safe_str(alpha, "EMITTER_GONE")
                log(code, "EMITTER_GONE enviado a Alpha")

            # Informar al Alpha que todavía está esperando.
            alpha_pending = pending.get(code)
            if alpha_pending and not alpha_pending.closed:
                await safe_str(alpha_pending, "EMITTER_GONE")
                log(code, "EMITTER_GONE enviado a Alpha en espera")

    elif role == "ALPHA":
        was_listener = listeners.get(code) is ws
        was_pending = pending.get(code) is ws

        if was_listener:
            listeners.pop(code, None)

        if was_pending:
            pending.pop(code, None)

        # Solo detener Beta si se desconectó el Alpha activo.
        # Una conexión antigua no debe detener la nueva sesión.
        if was_listener and listeners.get(code) is None:
            beta = emitters.get(code)

            if beta and not beta.closed:
                await safe_str(beta, "STOP")
                log(code, "STOP enviado a Beta: Alpha desconectado")

    elif role == "WATCHER":
        group = watchers.get(code)

        if group is not None:
            group.discard(ws)

            if not group:
                watchers.pop(code, None)


# ── Ping para mantener el servicio activo ─────────────────────────────────────
async def self_ping(app):
    url = os.environ.get(
        "RAILWAY_STATIC_URL",
        os.environ.get("RENDER_EXTERNAL_URL", "")
    )

    if not url:
        log("SYSTEM", "Sin URL pública configurada para self-ping")
        return

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    url = url.rstrip("/") + "/ping"

    timeout = aiohttp.ClientTimeout(total=10)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            await asyncio.sleep(240)

            try:
                async with session.get(url) as response:
                    await response.release() if False else asyncio.sleep(0)
                    log("SYSTEM", f"Self-ping HTTP {response.status}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log("SYSTEM", f"Error en self-ping: {e}")


async def handle_ping(request):
    return web.Response(text="Alpha-Beta Relay OK", status=200)


# ── Handler principal de WebSocket ────────────────────────────────────────────
async def handle(request):
    ws = web.WebSocketResponse(heartbeat=15)
    await ws.prepare(request)

    code = None
    role = None

    try:
        # Primer mensaje: identificación del cliente.
        try:
            msg = await asyncio.wait_for(
                ws.receive(),
                timeout=15
            )
        except asyncio.TimeoutError:
            await ws.close()
            return ws

        if msg.type != aiohttp.WSMsgType.TEXT:
            await ws.close()
            return ws

        text = msg.data.strip()

        # ── BETA: EMISOR ──────────────────────────────────────────────────────
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
            log(code, "Beta conectado")

            # Cerrar Beta anterior, si existe.
            old = emitters.get(code)

            if old and not old.closed:
                try:
                    await old.close()
                except Exception:
                    pass

            # Registrar la nueva conexión.
            emitters[code] = ws
            last_seen[code] = time.time()

            watchers.setdefault(code, set())

            await safe_str(ws, f"CODE:{code}")
            log(code, f"CODE:{code} enviado a Beta")

            # Conectar automáticamente al Alpha en espera.
            alpha_pending = pending.get(code)

            if alpha_pending and not alpha_pending.closed:
                pending.pop(code, None)
                listeners[code] = alpha_pending

                await safe_str(alpha_pending, "OK")
                await safe_str(ws, "READY")

                if code in last_info:
                    await safe_str(alpha_pending, last_info[code])

                log(
                    code,
                    "Alpha en espera conectado automáticamente; READY enviado"
                )

            else:
                # Si Alpha ya está conectado, preparar la sesión.
                alpha = listeners.get(code)

                if alpha and not alpha.closed:
                    await safe_str(alpha, "OK")
                    await safe_str(ws, "READY")
                    log(code, "Alpha existente; READY enviado a Beta")

            # Loop principal de Beta.
            async for msg in ws:

                if msg.type == aiohttp.WSMsgType.BINARY:
                    last_seen[code] = time.time()

                    alpha = listeners.get(code)

                    if alpha and not alpha.closed:
                        ok = await safe_bytes(alpha, msg.data)

                        if not ok and listeners.get(code) is alpha:
                            listeners.pop(code, None)
                            log(code, "Alpha caído durante transmisión")

                elif msg.type == aiohttp.WSMsgType.TEXT:
                    txt = msg.data.strip()
                    last_seen[code] = time.time()

                    if txt.startswith("INFO|"):
                        last_info[code] = txt

                        # Reenviar información al Alpha.
                        alpha = listeners.get(code)

                        if alpha and not alpha.closed:
                            await safe_str(alpha, txt)

                        # Reenviar información a los observadores.
                        dead = set()

                        for watcher in list(watchers.get(code, set())):
                            if watcher.closed:
                                dead.add(watcher)
                            else:
                                ok = await safe_str(watcher, txt)

                                if not ok:
                                    dead.add(watcher)

                        group = watchers.get(code)

                        if group is not None:
                            group.difference_update(dead)

                    elif txt == "GET_INFO":
                        # Beta solicita que Alpha envíe información.
                        alpha = listeners.get(code)

                        if alpha and not alpha.closed:
                            await safe_str(alpha, txt)

                    else:
                        # Otros mensajes de Beta -> Alpha.
                        alpha = listeners.get(code)

                        if alpha and not alpha.closed:
                            await safe_str(alpha, txt)

                elif msg.type in (
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR
                ):
                    break

        # ── ALPHA: RECEPTOR ───────────────────────────────────────────────────
        elif text.startswith("JOIN:"):
            code = text[5:].strip()

            if not code.isdigit() or len(code) != 4:
                await safe_str(ws, "ERROR:INVALID_CODE")
                await ws.close()
                return ws

            role = "ALPHA"

            # Cerrar Alpha anterior, si existe.
            old = listeners.get(code)

            if old and not old.closed:
                try:
                    await old.close()
                except Exception:
                    pass

            old_pending = pending.get(code)

            if old_pending and not old_pending.closed:
                try:
                    await old_pending.close()
                except Exception:
                    pass

            beta = emitters.get(code)

            if beta and not beta.closed:
                # Beta ya existe: conexión inmediata.
                listeners[code] = ws

                await safe_str(ws, "OK")
                await safe_str(beta, "READY")

                if code in last_info:
                    await safe_str(ws, last_info[code])

                log(code, "Alpha conectado; READY enviado a Beta")

            else:
                # Beta todavía no existe: esperar automáticamente.
                pending[code] = ws

                await safe_str(ws, "WAITING")
                log(code, "Alpha esperando a que aparezca Beta")

            # Loop principal de Alpha.
            async for msg in ws:

                if msg.type == aiohttp.WSMsgType.TEXT:
                    txt = msg.data.strip()
                    beta = emitters.get(code)

                    if beta and not beta.closed:
                        ok = await safe_str(beta, txt)

                        if ok:
                            log(code, f"Comando Alpha -> Beta: {txt}")

                elif msg.type == aiohttp.WSMsgType.BINARY:
                    # Permitir también mensajes binarios Alpha -> Beta.
                    beta = emitters.get(code)

                    if beta and not beta.closed:
                        await safe_bytes(beta, msg.data)

                elif msg.type in (
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR
                ):
                    break

        # ── WATCHER: SOLO INFORMACIÓN ─────────────────────────────────────────
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

            # Entregar la última información conocida.
            if code in last_info:
                await safe_str(ws, last_info[code])

            # Solicitar información actualizada a Beta.
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
            log("???", f"Mensaje desconocido: {text}")
            await safe_str(ws, "ERROR:UNKNOWN_COMMAND")
            await ws.close()
            return ws

    except asyncio.CancelledError:
        raise

    except Exception as e:
        log(code or "???", f"Error [{role}]: {e}")

    finally:
        # Limpiar siempre el registro de esta conexión.
        if code and role:
            try:
                await cleanup(code, role, ws)
            except Exception as e:
                log(code, f"Error durante limpieza: {e}")

        if not ws.closed:
            try:
                await ws.close()
            except Exception:
                pass

    return ws


# ── Aplicación principal ──────────────────────────────────────────────────────
async def main():
    port = int(os.environ.get("PORT", "8765"))

    app = web.Application()
    app.router.add_get("/", handle)
    app.router.add_get("/ws", handle)
    app.router.add_get("/ping", handle_ping)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(
        runner,
        host="0.0.0.0",
        port=port
    )

    await site.start()

    print(
        f"Alpha-Beta Relay corriendo en puerto {port}",
        flush=True
    )

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
