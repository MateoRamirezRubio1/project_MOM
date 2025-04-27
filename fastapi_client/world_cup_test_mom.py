"""
DEMO MOM – Copa del Mundo 2026
==============================

• 2 tópicos   (world_cup, news_flash)
• 2 colas     (ticket-requests, vip-support)
• 6 usuarios  (fifa, argFan, braFan, newsDesk, marketing, staff)

Demuestra:
    – Autenticación multi-usuario
    – Ciclo de vida Topics / Queues con permisos de borrado
    – Publicación y particionamiento
    – Consumer-groups y commit de offset
    – Enqueue / Dequeue / Ack y re-enqueue por TTL
"""

import requests, base64, json, textwrap, time
from colorama import Fore, Style, init as colorama_init

API = "http://localhost:8000"  # Cliente FastAPI
TTL = 31  # segundos para mostrar re-enqueue

USERS = {
    "fifa": "pwd",
    "argFan": "pwd",
    "braFan": "pwd",
    "newsDesk": "pwd",
    "marketing": "pwd",
    "staff": "pwd",
}

colorama_init()


def _token(user, pwd):
    r = requests.post(f"{API}/login/", json={"user": user, "pass_": pwd})
    r.raise_for_status()
    return r.json()["token"]


TOK = {u: _token(u, p) for u, p in USERS.items()}
H = lambda u: {"X-Token": TOK[u]}


# ───────── helpers de impresión ─────────
def status_color(code):
    if 200 <= code < 300:
        return Fore.GREEN
    if 300 <= code < 400:
        return Fore.YELLOW
    return Fore.RED


def maybe_b64(s):
    if isinstance(s, str) and len(s) % 4 == 0:
        try:
            return base64.b64decode(s).decode()
        except Exception:
            pass
    return s


def clean(obj):
    "Acorta payloads y decodifica base64 si aplica"
    if isinstance(obj, list):
        return [clean(x) for x in obj]
    if isinstance(obj, dict):
        out = obj.copy()
        if "Payload" in out and out["Payload"] is not None:
            out["Payload"] = textwrap.shorten(maybe_b64(out["Payload"]), 40)
        return out
    return obj


def log(step, resp_or_obj):
    if isinstance(resp_or_obj, requests.Response):
        code = resp_or_obj.status_code
        try:
            body = resp_or_obj.json()
        except Exception:
            body = ""
    else:
        code, body = "", resp_or_obj
    c = status_color(code) if code else ""
    body_txt = (
        json.dumps(clean(body), ensure_ascii=False, indent=2) if body != "" else ""
    )
    print(f"{Fore.CYAN}{step}{Style.RESET_ALL}")
    if code != "":
        print(f"  {c}{code}{Style.RESET_ALL}")
    if body_txt:
        print(f"{body_txt}")
    print("-" * 60)


# ───────── DEMO ─────────
def main():
    # Resumen inicial
    print(f"{Fore.MAGENTA}=== DEMO MOM – Roles ==={Style.RESET_ALL}")
    print("* fifa       : creador de topics / cola de tickets")
    print("* newsDesk   : segundo productor (noticias) y dueño cola vip")
    print("* argFan/braFan : grupos fans-arg / fans-bra")
    print("* marketing  : grupo sponsors (consume noticias)")
    print("* staff      : atiende colas\n")

    t_events, t_news = "world_cup", "news_flash"
    q_tickets, q_vip = "ticket-requests", "vip-support"
    part_events = 1  # partición donde caen los goles

    # 1. Creación de tópicos
    log(
        "FIFA crea topic 'world_cup'",
        requests.post(f"{API}/topics/", json={"name": t_events}, headers=H("fifa")),
    )
    log(
        "newsDesk crea topic 'news_flash'",
        requests.post(f"{API}/topics/", json={"name": t_news}, headers=H("newsDesk")),
    )

    # 2. Publicaciones
    for ev in ["Gol ARG-FRA m12", "Gol BRA-ENG m27", "Roja m90+3"]:
        log(
            f"FIFA publica evento: {ev}",
            requests.post(
                f"{API}/topics/{t_events}/messages/",
                json={"key": "match1", "payload": ev},
                headers=H("fifa"),
            ),
        )

    for hl in ["Patrocinador oficial anunciado", "Nuevo balón 2026 presentado"]:
        log(
            f"newsDesk publica headline: {hl}",
            requests.post(
                f"{API}/topics/{t_news}/messages/",
                json={"key": "pr", "payload": hl},
                headers=H("newsDesk"),
            ),
        )

    # 3. Consumo por grupos
    r = requests.get(
        f"{API}/topics/{t_events}/messages/",
        params={"group": "fans-arg", "partition": part_events},
        headers=H("argFan"),
    )
    log("argFan (grupo fans-arg) pull", r)
    off_arg = r.json()[-1]["Offset"] + 1 if r.json() else 0
    requests.post(
        f"{API}/topics/{t_events}/offsets/",
        json={"group": "fans-arg", "partition": part_events, "offset": off_arg},
        headers=H("argFan"),
    )

    r = requests.get(
        f"{API}/topics/{t_events}/messages/",
        params={"group": "fans-bra", "partition": part_events},
        headers=H("braFan"),
    )
    log("braFan (fans-bra) pull", r)

    r = requests.get(
        f"{API}/topics/{t_news}/messages/",
        params={"group": "sponsors", "partition": 0},
        headers=H("marketing"),
    )
    log("marketing (sponsors) recibe news", r)

    # 4. Permisos de borrado
    log(
        "argFan intenta borrar topic news_flash (403)",
        requests.delete(f"{API}/topics/{t_news}", headers=H("argFan")),
    )
    log(
        "newsDesk borra topic news_flash",
        requests.delete(f"{API}/topics/{t_news}", headers=H("newsDesk")),
    )

    # 5. Colas
    log(
        "FIFA crea cola ticket-requests",
        requests.post(f"{API}/queues/", json={"name": q_tickets}, headers=H("fifa")),
    )
    log(
        "newsDesk crea cola vip-support",
        requests.post(f"{API}/queues/", json={"name": q_vip}, headers=H("newsDesk")),
    )

    for fan in ("argFan", "braFan"):
        log(
            f"{fan} enqueue solicitud ticket",
            requests.post(
                f"{API}/queues/{q_tickets}/messages/",
                json={"payload": f"Ticket request for {fan}"},
                headers=H(fan),
            ),
        )

    # staff atiende primera y la ackea
    r = requests.get(f"{API}/queues/{q_tickets}/messages/", headers=H("staff"))
    log("staff dequeue #1", r)
    mid1 = r.json()["ID"]
    log(
        "staff ack #1",
        requests.post(
            f"{API}/queues/{q_tickets}/ack/", json={"id": mid1}, headers=H("staff")
        ),
    )

    # staff atiende segunda PERO NO HACE ACK
    r = requests.get(f"{API}/queues/{q_tickets}/messages/", headers=H("staff"))
    log("staff dequeue #2 (sin ack)", r)

    # Espera TTL para re-enqueue
    print(f"{Fore.YELLOW}Esperando {TTL}s para mostrar re-enqueue…{Style.RESET_ALL}")
    time.sleep(TTL)
    r = requests.get(f"{API}/queues/{q_tickets}/messages/", headers=H("staff"))
    log("staff ve mensaje re-enqueued", r)
    mid2 = r.json().get("ID")
    if mid2:
        requests.post(
            f"{API}/queues/{q_tickets}/ack/", json={"id": mid2}, headers=H("staff")
        )

    # 6. Listar y borrar colas
    log("listar colas (fifa)", requests.get(f"{API}/queues/", headers=H("fifa")))
    log(
        "braFan intenta borrar ticket-requests (403)",
        requests.delete(f"{API}/queues/{q_tickets}", headers=H("braFan")),
    )
    log(
        "FIFA borra ticket-requests",
        requests.delete(f"{API}/queues/{q_tickets}", headers=H("fifa")),
    )
    log(
        "newsDesk borra vip-support",
        requests.delete(f"{API}/queues/{q_vip}", headers=H("newsDesk")),
    )


if __name__ == "__main__":
    main()
