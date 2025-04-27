import requests, base64

API = "http://localhost:8000"  # cliente FastAPI
USERS = ["alice", "bob", "charlie"]


def login(u):  # devuelve token
    r = requests.post(f"{API}/login/", json={"user": u, "pass_": "pwd"})
    r.raise_for_status()
    return r.json()["token"]


def out(label, r):
    try:
        body = r.json()
    except Exception:
        body = "(no body)"
    print(f"{label:<27} => {r.status_code} | {body}")


def decode(b):
    try:
        return base64.b64decode(b).decode()
    except Exception:
        return b


def main():
    toks = {u: login(u) for u in USERS}
    H = lambda u: {"X-Token": toks[u]}

    topic = "pagos"
    queue = "emails"

    # -------- TOPIC: ciclo de vida y consumer-groups --------
    out(
        "alice crea topic",
        requests.post(f"{API}/topics/", json={"name": topic}, headers=H("alice")),
    )
    out(
        "bob duplica topic",
        requests.post(f"{API}/topics/", json={"name": topic}, headers=H("bob")),
    )
    out("listar topics (bob)", requests.get(f"{API}/topics/", headers=H("bob")))

    # publish 3 mensajes diferentes claves
    for i in range(3):
        out(
            f"publish m{i}",
            requests.post(
                f"{API}/topics/{topic}/messages/",
                json={"key": f"k{i}", "payload": f"hola {i}"},
                headers=H("alice"),
            ),
        )

    # bob lee con group=g1
    r = requests.get(
        f"{API}/topics/{topic}/messages/",
        params={"group": "g1", "partition": 0},
        headers=H("bob"),
    )
    out("bob pull g1", r)
    msgs = r.json()
    last_offset = msgs[-1]["Offset"] + 1 if msgs else 0

    # commit
    out(
        "bob commit",
        requests.post(
            f"{API}/topics/{topic}/offsets/",
            json={"group": "g1", "partition": 0, "offset": last_offset},
            headers=H("bob"),
        ),
    )

    # bob vuelve a leer (vacío)
    out(
        "bob pull vacío",
        requests.get(
            f"{API}/topics/{topic}/messages/",
            params={"group": "g1", "partition": 0},
            headers=H("bob"),
        ),
    )

    # charlie (g2) aún ve mensajes
    out(
        "charlie pull g2",
        requests.get(
            f"{API}/topics/{topic}/messages/",
            params={"group": "g2", "partition": 0},
            headers=H("charlie"),
        ),
    )

    # borrado autorizado / no autorizado
    out("bob delete topic", requests.delete(f"{API}/topics/{topic}", headers=H("bob")))
    out(
        "alice delete topic",
        requests.delete(f"{API}/topics/{topic}", headers=H("alice")),
    )

    # -------- QUEUE: ciclo de vida --------
    out(
        "alice crea queue",
        requests.post(f"{API}/queues/", json={"name": queue}, headers=H("alice")),
    )

    for i in range(2):
        out(
            f"enqueue {i}",
            requests.post(
                f"{API}/queues/{queue}/messages/",
                json={"payload": f"mail {i}"},
                headers=H("alice"),
            ),
        )

    r = requests.get(f"{API}/queues/{queue}/messages/", headers=H("bob"))
    out("bob dequeue", r)
    mid = r.json().get("ID")
    if mid:
        out(
            "bob ack",
            requests.post(
                f"{API}/queues/{queue}/ack/", json={"id": mid}, headers=H("bob")
            ),
        )

    out("listar queues", requests.get(f"{API}/queues/", headers=H("alice")))

    out("bob delete queue", requests.delete(f"{API}/queues/{queue}", headers=H("bob")))
    out(
        "alice delete queue",
        requests.delete(f"{API}/queues/{queue}", headers=H("alice")),
    )


if __name__ == "__main__":
    main()
