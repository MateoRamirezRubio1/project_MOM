from fastapi import FastAPI, HTTPException, Depends, Header
from pydantic import BaseModel
import requests, base64

MOM_URL = "http://localhost:8080"  # cambia si tu broker está en otra IP

app = FastAPI()


# ──────────────── MODELOS ────────────────
class Topic(BaseModel):
    name: str


class TopicMsg(BaseModel):
    key: str
    payload: str


class QueueMsg(BaseModel):
    payload: str  # las colas no usan “key”


class Queue(BaseModel):
    name: str


class Ack(BaseModel):
    id: str


class TokenBody(BaseModel):
    user: str
    pass_: str


class CommitBody(BaseModel):
    group: str
    partition: int
    offset: int


# ──────────────── HELPERS ────────────────
def _login(user: str, pwd: str) -> str:
    r = requests.post(f"{MOM_URL}/login", json={"user": user, "pass": pwd})
    if r.ok:
        return r.json()["token"]
    raise HTTPException(r.status_code, r.text)


def token_dep(X_Token: str = Header(...)):
    return X_Token


def decode_payloads(msgs):
    for m in msgs:
        try:
            m["Payload"] = base64.b64decode(m["Payload"]).decode()
        except Exception:
            pass
    return msgs


# ──────────────── AUTH ────────────────
@app.post("/login/")
def login(body: TokenBody):
    return {"token": _login(body.user, body.pass_)}


# ──────────────── TOPICS ────────────────
@app.post("/topics/")
def create_topic(t: Topic, token: str = Depends(token_dep)):
    r = requests.post(f"{MOM_URL}/topics", json=t.dict(), headers={"X-Token": token})
    if r.status_code == 201:
        return {"msg": "topic created"}
    raise HTTPException(r.status_code, r.text)


@app.get("/topics/")
def list_topics(token: str = Depends(token_dep)):
    r = requests.get(f"{MOM_URL}/topics", headers={"X-Token": token})
    if r.ok:
        return r.json()
    raise HTTPException(r.status_code, r.text)


@app.delete("/topics/{topic}")
def delete_topic(topic: str, token: str = Depends(token_dep)):
    r = requests.delete(f"{MOM_URL}/topics/{topic}", headers={"X-Token": token})
    if r.status_code == 204:
        return {"msg": "topic deleted"}
    raise HTTPException(r.status_code, r.text)


@app.post("/topics/{topic}/messages/")
def publish(topic: str, m: TopicMsg, token: str = Depends(token_dep)):
    r = requests.post(
        f"{MOM_URL}/topics/{topic}/messages", json=m.dict(), headers={"X-Token": token}
    )
    if r.ok:
        return r.json()
    raise HTTPException(r.status_code, r.text)


@app.get("/topics/{topic}/messages/")
def pull(
    topic: str,
    partition: int = 0,
    group: str = "default",
    max: int = 10,
    token: str = Depends(token_dep),
):
    r = requests.get(
        f"{MOM_URL}/topics/{topic}/messages",
        params={"partition": partition, "group": group, "max": max},
        headers={"X-Token": token},
    )
    if r.ok:
        return decode_payloads(r.json())
    raise HTTPException(r.status_code, r.text)


@app.post("/topics/{topic}/offsets/")
def commit_offset(topic: str, body: CommitBody, token: str = Depends(token_dep)):
    r = requests.post(
        f"{MOM_URL}/topics/{topic}/offsets",
        json=body.dict(),
        headers={"X-Token": token},
    )
    if r.status_code == 204:
        return {"msg": "offset committed"}
    raise HTTPException(r.status_code, r.text)


# ──────────────── QUEUES ────────────────
@app.post("/queues/")
def create_queue(q: Queue, token: str = Depends(token_dep)):
    r = requests.post(f"{MOM_URL}/queues", json=q.dict(), headers={"X-Token": token})
    if r.status_code == 201:
        return {"msg": "queue created"}
    raise HTTPException(r.status_code, r.text)


@app.get("/queues/")
def list_queues(token: str = Depends(token_dep)):
    r = requests.get(f"{MOM_URL}/queues", headers={"X-Token": token})
    if r.ok:
        return r.json()
    raise HTTPException(r.status_code, r.text)


@app.delete("/queues/{queue}")
def delete_queue(queue: str, token: str = Depends(token_dep)):
    r = requests.delete(f"{MOM_URL}/queues/{queue}", headers={"X-Token": token})
    if r.status_code == 204:
        return {"msg": "queue deleted"}
    raise HTTPException(r.status_code, r.text)


@app.post("/queues/{queue}/messages/")
def enqueue(queue: str, m: QueueMsg, token: str = Depends(token_dep)):
    r = requests.post(
        f"{MOM_URL}/queues/{queue}/messages", json=m.dict(), headers={"X-Token": token}
    )
    if r.status_code == 201:
        return {"msg": "enqueued"}
    raise HTTPException(r.status_code, r.text)


@app.get("/queues/{queue}/messages/")
def dequeue(queue: str, token: str = Depends(token_dep)):
    r = requests.get(f"{MOM_URL}/queues/{queue}/messages", headers={"X-Token": token})
    if r.ok:
        msg = r.json()
        return decode_payloads([msg])[0] if msg else {}
    raise HTTPException(r.status_code, r.text)


@app.post("/queues/{queue}/ack/")
def ack(queue: str, body: Ack, token: str = Depends(token_dep)):
    r = requests.post(
        f"{MOM_URL}/queues/{queue}/ack", json=body.dict(), headers={"X-Token": token}
    )
    if r.status_code == 204:
        return {"msg": "acked"}
    raise HTTPException(r.status_code, r.text)
