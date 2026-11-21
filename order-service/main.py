import json, logging, os, time
from uuid import uuid4
import pika, psycopg2
from fastapi import FastAPI, Header
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="{\"timestamp\":\"%(asctime)s\",\"service\":\"order-service\",\"message\":%(message)s}")
logger = logging.getLogger("order-service")
app = FastAPI(title="MaltaShop Order Service")

class OrderRequest(BaseModel):
    customer_id: str
    product_id: str
    quantity: int = Field(..., gt=0)

def get_db():
    return psycopg2.connect(os.environ["DATABASE_URL"])

@app.on_event("startup")
def startup():
    for _ in range(15):
        try:
            conn = get_db()
            with conn, conn.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS orders (order_id TEXT PRIMARY KEY, idempotency_key TEXT UNIQUE NOT NULL, customer_id TEXT NOT NULL, product_id TEXT NOT NULL, quantity INTEGER NOT NULL, status TEXT NOT NULL, correlation_id TEXT NOT NULL)")
            conn.close()
            return
        except Exception:
            time.sleep(1)

@app.get("/health")
def health():
    return {"status": "ok", "service": "order-service"}

@app.post("/orders", status_code=201)
def create_order(payload: OrderRequest, idempotency_key: str = Header(...), x_correlation_id: str | None = Header(default=None)):
    corr_id = x_correlation_id or f"corr-{uuid4().hex[:8]}"
    conn = get_db()
    with conn, conn.cursor() as cur:
        cur.execute("SELECT order_id, status, correlation_id FROM orders WHERE idempotency_key = %s", (idempotency_key,))
        row = cur.fetchone()
        if row:
            conn.close()
            return {"order_id": row[0], "status": row[1], "correlation_id": row[2], "idempotent_replay": True}
        order_id = f"ORD-{uuid4().hex[:8].upper()}"
        event_id = str(uuid4())
        cur.execute("INSERT INTO orders VALUES (%s,%s,%s,%s,%s,%s,%s)", (order_id, idempotency_key, payload.customer_id, payload.product_id, payload.quantity, "CONFIRMED", corr_id))
    conn.close()

    event = {"event_id": event_id, "event_type": "OrderPlaced", "order_id": order_id, "customer_id": payload.customer_id, "product_id": payload.product_id, "quantity": payload.quantity, "correlation_id": corr_id}
    params = pika.URLParameters(os.environ["BROKER_URL"])
    conn_mq = pika.BlockingConnection(params)
    ch = conn_mq.channel()
    ch.queue_declare(queue="order_placed", durable=True)
    ch.basic_publish(exchange="", routing_key="order_placed", body=json.dumps(event), properties=pika.BasicProperties(delivery_mode=2, correlation_id=corr_id))
    conn_mq.close()
    logger.info(json.dumps({"event": "OrderPlacedPublished", "order_id": order_id, "event_id": event_id, "correlation_id": corr_id}))
    return {"order_id": order_id, "status": "CONFIRMED", "correlation_id": corr_id}
