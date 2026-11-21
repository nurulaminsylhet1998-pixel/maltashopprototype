import json, logging, os, time
import pika, psycopg2

logging.basicConfig(level=logging.INFO, format="{\"timestamp\":\"%(asctime)s\",\"service\":\"notification-service\",\"message\":%(message)s}")
logger = logging.getLogger("notification-service")

def get_db():
    return psycopg2.connect(os.environ["DATABASE_URL"])

def init_db():
    for _ in range(15):
        try:
            conn = get_db()
            with conn, conn.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS processed_events (event_id TEXT PRIMARY KEY)")
                cur.execute("CREATE TABLE IF NOT EXISTS notifications (id SERIAL PRIMARY KEY, order_id TEXT NOT NULL, customer_id TEXT NOT NULL, correlation_id TEXT NOT NULL, status TEXT NOT NULL)")
            conn.close()
            return
        except Exception:
            time.sleep(1)

def main():
    init_db()
    params = pika.URLParameters(os.environ["BROKER_URL"])
    while True:
        try:
            conn_mq = pika.BlockingConnection(params)
            ch = conn_mq.channel()
            ch.queue_declare(queue="order_placed", durable=True)
            ch.basic_qos(prefetch_count=1)
            def cb(ch, method, props, body):
                data = json.loads(body.decode("utf-8"))
                eid, oid, cid = data["event_id"], data["order_id"], data["customer_id"]
                corr = data.get("correlation_id") or (props.correlation_id if props else "unknown")
                conn = get_db()
                with conn, conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM processed_events WHERE event_id = %s", (eid,))
                    if not cur.fetchone():
                        cur.execute("INSERT INTO processed_events VALUES (%s)", (eid,))
                        cur.execute("INSERT INTO notifications (order_id, customer_id, correlation_id, status) VALUES (%s,%s,%s,%s)", (oid, cid, corr, "SENT"))
                conn.close()
                logger.info(json.dumps({"event": "OrderNotificationSent", "order_id": oid, "event_id": eid, "correlation_id": corr}))
                ch.basic_ack(delivery_tag=method.delivery_tag)
            ch.basic_consume(queue="order_placed", on_message_callback=cb)
            ch.start_consuming()
        except Exception as e:
            logger.warning(json.dumps({"event": "Reconnect", "error": str(e)}))
            time.sleep(3)

if __name__ == "__main__":
    main()
