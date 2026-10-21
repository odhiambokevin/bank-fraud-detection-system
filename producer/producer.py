import argparse
import glob
import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import polars as pl
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

log = logging.getLogger("producer")
BASE_EPOCH_MS = int(datetime(2017, 11, 30, tzinfo=timezone.utc).timestamp() * 1000)
BATCH = 5000

def env_int(name, default):
    return int(os.getenv(name, default))

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=os.getenv("DATA_DIR", "/data"))
    ap.add_argument("--cache-dir", default=os.getenv("CACHE_DIR", "/cache"))
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
    ap.add_argument("--topic-raw", default=os.getenv("TOPIC_RAW", "transactions.raw"))
    ap.add_argument("--topic-gt", default=os.getenv("TOPIC_GT", "ground-truth"))
    ap.add_argument("--rate", type=float, default=float(os.getenv("REPLAY_RATE", "200")),
                    help="events per second (ignored with --burst)")
    ap.add_argument("--burst", action="store_true", default=os.getenv("REPLAY_BURST", "0") == "1")
    ap.add_argument("--max-rows", type=int, default=env_int("MAX_ROWS", 0), help="0 = whole dataset")
    return ap.parse_args()

def prepare(data_dir, cache_dir):
    """join csvs, sort once, cache as parquet keyed by the input files' size/mtime."""
    tx_path = os.path.join(data_dir, "train_transaction.csv")
    id_path = os.path.join(data_dir, "train_identity.csv")
    if not os.path.exists(tx_path):
        sys.exit(f"{tx_path} not found. Put the CSVs in ./data.")
    sig = hashlib.md5()
    for p in (tx_path, id_path):
        if os.path.exists(p):
            st = os.stat(p)
            sig.update(f"{p}:{st.st_size}:{int(st.st_mtime)}".encode())
    os.makedirs(cache_dir, exist_ok=True)
    cached = os.path.join(cache_dir, f"prepared_{sig.hexdigest()[:12]}.parquet")
    if os.path.exists(cached):
        log.info("using cached %s", cached)
        return cached
    for old in glob.glob(os.path.join(cache_dir, "prepared_*.parquet")):
        os.remove(old)
    log.info("joining and sorting CSVs (one-off, can take a minute) ...")
    tx = pl.scan_csv(tx_path, infer_schema_length=None, null_values=["", "NaN"])
    if os.path.exists(id_path):
        ident = pl.scan_csv(id_path, infer_schema_length=None, null_values=["", "NaN"])
        tx = tx.join(ident, on="TransactionID", how="left")
    tx.sort("TransactionDT").sink_parquet(cached) 
    return cached

def make_producer(bootstrap):
    for attempt in range(1, 31):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap, acks="all", linger_ms=20, retries=5,
                compression_type="gzip", max_request_size=5_242_880,
            )
        except NoBrokersAvailable:
            log.warning("broker not reachable yet (%d/30)", attempt)
            time.sleep(2)
    sys.exit("could not reach Kafka")

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    lf = pl.scan_parquet(prepare(args.data_dir, args.cache_dir))
    total = lf.select(pl.len()).collect().item()
    n = min(total, args.max_rows) if args.max_rows else total
    log.info("replaying %d of %d rows, rate=%s", n, total, "burst" if args.burst else f"{args.rate}/s")

    producer = make_producer(args.bootstrap)
    interval = 0.0 if args.burst or args.rate <= 0 else 1.0 / args.rate
    started, sent = time.perf_counter(), 0

    for offset in range(0, n, BATCH):
        batch = lf.slice(offset, min(BATCH, n - offset)).collect()
        for row in batch.iter_rows(named=True):
            label = int(row.pop("isFraud") or 0)
            tid = int(row["TransactionID"])
            payload = {k: v for k, v in row.items() if v is not None}   # drop nulls: ~half the bytes
            payload["event_time_ms"] = BASE_EPOCH_MS + int(row["TransactionDT"]) * 1000
            producer.send(args.topic_raw, key=str(row["card1"]).encode(),
                          value=json.dumps(payload, separators=(",", ":")).encode())
            producer.send(args.topic_gt, key=str(tid).encode(),
                          value=json.dumps({"transaction_id": tid, "is_fraud": label}).encode())
            sent += 1
            if interval:
                delay = started + sent * interval - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)
        if (offset // BATCH) % 10 == 0:
            log.info("sent %d/%d (%.0f ev/s)", sent, n, sent / max(time.perf_counter() - started, 1e-6))
    producer.flush()
    log.info("done: %d events in %.1fs", sent, time.perf_counter() - started)

if __name__ == "__main__":
    main()
