# app.py — S3 ALB .log(.gz) -> OpenSearch ingestor (SQS-driven, IRSA-ready, hardened)

import os, logging, gzip, re, threading, time, json
from datetime import datetime
from urllib.parse import unquote_plus
from flask import Flask, jsonify, request

import boto3
from botocore.exceptions import ClientError
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearchpy.helpers import bulk as os_bulk

# -------- Logging --------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("alb-ingestor")

# -------- Env --------
# S3_BUCKET is optional now (we read bucket from each S3 event). If set to s3://bucket[/prefix], we use it as a fallback.
S3_BUCKET_ENV   = os.getenv("S3_BUCKET")  # e.g. s3://uattestalblogs  or s3://uattestalblogs/AWSLogs/...
OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST")
OPENSEARCH_INDEX= os.getenv("OPENSEARCH_INDEX")
AWS_REGION      = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
FLASK_PORT      = int(os.getenv("FLASK_PORT", "5000"))

# SQS config
SQS_QUEUE_URL       = os.getenv("SQS_QUEUE_URL")  # e.g. https://sqs.eu-west-1.amazonaws.com/123456789012/alb-log-opensearch
SQS_WAIT_TIME_SECS  = int(os.getenv("SQS_WAIT_TIME_SECS", "20"))  # long-poll
SQS_MAX_MSGS        = int(os.getenv("SQS_MAX_MSGS", "10"))        # batch size
SQS_VISIBILITY_SECS = int(os.getenv("SQS_VISIBILITY_SECS", "300"))# 5m default

# -------- S3 bucket & prefix parsing (fallbacks) --------
def parse_bucket_prefix(s3_uri: str):
    if not s3_uri:
        return None, ""
    p = s3_uri.replace("s3://", "").strip("/")
    if "/" in p:
        b, pref = p.split("/", 1)
        return b, (pref + "/" if not pref.endswith("/") else pref)
    return p, ""

FALLBACK_S3_BUCKET, FALLBACK_S3_PREFIX = parse_bucket_prefix(S3_BUCKET_ENV)

def build_full_key_with_fallback(key: str) -> str:
    """For manual /ingest debug only — prepend fallback prefix if provided."""
    if not key:
        return key
    if FALLBACK_S3_PREFIX and not key.startswith(FALLBACK_S3_PREFIX):
        return f"{FALLBACK_S3_PREFIX}{key}"
    return key

# -------- AWS / OpenSearch helpers --------
def build_boto_session():
    session = boto3.Session(region_name=AWS_REGION)
    region = AWS_REGION or session.region_name
    if not region:
        raise RuntimeError("AWS region not set. Set AWS_REGION or AWS_DEFAULT_REGION.")
    return session, region

def build_awsauth(region: str):
    session, _ = build_boto_session()
    creds = session.get_credentials()
    if creds is None:
        raise RuntimeError("No AWS credentials available (IRSA/env).")
    _ = creds.access_key  # force refresh now to surface IRSA issues
    return AWS4Auth(creds.access_key, creds.secret_key, region, "es", session_token=creds.token)

def s3_client():
    _, region = build_boto_session()
    return boto3.client("s3", region_name=region)

def sqs_client():
    _, region = build_boto_session()
    return boto3.client("sqs", region_name=region)

def os_client():
    _, region = build_boto_session()
    awsauth = build_awsauth(region)
    return OpenSearch(
        hosts=[{"host": OPENSEARCH_HOST, "port": 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=60,
        max_retries=3,
        retry_on_timeout=True,
    )

def ensure_index(client):
    if not client.indices.exists(index=OPENSEARCH_INDEX):
        body = {
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {
                "properties": {
                    "@timestamp": {"type": "date"},
                    "type": {"type": "keyword"},
                    "time": {"type": "date"},
                    "elb": {"type": "keyword"},
                    "client_ip": {"type": "ip"},
                    "client_port": {"type": "integer"},
                    "target_ip": {"type": "ip"},
                    "target_port": {"type": "integer"},
                    "request_processing_time": {"type": "float"},
                    "target_processing_time": {"type": "float"},
                    "response_processing_time": {"type": "float"},
                    "elb_status_code": {"type": "integer"},
                    "target_status_code": {"type": "integer"},
                    "received_bytes": {"type": "long"},
                    "sent_bytes": {"type": "long"},
                    "request": {"type": "text"},
                    "user_agent": {"type": "text"},
                    "ssl_cipher": {"type": "keyword"},
                    "ssl_protocol": {"type": "keyword"},
                    "target_group_arn": {"type": "keyword"},
                    "trace_id": {"type": "keyword"},
                    "domain_name": {"type": "keyword"},
                    "chosen_cert_arn": {"type": "keyword"},
                    "matched_rule_priority": {"type": "integer"},
                    "request_creation_time": {"type": "date"},
                    "actions_executed": {"type": "keyword"},
                    "redirect_url": {"type": "keyword"},
                    "error_reason": {"type": "keyword"},
                    "target_port_list": {"type": "keyword"},
                    "target_status_code_list": {"type": "keyword"},
                    "classification": {"type": "keyword"},
                    "classification_reason": {"type": "keyword"},
                    "conn_trace_id": {"type": "keyword"},
                    "log_file": {"type": "keyword"},
                    "log_line_number": {"type": "integer"},
                }
            },
        }
        client.indices.create(index=OPENSEARCH_INDEX, body=body)
        log.info(f"Created index {OPENSEARCH_INDEX}")

# ---------- ALB parsing ----------
_field_pat = re.compile(r'([^\s]+|"[^"]*")')

def _safe_iso(val):
    if not val or val == "-":
        return None
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00")).isoformat()
    except Exception:
        return None

def looks_like_alb(sample: str) -> bool:
    for ln in sample.strip().split("\n")[:3]:
        if not ln.strip():
            continue
        if not re.match(r'^(http|https|h2|ws|wss)\s', ln):
            return False
        if len(_field_pat.findall(ln)) < 25:
            return False
    return True

def parse_line(line: str):
    if not line.strip():
        return None
    parts = [p.strip('"') for p in _field_pat.findall(line)]
    if len(parts) < 25:
        return None

    names = [
        'type','time','elb','client_ip_port','target_ip_port',
        'request_processing_time','target_processing_time','response_processing_time',
        'elb_status_code','target_status_code','received_bytes','sent_bytes',
        'request','user_agent','ssl_cipher','ssl_protocol','target_group_arn',
        'trace_id','domain_name','chosen_cert_arn','matched_rule_priority',
        'request_creation_time','actions_executed','redirect_url','error_reason',
        'target_port_list','target_status_code_list','classification','classification_reason'
    ]
    if len(parts) >= 29:
        names.append('conn_trace_id')

    d = {}
    for i, name in enumerate(names):
        d[name] = None if i >= len(parts) or parts[i] == "-" else parts[i]

    if d.get("type") not in ("http","https","h2","ws","wss"):
        return None

    # date fields (hardened)
    parsed_time = _safe_iso(d.get("time"))
    if parsed_time:
        d["@timestamp"] = parsed_time
        d["time"] = parsed_time
    else:
        d["@timestamp"] = datetime.utcnow().isoformat() + "Z"
        d.pop("time", None)

    rc = d.get("request_creation_time")
    if rc:
        parsed_rc = _safe_iso(rc)
        if parsed_rc:
            d["request_creation_time"] = parsed_rc
        else:
            d.pop("request_creation_time", None)

    # ip:port splits
    def split_ip_port(val, ip_k, port_k):
        if not val: return
        if ":" in val:
            ip, port = val.rsplit(":", 1)
            d[ip_k] = ip
            try: d[port_k] = int(port)
            except: pass
        else:
            d[ip_k] = val

    split_ip_port(d.get("client_ip_port"), "client_ip", "client_port")
    split_ip_port(d.get("target_ip_port"), "target_ip", "target_port")

    # numerics
    for k in ("request_processing_time","target_processing_time","response_processing_time"):
        if d.get(k) not in (None, "-"):
            try: d[k] = float(d[k])
            except: d[k] = None
    for k in ("elb_status_code","target_status_code","received_bytes","sent_bytes","matched_rule_priority"):
        if d.get(k) not in (None, "-"):
            try: d[k] = int(float(d[k]))
            except: d[k] = None

    return d

# ---------- S3 helpers / ingest ----------
def fetch_object(bucket, key) -> str:
    cli = s3_client()
    try:
        obj = cli.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        raise RuntimeError(f"S3 get_object failed [{code}] for s3://{bucket}/{key}") from e

    body = obj["Body"].read()
    if key.endswith(".gz"):
        try:
            return gzip.decompress(body).decode("utf-8", errors="replace")
        except Exception as ex:
            raise RuntimeError(f"Failed to decompress .gz content: {ex}")
    return body.decode("utf-8", errors="replace")

def ingest_object(bucket: str, key: str):
    """Core ingest using explicit bucket+key (used by SQS flow)."""
    log.info(f"[INGEST] s3://{bucket}/{key}")
    text = fetch_object(bucket, key)

    if not looks_like_alb(text):
        raise ValueError("Content does not look like ALB logs")

    lines = [ln for ln in text.splitlines() if ln.strip()]
    actions, ok, fail = [], 0, 0
    for i, ln in enumerate(lines, 1):
        doc = parse_line(ln)
        if not doc:
            fail += 1
            if fail <= 5:
                log.warning(f"[PARSE] Failed line {i}: {ln[:160]}")
            continue
        doc["log_file"] = key
        doc["log_line_number"] = i
        doc_id = f"{key}:{i}"  # deterministic id
        actions.append({
            "_index": OPENSEARCH_INDEX,
            "_id": doc_id,
            "_op_type": "index",
            "_source": doc
        })
        ok += 1

    if not actions:
        raise ValueError("No parsable lines")

    client = os_client()
    ensure_index(client)
    success, errors = os_bulk(client, actions, refresh=True, raise_on_error=False)
    err_count = len(errors) if errors else 0
    for e in (errors or [])[:5]:
        log.error(f"[BULK-ERROR] {e}")
    log.info(f"[INGEST] done ok={ok} failed={fail} indexed={success} errors={err_count}")
    return {"lines_ok": ok, "lines_failed": fail, "indexed": success, "errors": err_count}

# ---------- SQS consumer ----------
def _extract_records_from_sqs_message(msg_body: str):
    """
    Returns a list of (bucket, key) from an S3->SQS event message body.
    Handles URL-encoded keys.
    """
    out = []
    try:
        data = json.loads(msg_body)
    except Exception:
        log.error("[SQS] message body is not JSON")
        return out

    records = data.get("Records") or []
    for r in records:
        s3r = r.get("s3") or {}
        b = (s3r.get("bucket") or {}).get("name")
        k = (s3r.get("object") or {}).get("key")
        if b and k:
            out.append((b, unquote_plus(k)))
    return out

def _sqs_loop():
    if not SQS_QUEUE_URL:
        log.warning("[SQS] SQS_QUEUE_URL not set; SQS consumer disabled.")
        return

    cli = sqs_client()
    log.info(f"[SQS] consumer started. queue={SQS_QUEUE_URL} wait={SQS_WAIT_TIME_SECS}s batch={SQS_MAX_MSGS}")

    while True:
        try:
            resp = cli.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=SQS_MAX_MSGS,
                WaitTimeSeconds=SQS_WAIT_TIME_SECS,
                VisibilityTimeout=SQS_VISIBILITY_SECS
            )
            msgs = resp.get("Messages", [])
            if not msgs:
                continue

            log.info(f"[SQS] received {len(msgs)} message(s)")
            for m in msgs:
                receipt = m["ReceiptHandle"]
                body = m.get("Body", "")
                try:
                    pairs = _extract_records_from_sqs_message(body)
                    if not pairs:
                        log.warning("[SQS] no S3 records in message; deleting to avoid loop")
                        cli.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt)
                        continue

                    all_ok = True
                    for (bucket, key) in pairs:
                        try:
                            ingest_object(bucket, key)
                        except Exception as e:
                            all_ok = False
                            log.exception(f"[SQS] ingest failed for s3://{bucket}/{key}: {e}")

                    if all_ok:
                        cli.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt)
                        log.info("[SQS] message deleted")
                    else:
                        log.warning("[SQS] leaving message for retry (visibility will expire)")

                except Exception as e:
                    log.exception(f"[SQS] handler error: {e}")
                    # leave message; it will reappear after visibility timeout

        except Exception as e:
            log.exception(f"[SQS] receive loop error: {e}")
            time.sleep(5)

# ---------- Flask ----------
app = Flask(__name__)

@app.route("/")
def root():
    return jsonify({
        "service": "sqs-s3-to-opensearch",
        "queue": SQS_QUEUE_URL,
        "fallback_bucket": FALLBACK_S3_BUCKET,
        "fallback_prefix": FALLBACK_S3_PREFIX,
        "index": OPENSEARCH_INDEX,
        "region": AWS_REGION,
        "host": OPENSEARCH_HOST
    })

@app.route("/health")
def health():
    out = {"status": "ok", "index": OPENSEARCH_INDEX}
    try:
        c = os_client()
        out["opensearch_ping"] = bool(c.ping())
    except Exception as e:
        out["opensearch_ping"] = False
        out["opensearch_error"] = str(e)
        out["status"] = "degraded"
    try:
        if FALLBACK_S3_BUCKET:
            s3_client().head_bucket(Bucket=FALLBACK_S3_BUCKET)
        out["s3"] = "ok"
    except Exception as e:
        out["s3"] = f"error: {e}"
        out["status"] = "degraded"
    try:
        if SQS_QUEUE_URL:
            sqs_client().get_queue_attributes(QueueUrl=SQS_QUEUE_URL, AttributeNames=["ApproximateNumberOfMessages"])
        out["sqs"] = "ok"
    except Exception as e:
        out["sqs"] = f"error: {e}"
        out["status"] = "degraded"
    return jsonify(out), (200 if out["status"] == "ok" else 503)

@app.route("/diag/identity")
def diag_identity():
    try:
        _, region = build_boto_session()
        sts = boto3.client("sts", region_name=region)
        resp = sts.get_caller_identity()
        return jsonify({"ok": True, "identity": resp})
    except ClientError as e:
        return jsonify({"ok": False, "error": str(e)}), 503
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/probe/s3-object")
def probe_s3_object():
    # For manual checks (uses fallback bucket/prefix if provided)
    key = request.args.get("key")
    if not key and not FALLBACK_S3_BUCKET:
        return jsonify({"ok": False, "error": "Provide ?key=... or set S3_BUCKET fallback"}), 400

    bucket = request.args.get("bucket") or FALLBACK_S3_BUCKET
    full_key = build_full_key_with_fallback(key) if key else key
    try:
        s3_client().head_object(Bucket=bucket, Key=full_key)
        return jsonify({"ok": True, "bucket": bucket, "key": full_key})
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        return jsonify({"ok": False, "bucket": bucket, "key": full_key, "error": code, "detail": str(e)}), 404

# Start SQS consumer thread on import (works under Gunicorn too)
threading.Thread(target=_sqs_loop, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=FLASK_PORT)
