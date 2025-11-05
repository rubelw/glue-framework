import json, time

def emit_run_stats(**kwargs):
    payload = {"ts": int(time.time())}
    payload.update(kwargs)
    print(json.dumps(payload))
