#!/usr/bin/env python3
# parse_event_log.py
# Usage: python3 parse_event_log.py /home/tuzi/spark-event-logs /home/tuzi/bench_results/shuffle_metrics.csv

import sys, os, json, glob, gzip, io
from collections import defaultdict

if len(sys.argv) < 3:
    print("Usage: parse_event_log.py <event_log_dir> <out_csv>")
    sys.exit(2)

logdir = sys.argv[1]
out_csv = sys.argv[2]

files = glob.glob(os.path.join(logdir, "*"))
files = [f for f in files if os.path.isfile(f)]
if not files:
    print("No event log files found in", logdir)
    sys.exit(1)

# For each app (file), accumulate shuffle metrics
rows = []
for f in files:
    total_shuffle_bytes_written = 0
    total_shuffle_records_written = 0
    total_remote_bytes_read = 0
    total_local_bytes_read = 0
    app_id = os.path.basename(f)
    opener = gzip.open if f.endswith(".gz") else open
    with opener(f, "rt", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            line = line.strip()
            if not line: 
                continue
            try:
                ev = json.loads(line)
            except:
                continue
            # Look for TaskEnd events that can carry shuffle metrics
            if isinstance(ev, dict) and ev.get("Event") == "SparkListenerTaskEnd":
                # The structure can be nested; try to safely extract metrics
                try:
                    # older versions embed metrics under 'Task Metrics' key or similar
                    taskMetrics = ev.get("Task Metrics") or ev.get("Task Metrics".lower()) or ev.get("Task Metrics".upper())
                    # fallback: check whole payload
                    if not taskMetrics:
                        # sometimes event has "Task Info" and "Task Metrics" fields
                        taskMetrics = ev.get("Task Metrics", None)
                except:
                    taskMetrics = None
                # fallback: find in raw fields
                # Many event logs put shuffle metrics under ev['Task Metrics'] keys like 'Shuffle Write' etc.
                # We'll scan JSON subtree for candidate keys.
                def deep_find(d, key):
                    if isinstance(d, dict):
                        if key in d: return d[key]
                        for v in d.values():
                            r = deep_find(v, key)
                            if r is not None:
                                return r
                    elif isinstance(d, list):
                        for item in d:
                            r = deep_find(item, key)
                            if r is not None:
                                return r
                    return None
                # Extract shuffle write bytes and records
                swb = deep_find(ev, "Shuffle Bytes Written") or deep_find(ev, "ShuffleBytesWritten") or deep_find(ev, "Shuffle Bytes")
                srw = deep_find(ev, "Shuffle Records Written") or deep_find(ev, "ShuffleRecordsWritten")
                rbr = deep_find(ev, "Remote Bytes Read") or deep_find(ev, "RemoteBytesRead") or deep_find(ev, "Remote Bytes")
                lbr = deep_find(ev, "Local Bytes Read") or deep_find(ev, "LocalBytesRead")
                # convert to int if present
                def toint(x):
                    try:
                        return int(x)
                    except:
                        return 0
                total_shuffle_bytes_written += toint(swb)
                total_shuffle_records_written += toint(srw)
                total_remote_bytes_read += toint(rbr)
                total_local_bytes_read += toint(lbr)
    rows.append((app_id, total_shuffle_bytes_written, total_shuffle_records_written, total_remote_bytes_read, total_local_bytes_read))

# write result CSV
with open(out_csv, "w") as fo:
    fo.write("app_id,shuffle_bytes_written,shuffle_records_written,remote_bytes_read,local_bytes_read\n")
    for r in rows:
        fo.write("{},{},{},{},{}\n".format(*r))
print("Wrote", out_csv)
