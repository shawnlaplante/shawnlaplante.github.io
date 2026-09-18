#!/usr/bin/env python3
"""
LaPlante Custom Tech Dashboard — YTD Revenue per Job Hour ETL.

Rebuilds index.html in place, replacing ONLY the `const DATA = {...};` block.
The `const PHOTOS = {...};` block and all surrounding markup are preserved
byte-for-byte.

Credentials come from environment variables — NEVER hardcode them here.
This file lives in a repo; anything written into it is permanent in git history.

Required env vars:
    ST_TENANT_ID
    ST_APP_KEY
    ST_CLIENT_ID
    ST_CLIENT_SECRET

Methodology (confirmed — do not deviate):
    Revenue per Job Hour = attributed revenue / the tech's OWN "Working" hours
    - Revenue is attributed by ServiceTitan's manual per-job Split % field,
      NOT split evenly and NOT hours-weighted.
    - Hours count only the tech's own clocked "Working" activity, and only on
      jobs in the Service/Maintenance matched set. A tech's raw Working total
      will often exceed this because of work in other business units. That is
      correct and expected — do not "fix" it.
    Built from raw API data, never from the native Tech Key Metrics report
    (proven inconsistent, ~20% hours discrepancy for at least one tech).
"""

import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

# The build server runs on UTC. Everything shown on the office TV must be in
# Shawn's local time, or the "Updated" stamp reads ~4 hours ahead and the page
# looks fresher than it is.
OFFICE_TZ = ZoneInfo("America/New_York")

# ---------------------------------------------------------------- config

TENANT = os.environ["ST_TENANT_ID"].strip()
APP_KEY = os.environ["ST_APP_KEY"].strip()
CLIENT_ID = os.environ["ST_CLIENT_ID"].strip()
CLIENT_SECRET = os.environ["ST_CLIENT_SECRET"].strip()

# ServiceTitan's Cloudflare blocks the default urllib User-Agent (error 1010).
USER_AGENT = "laplante-etl/2.0"

# 1-SERVICE and 3-MAINTENANCE. Everything else is excluded, even when a
# Service tech picks up Installation or Sales work.
BUSINESS_UNITS = {24748514, 24750178}

API = "https://api.servicetitan.io"
MIN_JOBS_FOR_MAIN = 5

# Sanity floors — refuse to publish a collapsed leaderboard.
MIN_TOTAL_JOBS = 1000
MIN_MAIN_TECHS = 15

# gross-pay-items returns no totalCount, so sweep a bounded range and detect
# the true end as the highest page that came back non-empty.
PAY_ITEM_PAGE_SWEEP = 200

TODAY = datetime.now(OFFICE_TZ).date()
JAN1 = date(TODAY.year, 1, 1).isoformat()
TOMORROW = (TODAY + timedelta(days=1)).isoformat()
TODAY_S = TODAY.isoformat()

# ---------------------------------------------------------------- auth

_token = {"value": None, "fetched_at": 0.0}
_token_lock = threading.Lock()


def token(force=False):
    """Access token, refreshed under a lock so pooled workers all recover."""
    with _token_lock:
        stale = time.time() - _token["fetched_at"] > 600
        if force or _token["value"] is None or stale:
            body = urllib.parse.urlencode({
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            }).encode()
            req = urllib.request.Request(
                "https://auth.servicetitan.io/connect/token",
                data=body,
                headers={
                    "User-Agent": USER_AGENT,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
            with urllib.request.urlopen(req, timeout=60) as r:
                _token["value"] = json.load(r)["access_token"]
            _token["fetched_at"] = time.time()
        return _token["value"]


def get(url, tries=6):
    """GET with auth refresh on 401 and backoff on throttling/5xx."""
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers={
                # Raw token, no "Bearer" prefix — ServiceTitan is specific here.
                "Authorization": token(),
                "ST-App-Key": APP_KEY,
                "User-Agent": USER_AGENT,
            })
            with urllib.request.urlopen(req, timeout=90) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 401:
                token(force=True)
                time.sleep(1)
                continue
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(2 * (attempt + 1))
                continue
            raise
        except Exception:
            if attempt == tries - 1:
                raise
            time.sleep(2 * (attempt + 1))
    raise RuntimeError("giving up on " + url)


def log(*a):
    print(*a, flush=True)


# ---------------------------------------------------------------- steps

def fetch_jobs():
    """Completed jobs YTD, filtered client-side to Service/Maintenance.

    The businessUnitIds query param is silently ignored by this endpoint,
    so filtering must happen here.
    """
    jobs, page = {}, 1
    while True:
        r = get(f"{API}/jpm/v2/tenant/{TENANT}/jobs"
                f"?completedOnOrAfter={JAN1}T00:00:00Z"
                f"&completedBefore={TOMORROW}T00:00:00Z"
                f"&jobStatus=Completed&pageSize=200&page={page}")
        for j in r.get("data", []):
            if j.get("businessUnitId") in BUSINESS_UNITS:
                jobs[str(j["id"])] = float(j.get("total") or 0)
        if not r.get("hasMore"):
            break
        page += 1
    log(f"[1/4] matched Service/Maintenance jobs: {len(jobs)}")
    return jobs


def fetch_splits(jobs):
    """Per-job technician revenue splits, concurrently."""
    revenue, job_ids = {}, {}
    lock = threading.Lock()
    failures = []

    def one(jid):
        try:
            r = get(f"{API}/payroll/v2/tenant/{TENANT}/jobs/{jid}/splits")
        except Exception as e:
            with lock:
                failures.append((jid, repr(e)[:90]))
            return
        total = jobs[jid]
        with lock:
            for s in r.get("data", []):
                pct = float(s.get("split") or 0)
                if pct > 0:
                    tid = s["technicianId"]
                    revenue[tid] = revenue.get(tid, 0.0) + total * pct / 100.0
                    job_ids.setdefault(tid, set()).add(jid)

    with ThreadPoolExecutor(max_workers=20) as ex:
        list(ex.map(one, list(jobs)))

    if failures:
        raise RuntimeError(
            f"{len(failures)} split lookups failed, e.g. {failures[:3]} — "
            "refusing to publish partial revenue")
    log(f"[2/4] techs with attributed revenue: {len(revenue)}")
    return revenue, job_ids


def fetch_hours(jobs):
    """Own 'Working' hours per employee, restricted to the matched job set."""
    base = (f"{API}/payroll/v2/tenant/{TENANT}/gross-pay-items"
            f"?dateOnOrAfter={JAN1}&dateBefore={TOMORROW}&pageSize=500")
    results = {}
    lock = threading.Lock()

    def one(p):
        r = get(base + f"&page={p}")
        hours, scanned, kept = {}, 0, 0
        for g in r.get("data", []):
            scanned += 1
            d = (g.get("date") or "")[:10]
            # The dateBefore filter leaks records outside the range, so
            # re-filter on the date string directly.
            if not (JAN1 <= d <= TODAY_S):
                continue
            if g.get("activity") != "Working":
                continue
            if str(g.get("jobId")) not in jobs:
                continue
            eid = g.get("employeeId")
            hours[eid] = hours.get(eid, 0.0) + float(g.get("paidDurationHours") or 0)
            kept += 1
        with lock:
            results[p] = (hours, scanned, kept)

    with ThreadPoolExecutor(max_workers=16) as ex:
        list(ex.map(one, range(1, PAY_ITEM_PAGE_SWEEP + 1)))

    nonempty = [p for p, (_, s, _) in results.items() if s > 0]
    last = max(nonempty) if nonempty else 0
    missing = [p for p in range(1, last + 1) if p not in results]
    if missing:
        raise RuntimeError(f"gap in pay-item pages: {missing[:10]}")
    if last >= PAY_ITEM_PAGE_SWEEP:
        raise RuntimeError(
            f"pay items filled the {PAY_ITEM_PAGE_SWEEP}-page sweep — "
            "raise PAY_ITEM_PAGE_SWEEP, data may be truncated")

    total = {}
    scanned = kept = 0
    for p, (h, s, k) in results.items():
        scanned += s
        kept += k
        for eid, v in h.items():
            total[eid] = total.get(eid, 0.0) + v
    log(f"[3/4] pay items scanned {scanned}, matched Working records {kept} "
        f"(through page {last})")
    return total


def fetch_names():
    names, page = {}, 1
    while True:
        r = get(f"{API}/settings/v2/tenant/{TENANT}/technicians"
                f"?pageSize=300&page={page}")
        for t in r.get("data", []):
            names[t["id"]] = (t.get("name") or "").strip()
        if not r.get("hasMore"):
            break
        page += 1
    log(f"[4/4] technicians resolved: {len(names)}")
    return names


def build_data(revenue, job_ids, hours, names):
    main, low = [], []
    for tid, rev in revenue.items():
        name = names.get(tid)
        # "Internal Notes" is a placeholder account, not a person.
        if not name or name == "Internal Notes":
            continue
        h = round(hours.get(tid, 0.0), 2)
        n = len(job_ids.get(tid, ()))
        row = {
            "name": name,
            "jobs": n,
            "hours": h,
            "revenue": round(rev, 2),
            # Legitimately null when a tech has splits but clocked no Working
            # time against those specific jobs. Sorts last; do not synthesize.
            "rate": (round(rev / h, 2) if h > 0 else None),
        }
        (main if n >= MIN_JOBS_FOR_MAIN else low).append(row)

    order = lambda x: (x["rate"] is not None, x["rate"] or 0)
    main.sort(key=order, reverse=True)
    low.sort(key=order, reverse=True)

    tj = sum(x["jobs"] for x in main)
    th = round(sum(x["hours"] for x in main), 2)
    tr = round(sum(x["revenue"] for x in main), 2)
    now = datetime.now(OFFICE_TZ)
    return {
        "period": "Year to Date — Jan 1 through "
                  + TODAY.strftime("%b %d, %Y").replace(" 0", " "),
        "updated": now.strftime("%b %d, %Y").replace(" 0", " ") + ", "
                   + now.strftime("%I:%M %p").lstrip("0"),
        "main": main,
        "low": low,
        "totals": {
            "jobs": tj, "hours": th, "revenue": tr,
            "blended": (round(tr / th, 2) if th else 0),
        },
    }


# ---------------------------------------------------------------- render

def extract_photos(src):
    """Return the exact `const PHOTOS = {...};` text.

    Must brace-match rather than split on ';' — base64 data URLs contain a
    semicolon ("data:image/png;base64,"), so a naive split truncates after
    ~39 characters and any byte-comparison built on it is meaningless.
    """
    pi = src.index("const PHOTOS")
    k = src.index("{", pi)
    depth = 0
    while True:
        if src[k] == "{":
            depth += 1
        elif src[k] == "}":
            depth -= 1
            if depth == 0:
                break
        k += 1
    end = src.index(";", k) + 1
    return src[pi:end], end


def render(path, data):
    src = open(path, encoding="utf-8").read()

    head = src[:src.index("const DATA")]
    photos, end = extract_photos(src)
    tail = src[end:]

    if not head.rstrip().endswith("<script>"):
        raise RuntimeError("HEAD does not end at <script> — refusing to write")
    if not tail.lstrip().startswith("function money"):
        raise RuntimeError("TAIL does not start at function money — refusing")
    if len(photos) < 20:
        raise RuntimeError("PHOTOS block looks empty — refusing to write")

    def rows(items):
        return ",\n".join(
            '    {"name":%s,"jobs":%d,"hours":%s,"revenue":%s,"rate":%s}' % (
                json.dumps(x["name"]), x["jobs"], repr(x["hours"]),
                repr(x["revenue"]),
                "null" if x["rate"] is None else repr(x["rate"]))
            for x in items)

    t = data["totals"]
    block = (
        "const DATA = {\n"
        "  period: %s,\n"
        "  updated: %s,\n"
        "  main: [\n%s\n  ],\n"
        "  low: [\n%s\n  ],\n"
        "  totals: {jobs:%d, hours:%s, revenue:%s, blended:%s}\n};\n"
    ) % (json.dumps(data["period"]), json.dumps(data["updated"]),
         rows(data["main"]), rows(data["low"]),
         t["jobs"], repr(t["hours"]), repr(t["revenue"]), repr(t["blended"]))

    open(path, "w", encoding="utf-8").write(head + block + "\n" + photos + tail)
    return len(photos)


def verify(data, photos_before, photos_after):
    t = data["totals"]
    checks = [
        # Compares the full brace-matched block text, not a length or prefix.
        ("PHOTOS preserved byte-for-byte", photos_before == photos_after),
        ("main list non-trivial", len(data["main"]) >= MIN_MAIN_TECHS),
        ("job count above sanity floor", t["jobs"] >= MIN_TOTAL_JOBS),
        ("main jobs sum to totals",
         sum(x["jobs"] for x in data["main"]) == t["jobs"]),
        ("blended matches revenue/hours",
         abs(t["revenue"] / t["hours"] - t["blended"]) < 0.01 if t["hours"] else False),
        ("sorted by rate descending", all(
            (data["main"][i - 1]["rate"] or -1) >= (data["main"][i]["rate"] or -1)
            for i in range(1, len(data["main"])))),
    ]
    bad = [n for n, ok in checks if not ok]
    for name, ok in checks:
        log(f"    {'PASS' if ok else 'FAIL'}  {name}")
    if bad:
        raise RuntimeError("verification failed: " + "; ".join(bad))


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "index.html"
    photos_before, _ = extract_photos(open(path, encoding="utf-8").read())

    jobs = fetch_jobs()
    if not jobs:
        raise RuntimeError("zero matched jobs — refusing to publish")
    revenue, job_ids = fetch_splits(jobs)
    hours = fetch_hours(jobs)
    names = fetch_names()

    data = build_data(revenue, job_ids, hours, names)
    log(f"    totals: {json.dumps(data['totals'])}")

    render(path, data)
    photos_after, _ = extract_photos(open(path, encoding="utf-8").read())
    verify(data, photos_before, photos_after)
    log(f"done — PHOTOS block {len(photos_after)} bytes, unchanged")


if __name__ == "__main__":
    main()
