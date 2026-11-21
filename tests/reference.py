#!/usr/bin/env python3
"""Slow independent oracle for small generated datasets."""
import datetime as dt
import json
from collections import defaultdict
from pathlib import Path
import re
import sys

DAY = 86_400_000
WEEK = 7 * DAY
ISO = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:Z|[+-][0-9]{2}:[0-9]{2})$")

def timestamp(value):
    if value and all("0" <= c <= "9" for c in value):
        return int(value)
    if not ISO.fullmatch(value):
        return None
    if len(value) == 25 and (int(value[20:22]) > 23 or int(value[23:25]) > 59):
        return None
    try:
        date = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        epoch = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
        delta = date - epoch
        return (delta.days * 86400 + delta.seconds) * 1000
    except (ValueError, OverflowError):
        return None

def read(path, count):
    with open(path, encoding="utf-8", newline="") as stream:
        next(stream, None)
        for line in stream:
            line = line.removesuffix("\n")
            values = line.split(",")
            if len(values) != count:
                continue
            fields = []
            for value in values:
                if value.startswith('"') and value.endswith('"') and len(value) >= 2:
                    value = value[1:-1]
                if '"' in value:
                    break
                fields.append(value)
            if len(fields) == count:
                yield fields

def all_rows(path, count):
    for file in sorted(path.glob("*.csv")):
        yield from read(file, count)

def compute(data):
    manifest = json.loads((data / "manifest.json").read_text())
    start = timestamp(manifest["window_start"])
    end = timestamp(manifest["window_end"])
    graph = defaultdict(set)
    for a, b in all_rows(data / "links", 2):
        if a and b:
            graph[a].add(b)
            graph[b].add(a)
    canon = {}
    for name in graph:
        if name in canon:
            continue
        seen, stack = set(), [name]
        while stack:
            cur = stack.pop()
            if cur not in seen:
                seen.add(cur)
                stack.extend(graph[cur] - seen)
        for cur in seen:
            canon[cur] = min(seen)
    products = dict(read(data / "products.csv", 2))
    campaigns = dict(read(data / "campaigns.csv", 2))
    winners = {}
    for row in all_rows(data / "events", 7):
        if not row[0]:
            continue
        ing = timestamp(row[3])
        if row[0] not in winners or ing < winners[row[0]][0]:
            winners[row[0]] = (ing, row)
    by_person = defaultdict(list)
    for ing, row in winners.values():
        _, user, raw, _, kind, product, price = row
        ts = timestamp(raw)
        if not user or not product or kind not in ("view", "cart", "purchase") or ts is None or not start <= ts < end or ing - ts > DAY:
            continue
        value = 0
        if kind == "purchase":
            if not price or not all("0" <= c <= "9" for c in price) or not 1 <= int(price) <= 10_000_000:
                continue
            value = int(price)
        by_person[canon.get(user, user)].append((ts, value, products.get(product, "unknown")))
    by_click_id = {}
    for row in all_rows(data / "clicks", 4):
        if row[0]:
            by_click_id.setdefault(row[0], row)
    clicks = defaultdict(list)
    for cid, user, campaign, raw in by_click_id.values():
        ts = timestamp(raw)
        if user and campaign in campaigns and ts is not None and start - WEEK <= ts < end:
            clicks[canon.get(user, user)].append((ts, cid, campaign))
    users = {}
    daily_campaign = defaultdict(lambda: [0, 0, 0, 0])
    for person, events in by_person.items():
        events.sort()
        sessions = []
        for event in events:
            if not sessions or event[0] - sessions[-1][-1][0] > 1_800_000:
                sessions.append([])
            sessions[-1].append(event)
        daily = defaultdict(lambda: [0, 0, 0, defaultdict(int)])
        for session in sessions:
            st = session[0][0]
            day = dt.date(1970, 1, 1) + dt.timedelta(days=st // DAY)
            sums = daily[day]
            sums[0] += 1
            sums[1] += len(session)
            revenue = sum(e[1] for e in session)
            sums[2] += revenue
            for _, price, category in session:
                if price:
                    sums[3][category] += price
            eligible = [c for c in clicks[person] if st-WEEK <= c[0] <= st]
            chosen = min(eligible, key=lambda c: (-c[0], c[1]))[2] if eligible else "organic"
            row = daily_campaign[(chosen, day)]
            row[0] += 1
            row[1] += int(revenue > 0)
            row[2] += revenue
            for campaign in {c[2] for c in eligible}:
                daily_campaign[(campaign, day)][3] += 1
        for day, (sessions, count, revenue, cats) in daily.items():
            top = min(cats, key=lambda c: (-cats[c], c)) if cats else None
            users[(person, day)] = (sessions, count, revenue, top)
    campaign_rows = {(campaign, day): (campaigns.get(campaign, "organic"), *v)
                     for (campaign, day), v in daily_campaign.items()}
    return users, campaign_rows

if __name__ == "__main__":
    import polars as pl
    expected_users, expected_campaigns = compute(Path(sys.argv[1]))
    out = Path(sys.argv[2])
    users = pl.read_parquet(out / "user_daily" / "*.parquet").to_dicts()
    camps = pl.read_parquet(out / "campaign_daily" / "*.parquet").to_dicts()
    actual_users = {(r["user_id"], r["day"]): (r["sessions"], r["events"], r["revenue_cents"], r["top_category"]) for r in users}
    actual_campaigns = {(r["campaign_id"], r["day"]): (r["channel"], r["sessions"], r["purchasing_sessions"], r["revenue_cents"], r["assisted_sessions"]) for r in camps}
    assert len(actual_users) == len(users) and actual_users == expected_users, "user_daily mismatch"
    assert len(actual_campaigns) == len(camps) and actual_campaigns == expected_campaigns, "campaign_daily mismatch"
    print(f"reference MATCH: {len(users)} user rows, {len(camps)} campaign rows")
