#!/usr/bin/env python3
"""
얼리버드 대시보드 데이터 갱신 스크립트
index.html 내 JS 데이터 배열을 BigQuery 최신 데이터로 교체 후 git push.

사전 설정:
  pip3 install google-cloud-bigquery
  gcloud auth application-default login
"""

import os
import re
import json
import subprocess
import sys
from datetime import datetime, timedelta

try:
    from google.cloud import bigquery
except ImportError:
    print("pip3 install google-cloud-bigquery")
    sys.exit(1)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HTML_PATH = os.path.join(BASE_DIR, "index.html")
PROJECT_ID = "socar-data"

# ── 쿼리 ──

Q_WEEKLY = """
SELECT
  EXTRACT(ISOWEEK FROM DATE(return_at_kst)) AS week,
  MIN(DATE(return_at_kst)) AS week_start,
  CASE
    WHEN coupon_policy_name LIKE '%미리예약%' THEN 'eb'
    WHEN coupon_policy_division IN ('SUPER_DEAL','SUPER_DEAL_CLOSING_SALE') THEN 'sd'
    WHEN coupon_policy_name LIKE '%당일최저가%' THEN 'sd'
    WHEN coupon_policy_name LIKE '%당일시작%' THEN 'sd'
    ELSE 'other'
  END AS ctype,
  COUNT(DISTINCT reservation_id) AS cnt,
  ROUND(SUM(revenue)/1e8, 2) AS rev,
  ROUND(SUM(contribution_margin)/1e8, 2) AS cm,
  ROUND(SUM(profit)/1e8, 2) AS pft
FROM `socar-data.socar_biz_profit.profit_socar_reservation`
WHERE coupon_policy_id IS NOT NULL
  AND EXTRACT(ISOYEAR FROM DATE(return_at_kst)) = 2026
  AND DATE(return_at_kst) <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND (coupon_policy_name LIKE '%미리예약%'
    OR coupon_policy_name LIKE '%당일최저가%'
    OR coupon_policy_name LIKE '%당일시작%'
    OR coupon_policy_division IN ('SUPER_DEAL','SUPER_DEAL_CLOSING_SALE'))
GROUP BY 1, 3
ORDER BY 3, 1
"""

Q_NEW_COUPON = """
SELECT
  DATE(r.created_at,'Asia/Seoul') AS create_date,
  IFNULL((SELECT p.id FROM `socar-data.tianjin_replica.coupon_info` cou,
    `socar-data.tianjin_replica.coupon_policy` p
    WHERE cou.reservation_id = r.id AND cou.policy_id = p.id), 0) AS pid,
  (SELECT p.name FROM `socar-data.tianjin_replica.coupon_info` cou,
    `socar-data.tianjin_replica.coupon_policy` p
    WHERE cou.reservation_id = r.id AND cou.policy_id = p.id) AS coupon_name,
  COUNT(DISTINCT r.id) AS cnt,
  ROUND(SUM(
    IFNULL((SELECT SUM(c.amount) FROM `socar-data.tianjin_replica.charged_info` c
      WHERE c.reservation_id = r.id AND c.charge_type IN ('rent','refund'))/1.1, 0)
    + IFNULL((SELECT SUM(c.amount) FROM `socar-data.tianjin_replica.charged_info` c
      WHERE c.reservation_id = r.id AND c.charge_type IN ('protection_fee'))/1.1, 0)
    - IFNULL((SELECT SUM(p2.amount) FROM `socar-data.tianjin_replica.paid_info` p2
      WHERE r.id = p2.reservation_id AND p2.paid_type IN ('coupon','bonus','coupon_oneway',
        'coupon_refund','oil_discount','promotion','promotion_refund',
        'coupon_oneway_refund','coupon_pf','coupon_pf_refund','oil_discount_refund'))/1.1, 0)
  )/1e6, 2) AS net_rev_m
FROM `socar-data.tianjin_replica.reservation_info` r
WHERE r.way IN ('round','z2d_oneway','d2d_oneway','d2d_round','d2d_rev')
  AND r.member_imaginary IN (0,9)
  AND r.state IN (1,2,3)
  AND DATE(r.created_at,'Asia/Seoul') >= DATE '2026-03-26'
  AND r.id IN (
    SELECT cou.reservation_id FROM `socar-data.tianjin_replica.coupon_info` cou
    JOIN `socar-data.tianjin_replica.coupon_policy` p ON cou.policy_id = p.id
    WHERE p.id IN (16264,16265,16266,16267,16268,16269,16270,16271)
  )
GROUP BY 1, 2, 3
ORDER BY 2, 1
"""


def run_query(client, sql):
    return [dict(row) for row in client.query(sql).result()]


def build_weekly_arrays(rows):
    """주간 데이터를 JS 배열 문자열로 변환"""
    eb, sd = {}, {}
    for r in rows:
        w = r["week"]
        d = {"cnt": r["cnt"], "rev": float(r["rev"]), "cm": float(r["cm"]), "pft": float(r["pft"]),
             "ws": str(r["week_start"])}
        if r["ctype"] == "eb":
            eb[w] = d
        elif r["ctype"] == "sd":
            sd[w] = d

    all_weeks = sorted(set(list(eb.keys()) + list(sd.keys())))
    labels = []
    eb_cnt, eb_rev, eb_cm, eb_pft = [], [], [], []
    sd_cnt, sd_rev, sd_cm, sd_pft = [], [], [], []

    for w in all_weeks:
        e = eb.get(w, {"cnt": 0, "rev": 0, "cm": 0, "pft": 0, "ws": ""})
        s = sd.get(w, {"cnt": 0, "rev": 0, "cm": 0, "pft": 0, "ws": ""})
        ws = e.get("ws") or s.get("ws", "")
        label = f"W{w}\\n{ws[5:]}" if ws else f"W{w}"
        labels.append(f"'{label}'")
        eb_cnt.append(str(e["cnt"]))
        eb_rev.append(str(e["rev"]))
        eb_cm.append(str(e["cm"]))
        eb_pft.append(str(e["pft"]))
        sd_cnt.append(str(s["cnt"]))
        sd_rev.append(str(s["rev"]))
        sd_cm.append(str(s["cm"]))
        sd_pft.append(str(s["pft"]))

    w_str = f"[{','.join(labels)}]"
    eb_str = f"{{cnt:[{','.join(eb_cnt)}],rev:[{','.join(eb_rev)}],cm:[{','.join(eb_cm)}],pft:[{','.join(eb_pft)}]}}"
    sd_str = f"{{cnt:[{','.join(sd_cnt)}],rev:[{','.join(sd_rev)}],cm:[{','.join(sd_cm)}],pft:[{','.join(sd_pft)}]}}"
    return w_str, eb_str, sd_str


def build_nc_days(rows):
    """신규 쿠폰 일별 데이터를 ncDays JS 배열로 변환"""
    pids = [16264, 16265, 16266, 16267, 16268, 16269, 16270, 16271]
    by_date = {}
    for r in rows:
        dt = str(r["create_date"])
        pid = r["pid"]
        if dt not in by_date:
            by_date[dt] = {f"c{p}": 0 for p in pids}
            by_date[dt].update({f"r{p}": 0 for p in pids})
        by_date[dt][f"c{pid}"] = r["cnt"]
        by_date[dt][f"r{pid}"] = float(r["net_rev_m"])

    lines = []
    for dt in sorted(by_date.keys()):
        d = by_date[dt]
        short = dt[5:].replace("-", "/")
        parts = [f"date:'{short}'"]
        for p in pids:
            parts.append(f"c{p}:{d.get(f'c{p}', 0)}")
        for p in pids:
            parts.append(f"r{p}:{d.get(f'r{p}', 0)}")
        lines.append("  {" + ",".join(parts) + "}")

    return "[\n" + ",\n".join(lines) + ",\n]"


def update_html(html, w_str, eb_str, sd_str, nc_days_str, yesterday):
    """HTML 내 JS 데이터 교체"""
    now = datetime.now().strftime("%Y.%m.%d %H:%M")

    # Weekly labels
    html = re.sub(r"const W=\[.*?\];", f"const W={w_str};", html)
    # Earlybird data
    html = re.sub(r"const eb=\{cnt:\[.*?\]\};", f"const eb={eb_str};", html)
    # Superdeal data
    html = re.sub(r"const sd=\{cnt:\[.*?\]\};", f"const sd={sd_str};", html)
    # ncDays
    html = re.sub(
        r"const ncDays = \[.*?\];",
        f"const ncDays = {nc_days_str};",
        html,
        flags=re.DOTALL,
    )
    # Date
    html = re.sub(
        r"데이터 기준: 2026\.01\.01 ~ [\d.]+ \(반납 기준\)",
        f"데이터 기준: 2026.01.01 ~ {yesterday} (반납 기준)",
        html,
    )
    html = re.sub(r"마지막 갱신: [\d. :]+", f"마지막 갱신: {now}", html)

    return html


def git_push():
    """변경사항 커밋 및 푸시"""
    os.chdir(BASE_DIR)
    subprocess.run(["git", "add", "index.html"], check=True)
    result = subprocess.run(["git", "diff", "--cached", "--quiet"])
    if result.returncode == 0:
        print("  변경사항 없음, 스킵.")
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    subprocess.run(
        ["git", "commit", "-m", f"auto-refresh earlybird dashboard ({now})"],
        check=True,
    )
    subprocess.run(["git", "push", "origin", "main"], check=True)
    print("  git push 완료!")


def main():
    print(f"[{datetime.now()}] 얼리버드 대시보드 갱신 시작...")
    client = bigquery.Client(project=PROJECT_ID)
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y.%m.%d")

    print("  주간 데이터 쿼리...")
    weekly_rows = run_query(client, Q_WEEKLY)
    w_str, eb_str, sd_str = build_weekly_arrays(weekly_rows)

    print("  신규 쿠폰 데이터 쿼리...")
    nc_rows = run_query(client, Q_NEW_COUPON)
    nc_days_str = build_nc_days(nc_rows)

    print("  HTML 갱신...")
    with open(HTML_PATH, "r", encoding="utf-8") as f:
        html = f.read()

    html = update_html(html, w_str, eb_str, sd_str, nc_days_str, yesterday)

    with open(HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    print("  git push...")
    git_push()

    print(f"[{datetime.now()}] 완료!")


if __name__ == "__main__":
    main()
