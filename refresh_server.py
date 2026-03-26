#!/usr/bin/env python3
"""
얼리버드 대시보드 새로고침 로컬 서버
http://localhost:5555/refresh 호출 시 BigQuery → HTML 갱신 → git push

시작: python3 refresh_server.py
"""

import os
import re
import json
import subprocess
import sys
import threading
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler

try:
    from google.cloud import bigquery
except ImportError:
    print("pip3 install google-cloud-bigquery")
    sys.exit(1)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HTML_PATH = os.path.join(BASE_DIR, "index.html")
PROJECT_ID = "socar-data"
PORT = 5555

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
GROUP BY 1, 2
ORDER BY 2, 1
"""


def run_refresh():
    """BigQuery 쿼리 → HTML 갱신 → git push"""
    client = bigquery.Client(project=PROJECT_ID)
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y.%m.%d")
    now = datetime.now().strftime("%Y.%m.%d %H:%M")

    # 주간 데이터
    weekly_rows = [dict(row) for row in client.query(Q_WEEKLY).result()]
    eb, sd = {}, {}
    for r in weekly_rows:
        w = r["week"]
        d = {"cnt": r["cnt"], "rev": float(r["rev"]), "cm": float(r["cm"]), "pft": float(r["pft"]), "ws": str(r["week_start"])}
        if r["ctype"] == "eb":
            eb[w] = d
        elif r["ctype"] == "sd":
            sd[w] = d

    all_weeks = sorted(set(list(eb.keys()) + list(sd.keys())))
    labels, eb_cnt, eb_rev, eb_cm, eb_pft = [], [], [], [], []
    sd_cnt, sd_rev, sd_cm, sd_pft = [], [], [], []
    for w in all_weeks:
        e = eb.get(w, {"cnt": 0, "rev": 0, "cm": 0, "pft": 0, "ws": ""})
        s = sd.get(w, {"cnt": 0, "rev": 0, "cm": 0, "pft": 0, "ws": ""})
        ws = e.get("ws") or s.get("ws", "")
        labels.append(f"'W{w}\\n{ws[5:]}'")
        eb_cnt.append(str(e["cnt"])); eb_rev.append(str(e["rev"])); eb_cm.append(str(e["cm"])); eb_pft.append(str(e["pft"]))
        sd_cnt.append(str(s["cnt"])); sd_rev.append(str(s["rev"])); sd_cm.append(str(s["cm"])); sd_pft.append(str(s["pft"]))

    w_str = f"[{','.join(labels)}]"
    eb_str = f"{{cnt:[{','.join(eb_cnt)}],rev:[{','.join(eb_rev)}],cm:[{','.join(eb_cm)}],pft:[{','.join(eb_pft)}]}}"
    sd_str = f"{{cnt:[{','.join(sd_cnt)}],rev:[{','.join(sd_rev)}],cm:[{','.join(sd_cm)}],pft:[{','.join(sd_pft)}]}}"

    # 신규 쿠폰
    nc_rows = [dict(row) for row in client.query(Q_NEW_COUPON).result()]
    pids = [16264, 16265, 16266, 16267, 16268, 16269, 16270, 16271]
    by_date = {}
    for r in nc_rows:
        dt = str(r["create_date"])
        pid = r["pid"]
        if dt not in by_date:
            by_date[dt] = {}
        by_date[dt][f"c{pid}"] = r["cnt"]
        by_date[dt][f"r{pid}"] = float(r["net_rev_m"])

    nc_lines = []
    for dt in sorted(by_date.keys()):
        d = by_date[dt]
        short = dt[5:].replace("-", "/")
        parts = [f"date:'{short}'"]
        for p in pids:
            parts.append(f"c{p}:{d.get(f'c{p}', 0)}")
        for p in pids:
            parts.append(f"r{p}:{d.get(f'r{p}', 0)}")
        nc_lines.append("  {" + ",".join(parts) + "}")
    nc_str = "[\n" + ",\n".join(nc_lines) + ",\n]"

    # HTML 갱신
    with open(HTML_PATH, "r", encoding="utf-8") as f:
        html = f.read()

    html = re.sub(r"const W=\[.*?\];", f"const W={w_str};", html)
    html = re.sub(r"const eb=\{cnt:\[.*?\]\};", f"const eb={eb_str};", html)
    html = re.sub(r"const sd=\{cnt:\[.*?\]\};", f"const sd={sd_str};", html)
    html = re.sub(r"const ncDays = \[.*?\];", f"const ncDays = {nc_str};", html, flags=re.DOTALL)
    html = re.sub(r"데이터 기준: 2026\.01\.01 ~ [\d.]+ \(반납 기준\)", f"데이터 기준: 2026.01.01 ~ {yesterday} (반납 기준)", html)
    html = re.sub(r"마지막 갱신: [\d. :]+", f"마지막 갱신: {now}", html)

    with open(HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    # git push
    os.chdir(BASE_DIR)
    subprocess.run(["git", "add", "index.html"], check=True)
    result = subprocess.run(["git", "diff", "--cached", "--quiet"])
    if result.returncode != 0:
        subprocess.run(["git", "commit", "-m", f"auto-refresh ({now})"], check=True)
        subprocess.run(["git", "push", "origin", "main"], check=True)

    return {"status": "ok", "updated": now, "weeks": len(all_weeks), "nc_days": len(by_date)}


class RefreshHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/refresh":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                result = run_refresh()
                self.wfile.write(json.dumps(result).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"status": "error", "message": str(e)}).encode())
        elif self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {args[0]}")


if __name__ == "__main__":
    print(f"새로고침 서버 시작: http://localhost:{PORT}")
    print(f"  새로고침: http://localhost:{PORT}/refresh")
    print(f"  상태확인: http://localhost:{PORT}/health")
    HTTPServer(("127.0.0.1", PORT), RefreshHandler).serve_forever()
