#!/usr/bin/env python3
"""
carry_dashboard.py — live dashboard for the PAPER carry engine (read-only, no deps)
===================================================================================
Reads carry_state.json / carry_equity.csv / carry_log.csv (written by carry_paper.py)
and serves an auto-refreshing web page. Stdlib only.

Run on the same box as carry_paper.py:
  python3 carry_dashboard.py --port 8090
Then open http://<server-ip>:8090  (open the port in iptables first).
"""
import argparse, json, os, csv, html
from http.server import BaseHTTPRequestHandler, HTTPServer

STATE = "carry_state.json"
EQUITY = "carry_equity.csv"
LOG = "carry_log.csv"
REFRESH = 30


def read_state():
    try:
        return json.load(open(STATE))
    except Exception:
        return {}


def read_equity(n=400):
    if not os.path.exists(EQUITY):
        return []
    rows = list(csv.DictReader(open(EQUITY)))
    return rows[-n:]


def read_log(n=25):
    if not os.path.exists(LOG):
        return []
    rows = list(csv.DictReader(open(LOG)))
    return rows[-n:][::-1]


def sparkline(eq, key, w=720, h=140, color="#00e676"):
    pts = [float(r[key]) for r in eq if r.get(key) not in (None, "")]
    if len(pts) < 2:
        return f'<svg width="{w}" height="{h}"></svg>'
    lo, hi = min(pts), max(pts)
    rng = (hi - lo) or 1
    zero_y = h - (0 - lo) / rng * (h - 16) - 8 if lo < 0 < hi else None
    coords = []
    for i, v in enumerate(pts):
        x = i / (len(pts) - 1) * w
        y = h - (v - lo) / rng * (h - 16) - 8
        coords.append(f"{x:.1f},{y:.1f}")
    zero_line = f'<line x1="0" y1="{zero_y:.1f}" x2="{w}" y2="{zero_y:.1f}" stroke="#452885" stroke-dasharray="3 3"/>' if zero_y is not None else ""
    last = pts[-1]
    col = "#00e676" if last >= 0 else "#ff4466"
    return (f'<svg width="{w}" height="{h}" style="width:100%">{zero_line}'
            f'<polyline fill="none" stroke="{col}" stroke-width="2" points="{" ".join(coords)}"/></svg>')


def card(label, value, sub="", color="#f0e8ff"):
    return (f'<div class="card"><div class="lbl">{label}</div>'
            f'<div class="val" style="color:{color}">{value}</div>'
            f'<div class="sub">{sub}</div></div>')


def page():
    st = read_state()
    eq = read_equity()
    log = read_log()
    pos = st.get("positions", {})
    fund = st.get("cum_funding", 0.0)
    basis = st.get("cum_basis", 0.0)
    unreal = st.get("unreal_basis", 0.0)
    fees = st.get("cum_fees", 0.0)
    net = st.get("net", fund + basis + unreal - fees)
    ticks = st.get("ticks", 0)
    last = st.get("last_update", "—")
    ncol = "#00e676" if net >= 0 else "#ff4466"
    bcol = "#00e676" if (basis + unreal) >= 0 else "#ff4466"

    rows = ""
    for s in sorted(pos):
        p = pos[s]
        rows += (f"<tr><td>{html.escape(s)}</td><td>{p.get('entry_basis_bp',0):+.2f}</td>"
                 f"<td class='g'>{p.get('funding_collected',0):+.4f}</td>"
                 f"<td>{p.get('notional',0):.0f}</td></tr>")
    if not rows:
        rows = "<tr><td colspan='4' class='dim'>no open positions (carry dormant — funding below entry threshold)</td></tr>"

    ev = ""
    for r in log:
        c = {"OPEN": "#00f8ff", "CLOSE": "#ffd740", "FUND": "#00e676"}.get(r.get("event", ""), "#7858b8")
        ev += (f"<tr><td class='dim'>{html.escape(r.get('ts','')[5:16])}</td>"
               f"<td style='color:{c}'>{html.escape(r.get('event',''))}</td>"
               f"<td>{html.escape(r.get('sym',''))}</td>"
               f"<td>{html.escape(r.get('funding_bp',''))}</td>"
               f"<td>{html.escape(r.get('basis_bp',''))}</td>"
               f"<td class='dim'>{html.escape(r.get('detail',''))}</td></tr>")

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<meta http-equiv="refresh" content="{REFRESH}">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Carry Paper Monitor</title>
<style>
:root{{--bg:#07050f;--bg2:#0e0a1a;--bg3:#130f22;--bd:#301d55;--bd2:#452885;--tx:#f0e8ff;--dim:#7858b8;--cy:#00f8ff}}
*{{box-sizing:border-box;margin:0;padding:0}}body{{background:var(--bg);color:var(--tx);font-family:'IBM Plex Mono',ui-monospace,monospace;padding:18px}}
h1{{font-size:16px;font-weight:700}}h1 span{{color:var(--cy)}}
.tag{{display:inline-block;background:#3a1d6e;color:#cba6ff;font-size:10px;padding:2px 7px;border-radius:4px;margin-left:8px;vertical-align:middle}}
.bar{{color:var(--dim);font-size:11px;margin:4px 0 16px}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-bottom:18px}}
.card{{background:var(--bg2);border:1px solid var(--bd);border-radius:8px;padding:12px 14px}}
.lbl{{color:var(--dim);font-size:10px;text-transform:uppercase;letter-spacing:.05em}}
.val{{font-size:22px;font-weight:600;margin:4px 0;font-variant-numeric:tabular-nums}}
.sub{{color:var(--dim);font-size:10px}}
.panel{{background:var(--bg2);border:1px solid var(--bd);border-radius:8px;padding:14px;margin-bottom:16px}}
.panel h2{{font-size:11px;color:var(--dim);text-transform:uppercase;letter-spacing:.05em;margin-bottom:10px}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
th{{text-align:left;color:var(--dim);font-weight:600;font-size:10px;text-transform:uppercase;padding:5px 8px;border-bottom:1px solid var(--bd)}}
td{{padding:6px 8px;border-bottom:1px solid rgba(48,29,85,.4);font-variant-numeric:tabular-nums}}
.g{{color:#00e676}}.dim{{color:var(--dim)}}
</style></head><body>
<h1>Carry <span>Paper</span> Monitor<span class="tag">PAPER — NO REAL ORDERS · STAGE</span></h1>
<div class="bar">tick {ticks} · last update {html.escape(str(last))} · auto-refresh {REFRESH}s</div>
<div class="cards">
{card("NET (paper)", f"{net:+.4f}", "funding + basis − fees", ncol)}
{card("Funding collected", f"{fund:+.4f}", "the harvest", "#00e676")}
{card("Basis P&amp;L", f"{basis+unreal:+.4f}", f"realized {basis:+.3f} · unreal {unreal:+.3f}", bcol)}
{card("Fees", f"{fees:.4f}", "maker, both legs", "#ff8866")}
{card("Open positions", f"{len(pos)}", "short perp + long spot", "#00f8ff")}
</div>
<div class="panel"><h2>Equity — net (paper)</h2>{sparkline(eq,'net')}</div>
<div class="panel"><h2>Funding (green-ish) vs cumulative — watch if basis swings as hard as funding</h2>{sparkline(eq,'cum_funding')}</div>
<div class="panel"><h2>Open book</h2>
<table><tr><th>symbol</th><th>entry basis (bp)</th><th>funding collected</th><th>notional</th></tr>{rows}</table></div>
<div class="panel"><h2>Recent events</h2>
<table><tr><th>time</th><th>event</th><th>sym</th><th>fund bp</th><th>basis bp</th><th>detail</th></tr>{ev}</table></div>
<div class="bar">PAPER simulation. Returns are optimistic (assumes maker fills, no execution-in-crisis). Not financial advice.</div>
</body></html>"""


class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/", "/index.html"):
            self.send_response(404); self.end_headers(); return
        body = page().encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8090)
    ap.add_argument("--host", default="0.0.0.0")
    args = ap.parse_args()
    print(f"carry dashboard on http://{args.host}:{args.port}  (reading {STATE}/{EQUITY}/{LOG})")
    HTTPServer((args.host, args.port), H).serve_forever()


if __name__ == "__main__":
    main()
