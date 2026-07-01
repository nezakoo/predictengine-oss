#!/usr/bin/env python3
"""PredictEngine Comprehensive Correlation Analysis — called by analyze.sh"""
import json,sys,os
from datetime import datetime,timezone,timedelta
from collections import defaultdict
from pathlib import Path

def load(p):
    with open(p) as f: return json.load(f)

def parse_ts(t):
    for k in ('ts_epoch','ts'):
        try:
            v=float(t.get(k) or 0)
            if v>1e9: return datetime.fromtimestamp(v,tz=timezone.utc).replace(tzinfo=None)
        except: pass
    for k in ('open_time','time'):
        try:
            s=t.get(k,'')
            if s:
                for fmt in ('%Y-%m-%d %H:%M:%S','%H:%M:%S'):
                    try: return datetime.strptime(s,fmt)
                    except: pass
        except: pass
    return None

def flt(v,d=0.0):
    try: return float(v) if v not in (None,'','nan') else d
    except: return d

def pct(v,n=3): return f"{flt(v):+.{n}f}%"
def vc(v,t=0): return 'pos' if flt(v)>t else ('neg' if flt(v)<t else 'neu')

# ── NORMALIZE ─────────────────────────────────────────────────────
def build_trades(d):
    rows=[]
    recon = d.get('reconciliation') or {}
    for t in recon.get('unmatched_engine',[]):
        dt=parse_ts(t)
        if not dt: continue
        rows.append({'sym':t.get('sym',''),'dir':t.get('dir',''),'strategy':t.get('strategy','?'),
            'ts_dt':dt,'entry_px':flt(t.get('entry_px')),'exit_px':flt(t.get('exit_px')),
            'net_pct':flt(t.get('net_exit')),'outcome':t.get('outcome','open'),'reason':t.get('reason',''),
            'is_live':t.get('is_live','0')=='1','vpin':flt(t.get('vpin_entry')),'score':flt(t.get('score')),
            'conf':flt(t.get('conf')),'atr':flt(t.get('atr_entry')),'dur_sec':flt(t.get('dur_sec')),
            'max_dp':flt(t.get('max_dp')),'min_dp':flt(t.get('min_dp')),
            'snap30':flt(t.get('snap30')),'snap60':flt(t.get('snap60')),
            'dyn_sl':flt(t.get('dyn_sl')),'dyn_tp':flt(t.get('dyn_tp')),
            'bnb_pnl':0.0,'bnb_commission':0.0,'entry_slip':0.0,'source':'engine'})
    for m in (d.get('reconciliation') or {}).get('matched',[]):
        try: dt=datetime.strptime(m.get('open_time',''),'%Y-%m-%d %H:%M:%S')
        except: dt=datetime.now()
        net=flt(m.get('bnb_pnl_pct'))
        rows.append({'sym':m.get('sym',''),'dir':m.get('dir',''),'strategy':m.get('strategy','matched'),
            'ts_dt':dt,'entry_px':flt(m.get('eng_entry') or m.get('bnb_entry')),
            'exit_px':flt(m.get('bnb_exit')),'net_pct':net,
            'outcome':'win' if net>0 else ('lose' if net<0 else 'flat'),
            'reason':m.get('eng_reason',''),'is_live':True,'vpin':0.0,'score':0.0,'conf':0.0,
            'atr':0.0,'dur_sec':flt(m.get('dur_sec')),'max_dp':0.0,'min_dp':0.0,
            'snap30':0.0,'snap60':0.0,'dyn_sl':0.0,'dyn_tp':0.0,
            'bnb_pnl':flt(m.get('bnb_pnl_usdt')),'bnb_commission':flt(m.get('bnb_commission')),
            'entry_slip':flt(m.get('entry_slip_pct')),'source':'matched'})
    rows.sort(key=lambda t:t['ts_dt'])
    return rows

# ── CASCADES ──────────────────────────────────────────────────────
def find_cascades(trades,window=90,min_n=3):
    used=set(); result=[]
    for i,t in enumerate(trades):
        if i in used: continue
        cluster=[t]; idx=[i]
        for j in range(i+1,len(trades)):
            t2=trades[j]
            if (t2['ts_dt']-t['ts_dt']).total_seconds()>window: break
            if t2['sym']!=t['sym'] and t2['dir']==t['dir']:
                cluster.append(t2); idx.append(j)
        if len(cluster)<min_n: continue
        for x in idx: used.add(x)
        wins=[x for x in cluster if x['outcome']=='win']
        losses=[x for x in cluster if x['outcome']=='lose']
        nets=[x['net_pct'] for x in cluster if x['outcome'] in ('win','lose')]
        result.append({'ts':t['ts_dt'].strftime('%H:%M:%S'),'dir':t['dir'],
            'coins':[x['sym'] for x in cluster],'strategies':sorted(set(x['strategy'] for x in cluster)),
            'total':len(cluster),'wins':len(wins),'losses':len(losses),
            'wr':round(len(wins)/len(cluster)*100) if cluster else 0,
            'avg_net':round(sum(nets)/len(nets),4) if nets else 0,
            'live_count':sum(1 for x in cluster if x['is_live']),'_trades':cluster})
    return result

# ── CONFLICTS ─────────────────────────────────────────────────────
def find_conflicts(trades,window=300):
    by_sym=defaultdict(list)
    for t in trades: by_sym[t['sym']].append(t)
    agree=[];oppose=[];reenter=[]
    for sym,st in by_sym.items():
        st.sort(key=lambda x:x['ts_dt'])
        for i,t1 in enumerate(st):
            for t2 in st[i+1:]:
                dt=(t2['ts_dt']-t1['ts_dt']).total_seconds()
                if dt>window: break
                if t1['strategy']==t2['strategy'] or dt<=0: continue
                base={'sym':sym,'dt_sec':round(dt),'ts':t1['ts_dt'].strftime('%H:%M:%S'),
                    'strat1':t1['strategy'],'dir1':t1['dir'],'out1':t1['outcome'],'net1':t1['net_pct'],'live1':t1['is_live'],
                    'strat2':t2['strategy'],'dir2':t2['dir'],'out2':t2['outcome'],'net2':t2['net_pct'],'live2':t2['is_live']}
                if t1['dir']==t2['dir']: agree.append({**base,'type':'agree'})
                else: oppose.append({**base,'type':'oppose'})
                if t1['outcome'] in ('win','lose','flat') and dt<=120:
                    reenter.append({**base,'type':'reenter',
                        'closer_won':t1['outcome']=='win','same_dir':t1['dir']==t2['dir'],
                        'flag':t1['outcome']=='win' and t2['outcome']=='lose'})
    return agree,oppose,reenter

# ── PER COIN ─────────────────────────────────────────────────────
def coin_analysis(trades,income_by_sym):
    by_sym=defaultdict(list)
    for t in trades: by_sym[t['sym']].append(t)
    result={}
    for sym,st in by_sym.items():
        decided=[t for t in st if t['outcome'] in ('win','lose')]
        wins=[t for t in decided if t['outcome']=='win']
        live=[t for t in st if t['is_live']]
        L=[t for t in decided if t['dir']=='long']
        S=[t for t in decided if t['dir']=='short']
        inc=income_by_sym.get(sym,{})
        result[sym]={'total':len(st),'live':len(live),'sim':len(st)-len(live),
            'wins':len(wins),'losses':len(decided)-len(wins),
            'wr':round(len(wins)/len(decided)*100) if decided else None,
            'long_wr':round(sum(1 for t in L if t['outcome']=='win')/len(L)*100) if L else None,
            'short_wr':round(sum(1 for t in S if t['outcome']=='win')/len(S)*100) if S else None,
            'sim_net':round(sum(t['net_pct'] for t in st),4),
            'bnb_pnl':round(inc.get('REALIZED_PNL',0),4),
            'bnb_comm':round(inc.get('COMMISSION',0),5),
            'bnb_fund':round(inc.get('FUNDING_FEE',0),5),
            'bnb_net':round(sum(inc.values()),4) if inc else 0.0,
            'strategies':sorted(set(t['strategy'] for t in st)),
            'multi_strat':len(set(t['strategy'] for t in st))>1,
            '_trades':st}
    return result

# ── FEES ─────────────────────────────────────────────────────────
def fee_analysis(bnb):
    totals=bnb.get('income_totals',{})
    overall=bnb.get('trades_stats',{}).get('overall',{})
    n=overall.get('total_trades',0)
    realized=totals.get('REALIZED_PNL',0); comm=totals.get('COMMISSION',0)
    fund=totals.get('FUNDING_FEE',0); net=realized+comm+fund
    return {'realized':round(realized,4),'commission':round(comm,5),'funding':round(fund,5),
        'net':round(net,4),'live_trades':n,'wr':overall.get('wr_pct',0),
        'avg_dur':overall.get('avg_dur_sec',0),
        'avg_fee':round(abs(comm)/n,5) if n else 0,
        'breakeven':round(abs(comm)/n*100,4) if n else 0,
        'by_sym':{sym:{k:round(v,5) for k,v in inc.items()} for sym,inc in bnb.get('income_by_sym',{}).items()}}

# ── SIGNAL CORRELATION ───────────────────────────────────────────
def sig_corr(trades):
    def bkt(pairs,size):
        d=defaultdict(list)
        for v,w in pairs: d[round(v/size)*size].append(w)
        return {k:{'wr':round(sum(v)/len(v)*100,1),'n':len(v)} for k,v in sorted(d.items()) if len(v)>=2}
    decided=[t for t in trades if t['outcome'] in ('win','lose')]
    won=lambda t: t['outcome']=='win'
    return {'vpin':bkt([(t['vpin'],won(t)) for t in decided if t['vpin']>0],0.05),
            'score':bkt([(t['score'],won(t)) for t in decided if t['score']!=0],10),
            'conf':bkt([(t['conf'],won(t)) for t in decided if t['conf']>0],10),
            'reason':{r:{'wr':round(sum(t['outcome']=='win' for t in g)/len(g)*100,1),'n':len(g)}
                for r,g in {r:[t for t in decided if t['reason']==r] for r in set(t['reason'] for t in decided)}.items() if g}}

# ── CLAUDE SUMMARY ───────────────────────────────────────────────
def claude_summary(d,trades,cascades,agree,oppose,reenter,per_coin,fees,sc):
    eng=d.get('engine_trades',{})
    rs=(d.get('reconciliation') or {}).get('stats',{})
    bal=(d.get('binance') or {}).get('balance',{})
    sim_t=[t for t in trades if not t['is_live']]
    live_t=[t for t in trades if t['is_live']]
    def wr_of(lst): 
        d2=[t for t in lst if t['outcome'] in ('win','lose')]
        return round(sum(1 for t in d2 if t['outcome']=='win')/len(d2)*100,1) if d2 else None
    bad_re=[e for e in reenter if e.get('flag')]
    issues=[]
    flag_coins=[s for s,v in per_coin.items() if v['bnb_net']<-0.3 and v['total']>=3]
    if flag_coins: issues.append(f"LOSING_COINS: {','.join(flag_coins)} — high fire rate + negative BNB P&L")
    if bad_re: issues.append(f"WIN_TO_LOSS_REENTER: {len(bad_re)} cases — {','.join(set(e['sym'] for e in bad_re))}")
    if oppose: issues.append(f"DIRECTION_CONFLICTS: {len(oppose)} cases on {','.join(set(e['sym'] for e in oppose[:5]))}")
    if cascades:
        best=max(cascades,key=lambda c:c['wr'])
        issues.append(f"CASCADE_PATTERN: {len(cascades)} events. Best: {best['ts']} {best['dir']} coins={best['coins'][:3]} WR={best['wr']}%")
    if fees['funding']!=0: issues.append(f"FUNDING: {fees['funding']:+.5f} USDT")
    strat_tbl={}
    for s,v in eng.items():
        if not isinstance(v,dict) or not v.get('trades'): continue
        ex=v.get('exits',{}); t=v['trades']
        strat_tbl[s]={'trades':t,'wr':v['wr'],'net_pct':round(v['net'],4),
            'avg_per_trade':round(v.get('avg_per_trade',0),4),
            'trail_pct':round(ex.get('trail',0)/t*100) if t else 0,
            'sl_pct':round(ex.get('sl',0)/t*100) if t else 0,
            'unknown_pct':round(ex.get('unknown',0)/t*100) if t else 0}
    return {'_meta':{'generated':d.get('generated',''),'since':d.get('since',''),'mode':d.get('mode',''),
                'balance':bal.get('balance',''),'unrealized':bal.get('crossUnPnl','')},
        'real_pnl':{'realized':fees['realized'],'commission':fees['commission'],'funding':fees['funding'],
            'net':fees['net'],'live_trades':fees['live_trades'],'wr':fees['wr'],'avg_dur_sec':fees['avg_dur'],
            'avg_fee_per_trade':fees['avg_fee'],'breakeven_gross_pct':fees['breakeven']},
        'engine_by_strategy':strat_tbl,
        'sim_vs_live':{'total':len(trades),'sim':len(sim_t),'live':len(live_t),
            'sim_wr':wr_of(sim_t),'live_wr':wr_of(live_t),
            'sim_net':round(sum(t['net_pct'] for t in sim_t),3),
            'live_net':round(sum(t['net_pct'] for t in live_t),3)},
        'reconciliation':{'matched':rs.get('matched_count',0),'unmatched_bnb':rs.get('unmatched_bnb_count',0),
            'unmatched_eng':rs.get('unmatched_eng_count',0),'avg_entry_slip':rs.get('avg_entry_slip_pct'),
            'avg_pnl_diff':rs.get('avg_pnl_diff_pct'),'avg_match_delta_ms':rs.get('avg_match_delta_ms')},
        'cascades':[{'ts':c['ts'],'dir':c['dir'],'coins':c['coins'],'strategies':c['strategies'],
            'wr':c['wr'],'avg_net':c['avg_net'],'live':c['live_count']} for c in cascades],
        'conflicts':{'oppose':len(oppose),'agree':len(agree),'reenter':len(reenter),'bad_reenter':len(bad_re),
            'top_oppose':oppose[:5],'top_bad_reenter':bad_re[:5]},
        'per_coin':{sym:{k:v for k,v in s.items() if not k.startswith('_')}
                    for sym,s in sorted(per_coin.items(),key=lambda x:x[1]['bnb_net'])},
        'signal_corr':{'vpin':sc['vpin'],'score':sc['score'],'by_reason':sc['reason']},
        'fees_by_sym':fees['by_sym'],
        'issues':issues}

# ── HTML ─────────────────────────────────────────────────────────
CSS="""
:root{--bg:#0a0e1a;--bg2:#0f1424;--bg3:#151b2e;--bg4:#1a2035;
 --g:#00ff9d;--r:#ff4d4d;--a:#ff9d00;--b:#4d9fff;--p:#9d4dff;--t:#c8d0e0;--m:#5a6478;--br:#1e2a40}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--t);font-family:'SF Mono','Fira Code',monospace;font-size:12px;line-height:1.5}
h1{color:var(--g);font-size:18px;padding:14px 18px;border-bottom:1px solid var(--br)}
h2{color:var(--b);font-size:11px;padding:9px 18px 5px;text-transform:uppercase;letter-spacing:1.5px;border-top:1px solid var(--br);margin-top:10px}
.kpis{display:grid;grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:6px;padding:8px 18px}
.kpi{background:var(--bg3);border-radius:5px;padding:9px 11px;border:1px solid var(--br)}
.kpi-l{color:var(--m);font-size:10px;margin-bottom:2px}.kpi-v{font-size:17px;font-weight:bold}
.pos{color:var(--g)}.neg{color:var(--r)}.neu{color:var(--m)}.amb{color:var(--a)}
table{width:calc(100% - 36px);border-collapse:collapse;margin:5px 18px}
th{color:var(--m);font-size:10px;text-align:left;padding:4px 7px;border-bottom:1px solid var(--br)}
td{padding:4px 7px;border-bottom:1px solid var(--bg2);vertical-align:middle}
tr:hover td{background:var(--bg2)}
.sym{color:#fff;font-weight:bold}.strat{color:var(--p)}
.lt{background:#00ff9d18;color:var(--g);border-radius:3px;padding:1px 4px;font-size:10px}
.st{background:#5a647818;color:var(--m);border-radius:3px;padding:1px 4px;font-size:10px}
.wr{font-weight:bold;background:rgba(255,157,0,0.08);}
.warn td{background:rgba(255,77,77,0.06)}
.cas{background:var(--bg3);border-radius:5px;padding:7px 11px;margin:3px 18px;border-left:3px solid var(--a);display:flex;gap:12px;flex-wrap:wrap;align-items:center}
.cas-coins{color:var(--m);font-size:11px;flex:1}
.bl{border-radius:3px;padding:1px 5px;font-size:10px;font-weight:bold}
.bl-long{background:#00ff9d18;color:var(--g)}.bl-short{background:#ff4d4d18;color:var(--r)}
.note{color:var(--a);font-size:11px;padding:3px 18px}
.tiny{font-size:10px;color:var(--m)}
details summary{cursor:pointer;color:var(--b);padding:4px 18px;font-size:11px;border-top:1px solid var(--br);margin-top:8px}
details[open] summary{color:var(--g)}
"""

def render(d,trades,cascades,agree,oppose,reenter,per_coin,fees,sc,summary):
    bal=(d.get('binance') or {}).get('balance',{})
    eng=d.get('engine_trades',{})
    date=d.get('generated','')[:16]

    # KPIs
    kpis=f"""
    <div class="kpi"><div class="kpi-l">BALANCE</div><div class="kpi-v">${flt(bal.get('balance',0)):.2f}</div></div>
    <div class="kpi"><div class="kpi-l">REALIZED</div><div class="kpi-v {vc(fees['realized'])}">{fees['realized']:+.4f}</div></div>
    <div class="kpi"><div class="kpi-l">COMMISSION</div><div class="kpi-v neg">{fees['commission']:+.5f}</div></div>
    <div class="kpi"><div class="kpi-l">FUNDING</div><div class="kpi-v {vc(fees['funding'])}">{fees['funding']:+.5f}</div></div>
    <div class="kpi"><div class="kpi-l">NET REAL</div><div class="kpi-v {vc(fees['net'])}">{fees['net']:+.4f}</div></div>
    <div class="kpi"><div class="kpi-l">LIVE TRADES</div><div class="kpi-v">{fees['live_trades']}</div></div>
    <div class="kpi"><div class="kpi-l">LIVE WR</div><div class="kpi-v {vc(fees['wr']-50)}">{fees['wr']:.1f}%</div></div>
    <div class="kpi"><div class="kpi-l">BREAKEVEN</div><div class="kpi-v amb">{fees['breakeven']:.3f}%</div></div>
    """

    issues=''.join(f'<div class="note">⚠️ {i}</div>' for i in summary['issues'])

    # Strategy table
    sr=''
    for s,v in sorted(eng.items()):
        if not isinstance(v,dict) or not v.get('trades'): continue
        ex=v.get('exits',{}); t=v['trades']
        sr+=f"""<tr><td><span class="strat">{s}</span></td><td>{t}</td>
          <td class="{vc(v['wr']-50)}">{v['wr']:.1f}%</td>
          <td class="{vc(v['net'])}">{pct(v['net'])}</td>
          <td class="{vc(v.get('avg_per_trade',0))}">{pct(v.get('avg_per_trade',0))}</td>
          <td class="tiny">trail={ex.get('trail',0)}({round(ex.get('trail',0)/t*100)}%) sl={ex.get('sl',0)}({round(ex.get('sl',0)/t*100)}%) unk={ex.get('unknown',0)}({round(ex.get('unknown',0)/t*100)}%)</td></tr>"""

    # Sim vs live
    sv=summary['sim_vs_live']
    svl=f"""<table><tr><th></th><th>Trades</th><th>WR</th><th>Net%</th></tr>
    <tr><td><span class="lt">LIVE</span></td><td>{sv['live']}</td>
      <td class="{vc((sv['live_wr'] or 0)-50)}">{sv['live_wr'] or '?'}%</td>
      <td class="{vc(sv['live_net'])}">{pct(sv['live_net'])}</td></tr>
    <tr><td><span class="st">SIM</span></td><td>{sv['sim']}</td>
      <td class="{vc((sv['sim_wr'] or 0)-50)}">{sv['sim_wr'] or '?'}%</td>
      <td class="{vc(sv['sim_net'])}">{pct(sv['sim_net'])}</td></tr></table>"""

    # Cascades
    ch=''
    for c in cascades:
        col='pos' if c['wr']>=60 else ('amb' if c['wr']>=40 else 'neg')
        ll=f'<span class="lt">{c["live_count"]} live</span>' if c['live_count'] else ''
        ch+=f"""<div class="cas"><span class="neu">{c['ts']}</span>
          <span class="bl bl-{c['dir']}">{c['dir'].upper()}</span>
          <span class="strat">{'+'.join(c['strategies'])}</span>
          <span class="{col}">{c['wr']}%WR</span>
          <span>{c['total']} coins</span>
          <span class="{vc(c['avg_net'])}">{pct(c['avg_net'],4)} avg</span>{ll}
          <div class="cas-coins">{', '.join(c['coins'])}</div></div>"""
    if not ch: ch='<div class="note">No cascades detected (need 3+ coins same dir within 90s)</div>'

    # Conflict table
    def conflict_table(events,title):
        if not events: return f'<div class="note">No {title} detected.</div>'
        rows=''.join(f"""<tr class="{'warn' if e.get('flag') or (e['out1']=='win' and e['out2']=='lose') else ''}">
          <td class="neu">{e['ts']}</td><td class="sym">{e['sym']}</td>
          <td><span class="strat">{e['strat1']}</span> {e['dir1']} → <span class="{vc(e['net1'])}">{e['out1']}</span> {pct(e['net1'])}</td>
          <td><span class="strat">{e['strat2']}</span> {e['dir2']} → <span class="{vc(e['net2'])}">{e['out2']}</span> {pct(e['net2'])}</td>
          <td class="tiny">{e['dt_sec']}s {"⚠️WIN→LOSS" if e.get('flag') else ""}</td></tr>"""
          for e in events[:15])
        return f'<table><tr><th>Time</th><th>Coin</th><th>Strategy 1</th><th>Strategy 2</th><th>Gap</th></tr>{rows}</table>'

    # Per coin
    cr=''
    for sym,s in sorted(per_coin.items(),key=lambda x:x[1]['bnb_net']):
        multi='⚠️' if s['multi_strat'] else ''
        dw=f"L:{s['long_wr']}% S:{s['short_wr']}%" if s['long_wr'] is not None else ''
        cr+=f"""<tr><td class="sym">{sym}</td>
          <td>{s['total']} <span class="lt">{s['live']}L</span> <span class="st">{s['sim']}S</span></td>
          <td class="{vc((s['wr'] or 0)-50)}">{s['wr'] or '?'}%</td>
          <td class="tiny">{dw}</td>
          <td class="{vc(s['sim_net'])}">{pct(s['sim_net'],4)}</td>
          <td class="{vc(s['bnb_pnl'])}">{s['bnb_pnl']:+.4f}</td>
          <td class="{vc(s['bnb_net'])}">{s['bnb_net']:+.4f}</td>
          <td class="strat tiny">{'+'.join(s['strategies'])} {multi}</td></tr>"""

    # Signal corr
    def st(bkt,lbl):
        if not bkt: return ''
        r=''.join(f"<tr><td>{k}</td><td class='{vc(v['wr']-50)}'>{v['wr']}%</td><td class='neu'>{v['n']}</td></tr>" for k,v in bkt.items())
        return f"<table><tr><th>{lbl}</th><th>WR</th><th>n</th></tr>{r}</table>"
    sig=f"""<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:6px;padding:0 18px">
      <div>{st(sc['vpin'],'VPIN')}</div><div>{st(sc['score'],'Score')}</div>
      <div>{st(sc.get('conf',{}),'Conf')}</div><div>{st(sc.get('reason',{}),'Exit reason')}</div></div>"""

    # Full trade log
    tr2=''
    for t in sorted(trades,key=lambda x:x['ts_dt'],reverse=True)[:100]:
        lb='<span class="lt">LIVE</span>' if t['is_live'] else '<span class="st">SIM</span>'
        sig_s=f"vpin={t['vpin']:.2f} sc={t['score']:.0f} conf={t['conf']:.0f}" if t['vpin'] else ''
        snap_s=f"s30={t['snap30']:+.2f}% s60={t['snap60']:+.2f}%" if t['snap30'] else ''
        sl_note=f"SL={t['dyn_sl']:.3f}%" if t['dyn_sl'] else ''
        tr2+=f"""<tr><td class="neu">{t['ts_dt'].strftime('%H:%M:%S')}</td>
          <td class="sym">{t['sym']}</td>
          <td class="{'pos' if t['dir']=='long' else 'neg'}">{t['dir']}</td>
          <td><span class="strat">{t['strategy']}</span></td>
          <td>{lb}</td><td>${t['entry_px']:.4f}</td>
          <td class="{vc(t['net_pct'])}">{t['outcome']}</td>
          <td class="{vc(t['net_pct'])}">{pct(t['net_pct'],4)}</td>
          <td class="tiny neu">{t['reason']}</td>
          <td class="tiny neu">{sig_s}</td>
          <td class="tiny neu">{snap_s} {sl_note}</td></tr>"""

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PredictEngine Correlation — {date}</title>
<style>{CSS}</style></head><body>
<h1>⚡ PredictEngine Correlation &nbsp;<span style="font-size:11px;color:var(--m)">{date}</span></h1>
<h2>📊 Real P&L (Binance)</h2><div class="kpis">{kpis}</div>{issues}
<h2>🎯 Strategy Performance</h2>
<table><tr><th>Strategy</th><th>Trades</th><th>WR</th><th>Net%</th><th>Avg/T</th><th>Exit mix</th></tr>{sr}</table>
<h2>⚫/🟢 Sim vs Live</h2>{svl}
<h2>🌊 Cascade Events (3+ coins · same dir · 90s window)</h2>{ch}
<h2>⚔️ Direction Conflicts (opposite dir · same coin · &lt;5min)</h2>{conflict_table(oppose,'conflicts')}
<h2>🔄 Close→Re-Enter (same coin · different strategy · &lt;2min)</h2>{conflict_table(reenter,'re-enter events')}
<h2>🪙 Per-Coin</h2>
<table><tr><th>Symbol</th><th>Fires</th><th>WR</th><th>Dir WR</th><th>Sim Net%</th><th>BNB P&L</th><th>BNB Net</th><th>Strategies</th></tr>{cr}</table>
<h2>📈 Signal → Outcome</h2>{sig}
<details><summary>📋 Full Trade Log ({len(trades)} trades)</summary>
<table><tr><th>Time</th><th>Sym</th><th>Dir</th><th>Strat</th><th>Type</th><th>Entry</th><th>Out</th><th>Net%</th><th>Reason</th><th>Signal</th><th>Snap/SL</th></tr>{tr2}</table>
</details>
<div class="tiny" style="padding:14px 18px;border-top:1px solid var(--br);margin-top:10px">
Generated: {d.get('generated','')} | Since: {d.get('since','')} | Mode: {d.get('mode','')}
</div></body></html>"""

# ── MAIN ─────────────────────────────────────────────────────────
def main():
    if len(sys.argv)<2: print("Usage: python3 analyze_correlation.py <analysis.json>"); sys.exit(1)
    p=Path(sys.argv[1]); d=load(p)
    if not d: d={}
    trades=build_trades(d)
    cascades=find_cascades(trades)
    agree,oppose,reenter=find_conflicts(trades)
    bnb = d.get('binance') or {}
    pc=coin_analysis(trades, bnb.get('income_by_sym',{}))
    fees=fee_analysis(bnb)
    sc=sig_corr(trades)
    summary=claude_summary(d,trades,cascades,agree,oppose,reenter,pc,fees,sc)

    out=Path('.')
    stem=p.stem
    cj=out/f"{stem}_corr.json"
    cj.write_text(json.dumps(summary,indent=2,default=str))
    print(f"✅  {cj}",file=sys.stderr)

    html=render(d,trades,cascades,agree,oppose,reenter,pc,fees,sc,summary)
    ch2=out/f"{stem}_corr.html"
    ch2.write_text(html)
    print(f"✅  {ch2}",file=sys.stderr)
    print(f"\nShare with Claude:  cat {cj} | pbcopy",file=sys.stderr)

if __name__=='__main__': main()
