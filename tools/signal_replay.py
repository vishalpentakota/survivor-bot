#!/usr/bin/env python3
"""Survivor signal replay — READ-ONLY step-zero instrumentation.

Replays the index-signal rules bar-by-bar over historical Dhan data and logs every
signal (OLD rules = commit 1e8d3bb vs NEW rules = working tree) plus a per-gate
rejection counter. NEVER places/modifies/cancels orders:
  * the only broker object used is `_RO`, a whitelist wrapper exposing exactly
    intraday_minute_data / expired_options_data / fetch_security_list;
  * SURVIVOR_LIVE_TRADING is removed from the environment before the client is built;
  * a self-check at startup asserts write methods are unreachable.

Usage: signal_replay.py [--days 45] [--indices NIFTY,BANKNIFTY,...] [--out DIR]

Outputs (in --out, default /opt/data/Projects/Survivor-bot/survivor-replay):
  signals.csv     one row per fired signal per rule-set (old/new), with outcome
  rejections.csv  per rule-set / index / gate counters
  summary.md      counts, win-rate, R-distribution, old-vs-new diff

Option outcome model (APPROXIMATE, flagged in every output):
  Dhan's expired-options endpoint only serves a *rolling ATM* series (strike changes as
  spot moves). We chain-link it (diff vs previous close when strike unchanged, vs bar open
  when it rolls) to get a single continuous path; entry price = ATM close at signal bar.
  Exit: engine sizing (SL = clamp(10% of premium, 7.5..10 pts), TP = max(15, 1.75*SL)),
  SL assumed first when SL and TP are both inside one 1-min bar, forced exit 15:10 IST.
  DTE is approximate (weekday-based from the security-master expiry weekday).
"""
import argparse, importlib.util, json, os, sys, time, subprocess
from datetime import datetime, timedelta, date

APP = "/opt/data/Projects/Survivor-bot/survivor-cloud-migration-vnext/app"
sys.path.insert(0, APP)
os.environ.pop("SURVIVOR_LIVE_TRADING", None)

import numpy as np
import pandas as pd


def _md(obj):
    if isinstance(obj, pd.Series):
        obj = obj.to_frame()
    obj = obj.reset_index()
    cols = [str(c) for c in obj.columns]
    out = ['| ' + ' | '.join(cols) + ' |', '|' + '---|' * len(cols)]
    for _, r in obj.iterrows():
        out.append('| ' + ' | '.join(str(v) for v in r.values) + ' |')
    return '\n'.join(out)


pd.DataFrame._md_ = _md
pd.Series._md_ = _md

OLD_COMMIT = "1e8d3bb"
SCRATCH = "/opt/data/cache/scratch/old_engine"


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def load_engines():
    os.makedirs(SCRATCH, exist_ok=True)
    src = subprocess.check_output(
        ["git", "-C", "/opt/data/Projects/Survivor-bot/survivor-cloud-migration-vnext", "show",
         f"{OLD_COMMIT}:app/survivor_engine.py"])
    p = os.path.join(SCRATCH, "survivor_engine_old.py")
    open(p, "wb").write(src)
    new = _load("engine_new", os.path.join(APP, "survivor_engine.py"))
    old = _load("engine_old", p)
    os.environ.pop("SURVIVOR_LIVE_TRADING", None)
    return old, new


class _RO:
    """Whitelist-only broker view. Anything else raises."""
    _OK = ("intraday_minute_data", "expired_options_data", "fetch_security_list")

    def __init__(self, raw):
        object.__setattr__(self, "_raw", raw)

    def __getattr__(self, n):
        if n not in self._OK:
            raise RuntimeError(f"signal_replay is read-only: '{n}' blocked")
        return getattr(self._raw, n)


def make_ro(new_engine):
    os.environ.pop("SURVIVOR_LIVE_TRADING", None)
    # credentials/token live in the live tree (vnext has none); read-only use of its client class
    LIVE_APP = "/opt/data/Projects/Survivor-bot/survivor-cloud-migration/app"
    saved = list(sys.path)
    sys.path.insert(0, LIVE_APP)
    for m in ("dhan_broker", "cloud_guard"):
        sys.modules.pop(m, None)
    cwd = os.getcwd(); os.chdir(LIVE_APP)
    try:
        live = _load("engine_live_creds", LIVE_APP + "/survivor_engine.py")
        c = live.SurvivorDhanClient()
    finally:
        os.chdir(cwd); sys.path[:] = saved
    raw = getattr(c.dhan, "_broker", c.dhan)
    ro = _RO(raw)
    for bad in ("place_order", "modify_order", "cancel_order", "place_slice_order", "kill_switch"):
        try:
            getattr(ro, bad)
            raise SystemExit("SAFETY SELF-CHECK FAILED: write method reachable: " + bad)
        except RuntimeError:
            pass
    return ro


# ───────────────────────── data ─────────────────────────
def _retry(fn, n=4):
    for i in range(n):
        r = fn()
        if r.get("status") == "success":
            return r
        time.sleep(1.5 * (i + 1))
    return r


def spot_1m(ro, sid, seg, itype, d0, d1, IST):
    out = []
    cur = d0
    while cur <= d1:
        end = min(cur + timedelta(days=28), d1)
        r = _retry(lambda: ro.intraday_minute_data(security_id=str(sid), exchange_segment=seg,
                                                    instrument_type=itype, from_date=str(cur), to_date=str(end)))
        d = r.get("data") or {}
        if isinstance(d, dict) and d.get("close"):
            out.append(pd.DataFrame({
                "timestamp": pd.to_datetime(d["timestamp"], unit="s", utc=True).tz_convert(IST),
                "open": d["open"], "high": d["high"], "low": d["low"], "close": d["close"],
                "volume": d["volume"]}))
        cur = end + timedelta(days=1)
        time.sleep(0.4)
    if not out:
        return pd.DataFrame()
    df = pd.concat(out).drop_duplicates("timestamp").set_index("timestamp").sort_index()
    return df


def resample3(df):
    return df.resample("3min").agg({"open": "first", "high": "max", "low": "min",
                                    "close": "last", "volume": "sum"}).dropna()


def option_day(ro, sid, day, side, IST, cache):
    """ATM rolling 1-min series for one trading day, chain-linked. Returns DataFrame or None."""
    key = f"{sid}_{day}_{side}"
    if key in cache:
        return cache[key]
    nxt = day + timedelta(days=1)
    res = None
    for code in (1,):
        r = _retry(lambda: ro.expired_options_data(
            security_id=int(sid), exchange_segment="NSE_FNO", instrument_type="OPTIDX",
            expiry_flag="WEEK", expiry_code=code, strike="ATM",
            drv_option_type="CALL" if side == "CE" else "PUT",
            required_data=["open", "high", "low", "close", "volume", "iv", "spot", "strike"],
            from_date=str(day), to_date=str(nxt), interval=1))
        time.sleep(0.4)
        d = (r.get("data") or {})
        d = d.get("data", d) if isinstance(d, dict) else {}
        leg = d.get("ce" if side == "CE" else "pe") if isinstance(d, dict) else None
        if leg and leg.get("close"):
            df = pd.DataFrame({
                "timestamp": pd.to_datetime(leg["timestamp"], unit="s", utc=True).tz_convert(IST),
                "open": leg["open"], "high": leg["high"], "low": leg["low"], "close": leg["close"],
                "volume": leg["volume"], "strike": leg["strike"], "iv": leg.get("iv") or [np.nan] * len(leg["close"])})
            df = df.drop_duplicates("timestamp").set_index("timestamp").sort_index()
            df = df[df.index.date == day]
            if not df.empty:
                res = chain_link(df)
                break
    cache[key] = res
    return res


def chain_link(df):
    """Make the rolling-ATM series continuous (see module doc)."""
    o, h, l, c, k = (df[x].values for x in ("open", "high", "low", "close", "strike"))
    n = len(df)
    po, ph, pl, pc = (np.zeros(n) for _ in range(4))
    prev = None
    for i in range(n):
        if prev is None:
            po[i], ph[i], pl[i], pc[i] = o[i], h[i], l[i], c[i]
        else:
            ref = c[i - 1] if k[i] == k[i - 1] else o[i]
            base = prev
            po[i] = base + (o[i] - ref)
            ph[i] = base + (h[i] - ref)
            pl[i] = base + (l[i] - ref)
            pc[i] = base + (c[i] - ref)
        prev = pc[i]
    out = df.copy()
    out["open"], out["high"], out["low"], out["close"] = po, ph, pl, pc
    out["raw_close"] = c
    return out


def expiry_weekday(ro):
    try:
        sec = ro.fetch_security_list(mode="compact")
        m = sec[(sec["SEM_INSTRUMENT_NAME"] == "OPTIDX") & (sec["SEM_TRADING_SYMBOL"].astype(str).str.startswith("NIFTY-"))]
        ds = pd.to_datetime(m["SEM_EXPIRY_DATE"], errors="coerce").dropna()
        return int(ds.dt.weekday.mode().iloc[0])
    except Exception:
        return None


def approx_dte(day, wd):
    if wd is None:
        return None
    return (wd - day.weekday()) % 7


# ───────────────────────── gates ─────────────────────────
def opt_gate(sess3, require_min, new_tod=None):
    """Mirror of verify_option_chart_momentum on a session-only 3m frame."""
    if sess3 is None or len(sess3) < require_min:
        return False, "opt_insufficient_session_bars"
    c = float(sess3["close"].iloc[-1]); e9 = float(sess3["ema9"].iloc[-1])
    vw = float(sess3["vwap"].iloc[-1]); st = int(sess3["st_dir"].iloc[-1])
    if e9 > 0 and c > e9 * 1.15:
        return False, "opt_anti_spike_15pct"
    if c >= e9 * 0.992 and (c >= vw * 0.99 or st == 1):
        return True, "ok"
    return False, "opt_momentum_weak"


def session3(opt1m, upto, eng):
    """3m session frame from chained 1m data up to `upto`, with the NEW engine's indicator set."""
    d = opt1m[opt1m.index < upto + pd.Timedelta(minutes=3)]
    d = d[["open", "high", "low", "close", "volume"]]
    d3 = resample3(d)
    d3 = d3[d3.index <= upto]
    if d3.empty:
        return d3
    d3 = d3.copy()
    d3["ema9"] = d3["close"].ewm(span=9, adjust=False).mean()
    tv = ((d3["high"] + d3["low"] + d3["close"]) / 3.0) * d3["volume"]
    cv = d3["volume"].cumsum().replace(0, np.nan)
    d3["vwap"] = (tv.cumsum() / cv).fillna(d3["close"])
    if len(d3) >= 3:
        _, sd = eng.compute_supertrend(d3, period=10, multiplier=2.0)
        d3["st_dir"] = sd
    else:
        d3["st_dir"] = 1
    return d3


def fast_strike_diag(eng, df, slope_mode):
    """First failing FAST_STRIKE gate for the direction implied by Supertrend (for counters)."""
    d = eng.compute_indicators(df)
    cur, prev = d.iloc[-1], d.iloc[-2]
    close = float(cur["close"]); ema9 = float(cur["ema9"]); ema21 = float(cur["ema21"])
    vwap = float(cur["vwap"]); rsi = float(cur["rsi"]); atr = float(cur["atr"])
    mh = float(cur["macd_hist"]); pmh = float(prev["macd_hist"]); st = int(cur["st_dir"])
    pts = ema21 - float(d.iloc[-4]["ema21"]) if len(d) >= 4 else 0.0
    sl = pts / atr if (slope_mode == "atr" and atr > 0) else pts
    thr = getattr(eng, "EMA21_SLOPE_ATR_MIN", 0.12) if slope_mode == "atr" else 2.5
    long_ = st == 1
    s = 1 if long_ else -1
    checks = [
        ("fs_ema_stack", (close > ema9 > ema21) if long_ else (close < ema9 < ema21)),
        ("fs_vwap", (close > vwap) if long_ else (close < vwap)),
        ("fs_rsi_band", (55 <= rsi <= 72) if long_ else (30 <= rsi <= 44)),
        ("fs_macd_sign", (mh > 0) if long_ else (mh < 0)),
        ("fs_ema21_slope", (s * sl) >= thr),
        ("fs_spark_break", ((close > prev["high"]) and (mh > pmh)) if long_ else ((close < prev["low"]) and (mh < pmh))),
    ]
    for name, ok in checks:
        if not ok:
            return name, dict(slope_ratio=(pts / atr if atr > 0 else 0.0), slope_pts=pts)
    return "fs_all_pass_but_none", dict(slope_ratio=(pts / atr if atr > 0 else 0.0), slope_pts=pts)


# ───────────────────────── outcome ─────────────────────────
def simulate(opt1m, t_entry, entry_px):
    """Walk 1m chained path after signal bar close. Returns dict with outcome/MFE/MAE in pts and R."""
    sl_pts = max(7.5, min(10.0, entry_px * 0.10))
    tp_pts = round(max(15.0, sl_pts * 1.75), 2)
    start = t_entry + pd.Timedelta(minutes=3)
    w = opt1m[opt1m.index >= start]
    if w.empty:
        return None
    base = float(opt1m[opt1m.index < start]["close"].iloc[-1]) if (opt1m.index < start).any() else entry_px
    shift = entry_px - base
    sl, tp = entry_px - sl_pts, entry_px + tp_pts
    mfe = mae = 0.0
    exit_px, reason, exit_t = None, "EOD", None
    for t, r in w.iterrows():
        hi, lo = r["high"] + shift, r["low"] + shift
        if t.strftime("%H:%M") >= "15:10":
            exit_px, reason, exit_t = r["open"] + shift, "EOD_1510", t
            break
        mae = min(mae, lo - entry_px)
        if lo <= sl:
            exit_px, reason, exit_t = sl, "SL", t
            break
        mfe = max(mfe, hi - entry_px)
        if hi >= tp:
            exit_px, reason, exit_t = tp, "TP", t
            break
    if exit_px is None:
        exit_px = float(w["close"].iloc[-1]) + shift
        exit_t = w.index[-1]
    pts = exit_px - entry_px
    return dict(sl_pts=sl_pts, tp_pts=tp_pts, exit_reason=reason, exit_time=str(exit_t),
                pnl_pts=round(pts, 2), r_mult=round(pts / sl_pts, 2),
                mfe_pts=round(mfe, 2), mae_pts=round(mae, 2),
                mfe_r=round(mfe / sl_pts, 2), mae_r=round(mae / sl_pts, 2))


# ───────────────────────── main ─────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=45)
    ap.add_argument("--indices", default="NIFTY,BANKNIFTY,MIDCPNIFTY")
    ap.add_argument("--out", default="/opt/data/Projects/Survivor-bot/survivor-replay")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    old, new = load_engines()
    IST = new.IST
    ro = make_ro(new)
    wd = expiry_weekday(ro)
    want = [x.strip() for x in a.indices.split(",")]
    idx_cfg = {v["symbol_prefix"]: (k, v) for k, v in new.INDEX_UNIVERSE.items() if v["symbol_prefix"] in want}

    end = datetime.now(IST).date()
    start = end - timedelta(days=int(a.days * 1.5) + 7)
    vix1 = spot_1m(ro, "21", "IDX_I", "INDEX", start, end, IST)
    vix_daily_open = {}
    if not vix1.empty:
        for dday, g in vix1.groupby(vix1.index.date):
            vix_daily_open[dday] = g

    def vix_at(ts):
        g = vix_daily_open.get(ts.date())
        if g is None:
            return None
        g = g[g.index <= ts]
        return round(float(g["close"].iloc[-1]), 2) if not g.empty else None

    rows, rej = [], {}
    cache = {}

    def bump(rs, idx, gate):
        rej[(rs, idx, gate)] = rej.get((rs, idx, gate), 0) + 1

    meta = dict(generated=str(datetime.now(IST)), old_commit=OLD_COMMIT, indices=want,
                expiry_weekday=wd, requested_days=a.days)

    for prefix, (name, cfg) in idx_cfg.items():
        print(f"[{prefix}] fetching spot…", flush=True)
        s1 = spot_1m(ro, cfg["security_id"], "IDX_I", "INDEX", start, end, IST)
        if s1.empty:
            print(f"[{prefix}] NO SPOT DATA", flush=True); continue
        s3 = resample3(s1)
        days = sorted(set(s3.index.date))[-a.days:]
        meta[f"{prefix}_days"] = [str(days[0]), str(days[-1]), len(days)]
        for dday in days:
            day_bars = s3[s3.index.date == dday]
            for ts in day_bars.index:
                hhmm = ts.strftime("%H:%M")
                if not ("09:20" <= hhmm <= "14:15"):
                    continue
                hist = s3[(s3.index <= ts) & (s3.index >= ts.normalize() - pd.Timedelta(days=5))]
                if len(hist) < 25:
                    continue
                for rs, eng in (("old", old), ("new", new)):
                    tod_ok = ("09:30" <= hhmm <= "11:30") or ("13:30" <= hhmm <= "14:15")
                    try:
                        sig = eng.evaluate_index_signal(hist)
                    except Exception as e:
                        bump(rs, prefix, "engine_exception:" + type(e).__name__); continue
                    if sig.get("signal") not in ("LONG", "SHORT"):
                        reason = sig.get("reason", "")
                        if reason.startswith("[REGIME_FILTER]"):
                            bump(rs, prefix, "regime_chop_adx<20")
                        else:
                            try:
                                g, _ = fast_strike_diag(eng, hist, "atr" if rs == "new" else "pts")
                            except Exception:
                                g = "diag_error"
                            bump(rs, prefix, g)
                        continue
                    direction, setup = sig["signal"], sig.get("setup", "?")
                    row = dict(ruleset=rs, index=prefix, date=str(dday), time=hhmm, setup=setup,
                               direction=direction, spot=sig.get("spot"), rsi=sig.get("rsi"),
                               adx=round(float(eng.compute_indicators(hist).iloc[-1].get("adx", 0)), 1),
                               vix=vix_at(ts), weekday=ts.strftime("%a"), dte_approx=approx_dte(dday, wd),
                               tod_gate_pass=tod_ok)
                    try:
                        dd = eng.compute_indicators(hist)
                        atr = float(dd.iloc[-1]["atr"])
                        pts = float(dd.iloc[-1]["ema21"]) - float(dd.iloc[-4]["ema21"])
                        row["ema21_slope_pts"] = round(pts, 2)
                        row["ema21_slope_atr"] = round(pts / atr, 3) if atr > 0 else None
                    except Exception:
                        pass
                    if not tod_ok:
                        row["rejected_by"] = "tod_gate"; bump(rs, prefix, "tod_gate(after_signal)")
                        rows.append(row); continue
                    opt = option_day(ro, cfg["security_id"], dday, "CE" if direction == "LONG" else "PE", IST, cache)
                    if opt is None or opt.empty:
                        row["rejected_by"] = "no_option_data"; bump(rs, prefix, "no_option_data")
                        rows.append(row); continue
                    sess = session3(opt, ts, new)
                    # OLD rule: >=15 bars of 4-day history (always satisfied by prior-day bars) — approximated as min 1 bar
                    ok, why = opt_gate(sess, 5 if rs == "new" else 1)
                    if not ok:
                        row["rejected_by"] = why; bump(rs, prefix, why)
                        rows.append(row); continue
                    entry = round(float(sess["close"].iloc[-1]), 2)
                    row["entry"] = entry
                    out = simulate(opt, ts, entry)
                    if out is None:
                        row["rejected_by"] = "no_forward_bars"; rows.append(row); continue
                    row.update(out); row["rejected_by"] = ""
                    bump(rs, prefix, "PASSED_ALL_GATES")
                    rows.append(row)
            print(f"[{prefix}] {dday} done; rows={len(rows)}", flush=True)
            pd.DataFrame(rows).to_csv(os.path.join(a.out, "signals.csv"), index=False)

    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(a.out, "signals.csv"), index=False)
    rj = pd.DataFrame([dict(ruleset=k[0], index=k[1], gate=k[2], count=v) for k, v in rej.items()])
    rj.sort_values(["ruleset", "index", "count"], ascending=[True, True, False]).to_csv(
        os.path.join(a.out, "rejections.csv"), index=False)
    json.dump(meta, open(os.path.join(a.out, "meta.json"), "w"), indent=2)
    write_summary(df, rj, meta, a.out)
    print("DONE", a.out)


def write_summary(df, rj, meta, out):
    L = ["# Survivor signal replay (READ-ONLY, approximate option model)", "", f"`{json.dumps(meta)}`", ""]
    if df.empty:
        L.append("No signals fired in window."); open(os.path.join(out, "summary.md"), "w").write("\n".join(L)); return
    tr = df[(df["rejected_by"] == "") & df["pnl_pts"].notna()].copy()
    L += ["## Raw signals fired by rule-set / setup", ""]
    L.append(df.groupby(["ruleset", "setup"]).size().unstack(0).fillna(0).astype(int)._md_()); L.append("")
    L += ["## Tradable (passed all gates) — outcomes", ""]
    if tr.empty:
        L.append("None.")
    else:
        # cluster: first signal per (rule-set,index,setup,direction,day)
        tr = tr.sort_values(["ruleset", "index", "date", "time"])
        tr["cluster_first"] = ~tr.duplicated(["ruleset", "index", "date", "setup", "direction"])
        for lab, sub in (("ALL tradable signals", tr), ("First per index/setup/dir/day (closer to what P9 allows)", tr[tr.cluster_first])):
            L += [f"### {lab}", ""]
            g = sub.groupby(["ruleset", "setup"]).agg(
                n=("r_mult", "size"), days=("date", "nunique"),
                win_pct=("pnl_pts", lambda s: round((s > 0).mean() * 100, 1)),
                avg_R=("r_mult", "mean"), med_R=("r_mult", "median"),
                avg_MFE_R=("mfe_r", "mean"), avg_MAE_R=("mae_r", "mean"),
                tp=("exit_reason", lambda s: (s == "TP").sum()), sl=("exit_reason", lambda s: (s == "SL").sum()),
            ).round(2)
            L.append(g._md_()); L.append("")
        L += ["### By index (first-per-day, new rules)", ""]
        sub = tr[(tr.ruleset == "new") & tr.cluster_first]
        if not sub.empty:
            L.append(sub.groupby("index").agg(n=("r_mult", "size"), win_pct=("pnl_pts", lambda s: round((s > 0).mean() * 100, 1)),
                                              avg_R=("r_mult", "mean")).round(2)._md_()); L.append("")
            L += ["### By approx DTE (first-per-day, new rules)", ""]
            L.append(sub.groupby("dte_approx", dropna=False).agg(n=("r_mult", "size"), win_pct=("pnl_pts", lambda s: round((s > 0).mean() * 100, 1)),
                                                              avg_R=("r_mult", "mean")).round(2)._md_()); L.append("")
            sub = sub.assign(vix_bucket=pd.cut(sub["vix"], [0, 11, 14, 18, 100]))
            L += ["### By India VIX bucket (first-per-day, new rules)", ""]
            L.append(sub.groupby("vix_bucket", observed=True).agg(n=("r_mult", "size"), win_pct=("pnl_pts", lambda s: round((s > 0).mean() * 100, 1)),
                                                               avg_R=("r_mult", "mean")).round(2)._md_()); L.append("")
    L += ["## Old vs new: signals whose existence differs", ""]
    key = ["index", "date", "time", "setup", "direction"]
    o = df[df.ruleset == "old"][key + ["rejected_by"]]; n = df[df.ruleset == "new"][key + ["rejected_by"]]
    m = o.merge(n, on=key, how="outer", suffixes=("_old", "_new"), indicator=True)
    L.append(m["_merge"].value_counts()._md_()); L.append("")
    L.append("Differences by (setup, which side):"); L.append("")
    L.append(m[m["_merge"] != "both"].groupby(["setup", "_merge"], observed=True).size()._md_()); L.append("")
    both = m[m["_merge"] == "both"]
    L.append("Same raw signal, different option-gate verdict (old_gate → new_gate):"); L.append("")
    diff = both[both.rejected_by_old != both.rejected_by_new]
    L.append(diff.groupby(["rejected_by_old", "rejected_by_new"]).size()._md_() if not diff.empty else "none"); L.append("")
    L += ["## Rejection counters (bars evaluated that did NOT fire / were killed, by gate)", ""]
    if not rj.empty:
        piv = rj.pivot_table(index="gate", columns=["ruleset", "index"], values="count", aggfunc="sum", fill_value=0)
        L.append(piv._md_()); L.append("")
    L += ["## Caveats", "",
          "- Option path is chain-linked rolling-ATM data; levels/gates are approximations of the true contract.",
          "- OLD option gate approximated (prior-day bars unavailable per-strike); old-vs-new option-gate differences are therefore only the 5-session-bar floor.",
          "- DTE from weekday of security-master expiry; holidays/expiry shifts ignored.",
          "- SL assumed first when SL and TP fall in the same 1-min bar. No costs/slippage; subtract ~₹55/lot round trip.",
          "- Budgets/cooldowns/daily limits (P9) are NOT simulated; see 'first per day' cut for a rough proxy.",
          "- Small N means no conclusion; read the counts first."]
    open(os.path.join(out, "summary.md"), "w").write("\n".join(L))


if __name__ == "__main__":
    main()
