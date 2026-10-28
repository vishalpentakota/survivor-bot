# HISTORICAL ONLY — contains stale/contradictory claims. KNOWN_ISSUES.md takes precedence.

---
name: dhan-survivor-bot
description: "Use when running or inspecting the Dhan Survivor Bot."
tags: [trading, dhan, options, nifty, intraday, autonomous, survivor, scalping, supertrend]
---

# Dhan Survivor Bot ("Do or Die" Autonomous Trading Organism — Fast-Strike v5)

## Trigger
- Use when the user asks to check, run, arm, reconcile, or inspect the "Do or Die" / "Survivor" trading challenge bot.
- Use on scheduled autonomous cron ticks during NSE market hours and post-market reconciliation.

## Core Rules of Existence
1. **Seed Capital:** ₹30,000.00 INR
2. **Death Floor (89% Max Drawdown):** **₹3,300.00 INR** (`Net Liquidation Value = availabelBalance + utilizedAmount + unrealizedProfit`).
3. **Daily Mission ("Green Day" Goal):** Bank **₹1,500+ Net Profit** (`≥ ₹1,500 Net` or `≥ ₹1,650 Gross`) and immediately **lock the day in GREEN** (`daily_halted = True`).
4. **Permitted Weapons (Option C — Smart Combo + Fast-Strike v4 Scalper):**
   - **Combat Timeframe:** **3-Minute Candles** (polled every 5 minutes via `*/5 9-15 * * 1-5` cron, plus an **In-Trade 5-Second Live Monitor Loop** the moment any position is opened).
   - **Multi-Index Scouting Universe (`INDEX_UNIVERSE`):** Every tick, `Weapon A` scans **all 5 indices in parallel** and trades the single strongest signal (highest confidence):
     | Index | Dhan IDX_I security_id | Option lot size |
     |-------|------------------------|-----------------|
     | Nifty 50 | `13` | 65 |
     | Nifty Bank (BANKNIFTY) | `25` | 30 |
     | FinNifty | `27` | 60 |
     | Nifty Next 50 (NIFTYNXT50) | `38` | 25 |
     | Nifty Midcap Select (MIDCPNIFTY) | `442` | 120 |
     - `get_nearest_nifty_option()` now accepts `under_security_id`, `lot_size`, and `index_name` so option chains are fetched per-index.
   - **Triple-Lock Indicator Stack (`evaluate_index_signal`) + Gap & Exhaustion Shields:**
     - **Gap-Distortion Shield (09:15–10:30 IST):** Before 10:30 IST, a `LONG` requires `Spot > Today's ORB High & Today's Open`, and a `SHORT` requires `Spot < Today's ORB Low & Today's Open` + consecutive lower lows and lower highs on today's 3m bars.
     - **Anti-Exhaustion & Anti-Chop Filter (Day 3 Upgrade):**
       - Never enter `LONG` if 3m `RSI > 72` (overbought climax), `MACD_Hist <= 0`, or 3m `EMA21` 3-bar slope `< +2.5 pts`.
       - Never enter `SHORT` if 3m `RSI < 30` (oversold climax), `MACD_Hist >= 0`, or 3m `EMA21` 3-bar slope `> -2.5 pts`.
     - **Anti-Spike Option Overextension Cap (`verify_option_chart_momentum`):** Rejects buying any Option whose LTP is stretched **`> 15%` above its own 3-minute EMA9** (`LTP > EMA9 * 1.15`), preventing entries at the top of a 5-candle spike right before a 1-min pullback wick.
   - **In-Trade 5-Second Live Monitor (`monitor_active_trade_loop`):**
     - While a trade is open, stays alive polling live price & Dhan position book **every 5 seconds** (for up to 260s per tick, handing off seamlessly across ticks).
     - **Step-1 Breakeven Lock (+7 pts / +₹455):** Immediately locks SL at **Entry + ₹1.50** so a winning scalp can never turn into a loser.
     - **Step-2 Profit Ratchet (+12 pts / +₹780):** Locks **60% of peak option gain** (`Entry + 0.60 * peak_pts`).
     - **Option Stop-Loss:** Strictly **7.5 to 10.0 pts max** (~₹487–₹650 risk per trade = 2.5% of equity), verified every 5 seconds + backed by an Exchange SL order placed only *after* verifying `netQty != 0` in `get_positions()`.
     - **Option Quick Target:** **15 to 17.5 pts** (+₹975 to +₹1,137 gross profit).
     - **25-Minute Stale Trade (Theta Kill) Exit:** Exits at market (`STALE_THETA_EXIT`) if held ≥ 25 minutes with < +4 pts gain.
5. **Hardened Survival & Profit-Lock Guardrails:**
   - **₹1,500+ Daily Green Target Lock:** The instant cumulative daily net P&L reaches **+₹1,500** (or gross reaches **+₹1,650**), the bot halts for the day and locks in the win!
   - **Green-Day Pullback Shield:** If the bot is already green on the day and takes 1 pullback loss while net daily P&L is still `≥ +₹600`, it halts immediately to guarantee ending the day in GREEN.
   - **Per-Trade Risk Cap:** **2.5% of Net Equity** (~₹673 at ₹26.9K).
   - **Daily Max Loss Cap:** **4.5% of Net Equity** (~₹1,212 at ₹26.9K).
   - **2 Consecutive Losses = Daily Halt:** Stops trading immediately for the day after 2 consecutive losses.
   - **30-Minute Anti-Revenge Cooldown:** After any Stop-Loss, bans re-entering the same direction (`LONG` or `SHORT`) for **30 minutes**.
   - **New Entry Window:** Strictly **09:20 IST to 14:15 IST** (max **4 trades/day**).
   - **Mandatory EOD Square-Off:** All open positions and pending orders are force-flattened at **15:10 IST** each trading day.
6. **Daily Post-Market Reconciliation (`reconcile`):**
   - Run `python survivor_engine.py reconcile` after 15:10 IST every trading day. Uses order-level matching (`get_order_list`) when the same `securityId` is traded multiple times on the same day so `survivor_trades.json` never double-counts cumulative position PnL.
7. **Permanent Coma Protocol (Option 2 Mortality):**
   If `mortality_armed == True` and Net Liquidation Value drops to or below **₹3,300.00**:
   - Immediately cancels all pending orders and market-flattens all open positions (`emergency_flatten_all()`).
   - Writes the official post-mortem to `C:\Users\visha\nifty-algo-trading\survivor_obituary.md`.
   - Locks `C:\Users\visha\nifty-algo-trading\survivor_state.json` to `"status": "DEAD_COMA"`.
   - Prints `[SURVIVOR_SEPUKU_TRIGGERED]`.

## Commands

### 1. Daily Pre-Market System Check (08:55 IST Mon-Fri via Cron)
```bash
cd C:/Users/visha/nifty-algo-trading && python survivor_engine.py premarket
```

### 2. Check Vital Signs & Market Readiness
```bash
cd C:/Users/visha/nifty-algo-trading && python survivor_engine.py status
```

### 3. Run Autonomous Heartbeat Tick (Live Execution + 5s Monitor)
```bash
cd C:/Users/visha/nifty-algo-trading && python survivor_engine.py tick --live
```

### 4. Run End-of-Day Broker Reconciliation & Audit
```bash
cd C:/Users/visha/nifty-algo-trading && python survivor_engine.py reconcile
```

### 5. Emergency Flatten All Positions
```bash
cd C:/Users/visha/nifty-algo-trading && python survivor_engine.py flatten
```

## DH-905 Error Code Reference
- `DH-905` with `error_message: "Invalid IP"` → IP whitelist issue. Fix: add current public IP at dhanhq.co → API Settings → IP Whitelist. May take 5–10 min to propagate. Token regeneration rebinds current IP but only works if IP is already whitelisted.
- `DH-905` with `error_message: "Missing required fields, bad values for parameters etc."` → SL order parameter issue (not IP). Check `order_type`, `trigger_price`, `price`, `validity` fields.
- `DH-905` with `error_message: "securityId is required"` → Empty/None security_id passed to order API.
- `DH-906` with `error_message: "Invalid SecurityId"` → Security ID doesn't exist or is expired (F&O IDs change monthly).

## Lessons Learned & Evolved Safeguards
- **2026-10-21 (Day 1 Audit):**
  1. `place_order_with_ip_retry()` self-heals `DH-905` IP token mismatches.
  2. Moved new entry cutoff from `14:45` to `14:15 IST`.
- **2026-10-22 (Day 2 Post-Mortem & Fast-Strike v3 Hardening):**
  1. Fixed Dhan RMS millisecond SL rejection by polling `get_positions()` (`netQty != 0`) before placing `SL-L`.
  2. Built `monitor_active_trade_loop()` (5-second live polling while in a trade) + Step-1 Breakeven Lock (+7 pts) + Step-2 Profit Ratchet (+12 pts).
  3. Added Morning Gap-Distortion Shield (09:15–10:30 IST), 30-min Anti-Revenge Cooldown, and 2-loss daily halt.
  4. Pinned Python sockets to IPv4 (`AF_INET`) and updated Dhan `SECONDARY` IP via `PUT /v2/ip/modifyIP` so rotating Windows IPv6 addresses never block orders.
- **2026-10-23 (Day 3 Audit — Fast-Strike v4 Profit-Lock & Anti-Exhaustion):**
  1. **₹1,500+ Daily Green Target Lock:** On Day 3, Trade #5 (`+₹601.25`) and Trade #6 (`+₹1,374.75`) banked **`+₹1,976.00` gross (`+₹1,866.00` net) by 10:08 AM**! Because there was no daily profit-target lock, the bot kept trading at 10:10 AM and 11:30 AM and gave back `-₹1,332.50` on Trades #7 and #8. Now, as soon as daily net P&L hits **+₹1,500** (or gross hits **+₹1,650**), the bot halts for the day and locks the green bag!
  2. **Anti-Exhaustion & Option Spike Filter:** Trade #7 lost `-₹656.50` in 41 seconds because it bought `22600 PE` at 10:10 AM when 3m RSI was already oversold (`26.8`) and the option price (`₹174.35`) was stretched **+25% above its 3m EMA9 (`₹139.60`)**. Now `RSI < 30` (for SHORT) / `RSI > 72` (for LONG) and `Option LTP > 1.15 * EMA9` are strictly blocked.
  3. **Midday Chop & MACD Sign Filter:** Trade #8 lost `-₹676.00` at 11:30 AM because it shorted while 3m `MACD_Hist` was positive (`+7.41`) and `EMA21` was flat. Now `SHORT` requires `MACD_Hist < 0` and 3-bar `EMA21` slope `<= -2.5 pts` (`LONG` requires `MACD_Hist > 0` and `EMA21` slope `>= +2.5 pts`).
  4. **Multi-Trade Same-Contract Reconciliation Fix:** `cmd_reconcile` now matches individual order fills from `get_order_list()` when the same `securityId` is traded more than once in a day.

- **2026-10-26 (Day 4 — Fast-Strike v5 Multi-Index Scouting):**
  1. Expanded scouting from Nifty 50 only → **all 5 indices in parallel** (Nifty 50, Nifty Bank, FinNifty, Nifty Next 50, Nifty Midcap Select). `Weapon A` trades the single strongest signal (highest confidence) across the whole universe instead of waiting for Nifty 50 alone to produce a setup.
  2. `get_nearest_nifty_option()` generalized with `under_security_id` / `lot_size` / `index_name` params; `INDEX_UNIVERSE` config holds verified Dhan index security IDs (`13`, `25`, `27`, `38`, `442`) and lot sizes (`65`, `30`, `60`, `25`, `120`).
  3. Converted `dhan-survivor-heartbeat` cron to `no_agent=True` (zero LLM token cost) backed by `survivor_tick_watchdog.py` + `--quiet` flag so idle scans stay silent and only trade entries/exits/ratchets/EOD/mortality events ping Telegram.
  4. **Equity SL Distance Fix (EOD Audit):** Day 4's first 4 trades were all `EQUITY_5X` (HDFCBANK, ICICIBANK). Trade #11 (ICICIBANK LONG 79 qty) was stopped out in just **6 seconds** because the equity SL was only **0.6%** (`stop_dist = max(entry_px * 0.006, atr * 1.2)` = ₹8.34 on a ₹1,390 stock — normal bid-ask noise hit it instantly). Fixed to `max(entry_px * 0.012, atr * 2.0)` — **1.2% minimum or 2x ATR**, giving equity trades room to breathe without getting whipsawed.
  5. **Journal Exit Price Fallback Bug (EOD Audit):** When `sl_order_id` was `null` (SL rejected by Dhan RMS), `_finalize_closed_trade()` used the **SL trigger price** as the exit price instead of the actual fill price. The journal recorded Trade #11 as a **-₹721.76 loss** when Dhan's actual `sellAvg` showed it was a **+₹845.60 profit** (ICICIBANK sold at ₹1,402.10 vs entry ₹1,390.70)! Fixed by adding a fallback in both `_finalize_closed_trade()` and `cmd_reconcile()`: when `sl_order_id` is null/None and the order-list time search fails, use the Dhan position book's `sellAvg` for that `securityId` as the exit price. Lifetime record corrected from 3W/9L → **5W/7L** and Day 4 was actually a **+₹695 GREEN day** (not -₹951 as initially recorded).

- **2026-10-27 (Day 5 — DH-905 Root Cause, Tick Size Fix, State Locking & SL Retry):**
  1. **Tick Size Fix:** `place_protective_sl_order()` and trailing SL modify were hard-coded to `0.05` tick size. NSE options with strike price < ₹100 use `0.025` tick size, causing `EXCH:16316: trigger price is not a multiple of tick size` rejections. Fixed to use `0.05` for prices ≥ ₹100, `0.025` for prices < ₹100.
  2. **Token Renewal Race Condition:** Added file-lock (`O_CREAT|O_EXCL`) around token renewal in `reconnect_with_fresh_token()` so multiple cron processes don't race to renew simultaneously.
  3. **DH-905 Error Code Clarification:** DH-905 is a generic "Input_Exception" code with multiple meanings — "Invalid IP", "Missing required fields", "securityId is required". The IP whitelist update takes 5–10 minutes to propagate on Dhan's backend after saving on dhanhq.co.
  4. **Entry Order Tick Size Fix:** NSE option LIMIT orders were being rejected with `EXCH:16283: order price is not multiple of tick size` because `price=round(opt["ask"], 2)` doesn't align to NSE tick sizes. NSE uses `0.05` tick for options ≥₹100 and `0.025` for options < ₹100. When the entry BUY LIMIT at ₹93.74 was rejected, no position was opened, but the engine still tried to place 4 SL orders — each rejected as "insufficient funds ₹1,75,162" because a naked SELL SL on options without a matching long position requires full short margin. Fixed by rounding all entry/SL/TP prices to proper tick size before placing orders.
  5. **Equity SL/TP Tick Size Fix:** Same tick-size rounding applied to equity SL and TP prices (0.05 tick for NSE equities).
  6. **State File Race Condition (2-Consecutive-Loss Halt Bypass):** Day 5 saw 4 consecutive losses (0W/4L) when the 2-consecutive-loss halt should have stopped after Trade #14. Root cause: concurrent `*/5` cron ticks could race on `survivor_state.json` — one process writes `daily_halted=True` while another overwrites with a stale state having `daily_halted=False`. Fixed by adding cross-process file locking (`O_CREAT|O_EXCL` lock file + atomic `os.replace` temp-file rename) to both `save_state()` and `load_state()`.
  7. **SL Order Rejection Retry:** Trade #15's exchange SL was rejected by Dhan RMS (`sl_order_id=null`), leaving only the 5-second monitor loop as protection. The option peaked at +11.9 pts but fell back and exited at -₹26 gross (₹1.65 slippage from the 5s polling gap). Fixed `place_protective_sl_order()` to retry up to 3 times on RMS rejection before falling back to monitor-only mode.

## Mandatory Agent Protocol on `[SURVIVOR_SEPUKU_TRIGGERED]` or `DEAD_COMA`
When a scheduled cron run or manual tick outputs `[SURVIVOR_SEPUKU_TRIGGERED]` or reports `DEAD_COMA`:
1. Call `cronjob(action='list')` to locate all active trading cron jobs.
2. Call `cronjob(action='remove', job_id=...)` on the survivor cron job(s) so the bot never wakes up again.
3. Read `C:\Users\visha\nifty-algo-trading\survivor_obituary.md` and deliver the complete Death Certificate & Post-Mortem to the user.
4. **Do NOT delete the skill or code files** (Option 2: Permanent Coma preserves the body/logs for inspection).
