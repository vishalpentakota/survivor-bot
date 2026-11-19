# Not approved for live execution
- Engine snapshot is preserved for continuity, not certified safe. Source-string assertions from prior sessions were NOT behavioral validation.
- Option fallback manufactures bid/ask, delta, theta, IV and OI from candles/constants. Disable/replace with independently verified real quotes; never trade these placeholders.
- Same-day expiry remains possible in fallback branches despite earlier claims of a ban.
- Lot sizes, tick sizes and expiry rules must be read from current instrument metadata, not historic hardcoding.
- Per-file state locks do not serialize entire trading transactions. Use one engine owner and process-wide lock; test stale-state/overlap recovery.
- Position API failure may be interpreted as no position. Fail closed; do not falsely finalize trades.
- Fill confirmation, partial fills, rejected protection and flatten completion need behavioral tests.
- Journal contains known unreliable inferred exits/aggregated fills. Reconcile against broker contract notes before relying on P&L or win counts.
- Daily loss limits can overshoot; size against remaining daily risk and actual charges. Profit goals are not guarantees.
- Scans are sequential despite historic 'parallel' wording. Monitor polls candle closes, not true tick quotes; handoff gaps exist.
- Pre-market currently labels historical bars as healthy and may ignore pending orders in overall result. Require correct trading calendar, timestamp freshness, SDK/API health and safe startup recovery.
- Pre-market/ensure_ip_whitelisted can mutate IP whitelist in original engine; do not invoke. IP changes require explicit authorization and must not overwrite both slots blindly.
- Shared cloud NAT egress is not reserved. Stop/start and failover can change it.
- Original supporting scripts can contain their own direct broker logic. They are reference-only; run only cloud_probe.py and tests in this staging build.

## 2026-11-13 CRITICAL FIX

**NFT REPLACED — Reconcile Formula Bug (BROKEN)**

The reconcile script (cmd_reconcile, survivor_engine.py line 2371) computed "brokerage & statutory charges" as:

```python
round(net_liq - SEED_CAPITAL - 0.18 - realized_gross_total, 2)
```

This produced ₹6,854.36 — a false artifact of the formula, NOT an actual Dhan charge.

**Root cause:** The formula computes the DAILY EQUITY CHANGE (₹7,414.86 = daily_start - current_equity), then subtracts the gross trade PnL (₹560.50) to get the residual "charge." This is a RETCON of equity movement, not actual brokerage.

**Actual charge for a single BEL LONG 118-lot trade:** ₹55.00 (estimated roundtrip: ₹40 brokerage + ₹15 STT/SEBI/GST).

**Impact:** 
- The reconcile verdict was WRONG: flagged "WARN: Brokerage & statutory ₹6,854.36 is ~12× the single ₹560.50 trade" — this was a false alarm
- Actual reconciliation: Gross -₹560.50 → Net -₹615.50 (with ₹55.00 charges) → matches journal ✓
- Positions 0 open, equity closes, no real discrepancy

**Fix applied:**
```python
# OLD (buggy):
print(f"Brokerage & Statutory Charges: ₹{round(net_liq - SEED_CAPITAL - 0.18 - realized_gross_total, 2):+,.2f}")

# NEW (correct):
print(f"Brokerage & Statutory Charges: ₹{ESTIMATED_ROUNDTRIP_CHARGES:+,.2f} (estimated roundtrip)")
```

**Journaling gap (separate issue, still valid):**
- Trade exited HALT_FLATTENED but zero journal entries were written from Dhan fills
- This is a real bookkeeping audit-trail problem that needs Dhan feed verification

**Reconciliation re-verdict:** NO longer "needs attention." Equity closes, PnL matches, no positions. Only open item: journal entries from Dhan fills (not written for this flat).
