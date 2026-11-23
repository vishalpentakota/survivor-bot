# t_2b6c9339 entry and protection acceptance

From the staging directory containing the immutable `qa_offline.py` and
`candidate/`, run:

```sh
env -i PATH=/usr/bin:/bin LC_ALL=C TMPDIR="$PWD" python3 -I -B qa_offline.py entry_protection
```

The harness installs the OS socket filter before loading candidate code. The
suite uses the candidate's real role adapter, tick dispatch, ledger, entry,
cancellation, protection, monitor and closure paths. Broker list, detail and
position responses advance independently. Entry and exit waits collapse, and
the monitor uses a deterministic clock. Option strategy signals and candidate
selection are controlled inputs.

| Test or scenario | Required observation |
| --- | --- |
| Cancel request accepted, order remains working | One entry intent, zero trades and zero stops. A later terminal zero fill with a corroborating flat position clears the intent. |
| Failed cancel with late partial or full fill, including position lag | Entry continues to fence scans until terminal fill and signed position agree. One opposite stop covers the actual confirmed quantity. |
| Missing acknowledgement, absent or unreadable order, regressing fill, position ahead | Persist the original intent and alert. No second entry, replacement stop or competing market exit. |
| Stop detail ID, security, exchange, product, side, quantity, order type, trigger or limit disagree | No verified stop is published. A later independent, matching resting read can complete the original intent. |
| Stop rejected or cancelled, corroborated terminal state | Verified close route. No estimated stop in the trade record. |
| Working partial stop | Cancel by verified identity, await terminal status, then market close only the remaining confirmed net. |
| Stop acknowledgement lost, stop acknowledgement save interrupted, or active handoff interrupted | Reopen the existing intent from disk and count the opening fill once. |
| Amendment fails explicitly | Keep the previous verified trigger and limit. A later iteration can retry once the old stop is verified. |
| Amendment acknowledgement unknown or accepted with stale stop reads | Keep the previous verified trigger and limit and retain the amendment intent. Matching resting evidence commits the change once. |
| Monitor finds stop unreadable or terminal | Escalate unverifiable protection. Terminal/corroborated exposure proceeds through the verified exit coordinator. |
| Options call/put tick, long/short monitor, bounded entry chase, contract lot | Correct direction and order identity; tick-aligned entry, stop trigger, stop limit and trailing values. Never round a confirmed residual up or down to place an SL. |
| Verified SL or market exit closes | Journal records actual executions; a proposed or unverified stop is never published as the protection price. |

The durable entry workflow is stored in the exit ledger's existing cycle. An
opening fill can be carried as a provisional active trade while protection is
unverified. `sl_order_id`, `stop_loss` and `sl_limit_price` record verified
protection for managed entries. `stop_execution_id` can identify a partially
executed stop for closure attribution. The amendment intent lives in the
active trade. Pending status returns a nonzero tick result and the existing
ledger outbox retains escalation across restart. An unreadable broker does
not establish whether a submitted order is resting or cancelled.

Contract metadata can be supplied as `{"NSE_FNO:23":{"tick_size":0.125,"lot_size":3}}`
in the existing `.tick_size_cache.json`. Legacy security-ID tick entries
remain supported. Option selection also accepts contract metadata from the
option leg and uses the strategy's existing lot configuration and NSE price
step as defaults.

A broker submission whose acknowledgement was lost and whose identity never
becomes visible stays unresolved. A stop amendment that remains ambiguous
also fences further amendments. Scheduled and manual liquidation retain
their existing verified order/position reconciliation and lock ownership.
