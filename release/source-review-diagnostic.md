# t_c53a542c source review diagnostic

Candidate application commit `6c223027251f5d1296c9e43c515384d3c4254607`.
The diagnostic used deployed files from the local release baseline and
candidate files with the standard export payload. It called
`review_sources` and `apply_source_review` from the release harness
without starting the application or executing the tests. All focused
subdelta checks passed against the pinned QA source. The open scopes
below retain their exact deployed and candidate fingerprints in the ledger.

### `app/cloud_guard.py`

Source pair matches. 1 unreviewed scope.

- `<module>`

### `app/cloud_probe.py`

Source pair matches. 0 unreviewed scopes.

### `app/dhan_broker.py`

Source pair matches. 4 unreviewed scopes.

- `<module>`
- `DhanTokenManager._load_state`
- `DhanTokenManager._refresh_token`
- `DhanTokenManager._renew_token`

### `app/risk_manager.py`

Source pair matches. 0 unreviewed scopes.

### `app/survivor_engine.py`

Source pair matches. 77 unreviewed scopes.

- `<module>`
- `StateSnapshot.__init__`
- `StateSnapshot.copy`
- `SurvivorDhanClient.__init__`
- `SurvivorDhanClient._candle_response`
- `SurvivorDhanClient._connect`
- `SurvivorDhanClient._poll_order_terminal`
- `SurvivorDhanClient.emergency_flatten_all`
- `SurvivorDhanClient.ensure_ip_whitelisted`
- `SurvivorDhanClient.fetch_candles`
- `SurvivorDhanClient.get_nearest_nifty_option`
- `SurvivorDhanClient.place_order_with_ip_retry`
- `SurvivorDhanClient.place_protective_sl_order`
- `SurvivorDhanClient.reconnect_with_fresh_token`
- `_abort_unfilled_entry`
- `_acquire_owner_lock`
- `_actual_charges`
- `_actual_rows`
- `_apply_exit_risk`
- `_broker_flat`
- `_chase_unfilled_entry`
- `_chase_unfilled_entry._filled`
- `_closure_amounts`
- `_closure_fields`
- `_cmd_tick_inner`
- `_entry_chase`
- `_entry_tick_result`
- `_execution_counts`
- `_exit_coordinator`
- `_extends_proof`
- `_finalize_closed_trade`
- `_flatten_trade`
- `_get_tick_size`
- `_halt_exit_unknown`
- `_inspect_active_stop`
- `_inspect_active_stop.amend_allowed`
- `_instrument_metadata`
- `_journal_proof`
- `_premarket_with_state`
- `_proof_executions`
- `_publish_tick_exposure`
- `_read_exit_journal`
- `_read_state_storage`
- `_reconcile_incomplete`
- `_reconcile_with_state`
- `_record_halt_exit_unknown`
- `_recovery_pending`
- `_release_owner_lock`
- `_resolve_role`
- `_resume_entry`
- `_resume_entry.publish`
- `_run_emergency`
- `_safe_get_positions`
- `_save_actual_totals`
- `_save_state_update`
- `_scheduled_flatten`
- `_scheduled_flatten.budget`
- `_scheduled_flatten.finish`
- `_start_entry`
- `_start_entry.check_before_send`
- `_start_entry.market_verified`
- `_tick_with_state`
- `_trade_identity`
- `_verified_trade_fills`
- `append_journal`
- `cmd_flatten`
- `cmd_premarket`
- `cmd_reconcile`
- `cmd_status`
- `cmd_tick`
- `main`
- `monitor_active_trade_loop`
- `scan_equity_candidates`
- `send_telegram`
- `state_transaction`
- `trigger_permanent_coma`
- `verify_option_chart_momentum`

**Source review remains blocked by 82 unreviewed scopes.** All QA catalog test
source hashes and method names match the candidate export payload.
