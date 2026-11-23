# t_c53a542c operator regressions on 6fabddd

## Exact test cases

| Test module | Test method | Assertion |
| --- | --- | --- |
| `test_operator_guard_client` | `test_read_only_exit_method_is_denied_even_with_live_switch` | Direct exit-method call fails under READ_ONLY with zero SDK submits. |
| `test_operator_guard_client` | `test_authorized_flatten_waits_for_orders_and_position_to_agree` | A 66-unit position and 65 executed units fence the authorized command. Converged proof permits one 65-unit market sell, and replay adds no order. |
| `test_operator_guard_client` | `test_read_only_uses_near_expiry_cache_and_denies_at_expiry` | Cache remains readable until expiry. At expiry, both token access and forced renewal deny without lock acquisition, replacement, or renewal. |
| `test_operator_guard_client` | `test_refresh_failure_cleans_lock_and_can_retry` | The one-hour renewal margin forces refresh of a five-minute cache. Failure releases the lock and a later request retries. |
| `test_operator_guard_client` | `test_failed_atomic_replacement_preserves_current_token_and_retries` | Mocked replacement failure cleans temporary and lock paths and preserves the prior in-memory token and expiry. Retry persists the new token and its expiry. |
| `test_operator_guard_client` | `test_env_loader_precedence_and_return_value` | Mocked file values include excluded keys in the return value. Ordinary keys load from file, the process live switch takes precedence, and file fallback authorizes the invocation without exporting the switch. Missing files return an empty mapping. |
| `test_cloud_probe_cli` | `test_default_cli_only_reads_public_ip` | Standalone CLI default performs a single mocked public IP GET. |
| `test_cloud_probe_cli` | `test_broker_cli_reads_using_cache_only_under_live_switch` | Standalone `--broker` takes the cache-only read-only client path. The whitelist, funds, positions, orders and mocked candle read are checked, with zero renewal or token writes. |
| `test_cloud_probe_cli` | `test_broker_cli_without_cache_stops_before_sdk_and_dhan_http` | Standalone `--broker` with a cache miss fails before SDK construction or Dhan HTTP. |
| `test_operator_option_pricing` | `test_option_chase_uses_verified_ltp_and_caps_one_amendment` | Real tick and `_start_entry` use broker LTP 100 despite bid 80, ask 130 and chain LTP 90. A later LTP of 101.375 yields one amendment at 101.5 on the original buy. One five-unit stop protects the confirmed fill, including on recovery. |
| `test_operator_option_pricing` | `test_option_ltp_beyond_cap_cancels_original_without_replacement` | An LTP of 101.625 cancels the sole buy order with zero amendments and zero exposure. |
| `test_operator_option_pricing` | `test_option_partial_during_chase_quote_cancels_then_closes_only_residual` | A two-unit fill becomes visible during the chase quote. The engine skips the amendment, verifies cancellation of the original order, and sends one two-unit market sell. Net returns to zero and replay adds no order. |

## RED and GREEN

With the new tests on the original application bytes, nine passed and three
failed. Token replacement failure exposed the unpersisted replacement in
memory. The exact 101.50 cap floored to 101.375 due to binary floating-point
rounding. A partial fill during the quote read still allowed an amendment.
The source patch publishes token state after persistence, computes the cap
in decimal, and repeats the order and exposure check after the quote read.
All 12 focused cases passed under the socket-denied runner. Approved tests
and source adjudications received no edits.

The probe test is in `tests/test_cloud_probe_cli.py`. Its standalone CLI
checks run against the staging checkout because the source archives exclude
the standalone probe; the focused socket-denied checkout run passed 3/3.

## Coverage boundaries

These cases cover simulated broker evidence and file operations. Remaining
behaviors include fills after the final broker snapshot, exchange treatment
of a concurrent amendment, live LTP and order behaviour, probe read
failures, token provider retry timing and OAuth, and filesystem durability
under power loss. Operational rollback and unresolved source adjudications
remain outside this run.

## Gate inputs

The replay used a fresh export of the staging candidate, the prior scratch
rollback archive with its SHA-256 verified against its parity report, and
the dependency tree already staged by the offline runner. A dependency
bootstrap from that staged tree could not enumerate distribution RECORD
files. The replay reused the staged packages and ran the existing
socket-denied CLI, test, fixture and rollback entry points directly.
