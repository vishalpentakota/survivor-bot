# Round 5 exit closure refusal and restart cases

Each row maps to the named subtest in `test_closure_refusal_restart_matrix`
in `test_exit_invalidation.py`. Each subtest starts with a committed ledger
and journal proof and an active trade whose risk update has yet to be applied.
It calls `closure()` under the ledger lock, then restarts with the original
tick state. It checks persisted state apart from the tick heartbeat, an unchanged journal,
a nonzero tick status, and only the original sell. Contradiction cases then restart again
with the position endpoint unavailable.

| Refusal in `ExitCoordinator.closure()` | Class | Matrix case |
| --- | --- | --- |
| Existing flatten cycle blocks closure | Unavailable evidence | `flatten_cycle` |
| Existing cycle with settlement disabled | Unavailable evidence | `settle_disabled` |
| Order list or position snapshot read fails | Unavailable evidence | `unavailable_orders`, `unavailable_positions` |
| Order book changes across the position read | Unavailable evidence | `moving_snapshot` |
| Stable snapshot reports nonzero broker exposure | Newer contradictory evidence | `nonzero_position` |
| Stable snapshot reports a working order | Newer contradictory evidence | `working_order` |
| Order status or execution fields cannot be validated | Unavailable evidence | `unknown_status`, `malformed_order` |
| List and detail disagree on a saved execution | Unavailable evidence | `disagreeing_saved_detail` |
| List and detail agree on a change to a saved execution | Newer contradictory evidence | `changed_saved_fill` |
| Cycle has no pending order to settle | Unavailable evidence | `no_pending` |
| Settlement cannot verify the pending intent | Unavailable evidence | `settle_refused` |
| List and detail agree on a changed baseline fill quantity | Newer contradictory evidence | `changed_baseline_fill` |
| List and detail disagree on the changed baseline fill | Unavailable evidence | `disagreeing_baseline_detail` |
| Execution net cannot be verified from the order data | Unavailable evidence | `unavailable_net_detail` |
| Corroborated executions imply a nonzero net | Newer contradictory evidence | `nonzero_execution_net` |
| Saved execution ID is absent from the book | Unavailable evidence | `missing_saved_order` |
| Final stable snapshot reports nonzero exposure | Newer contradictory evidence | `nonzero_final_position` |
| Final stable snapshot reports a working order | Newer contradictory evidence | `working_final_order` |
| Final stable book differs from the verified book | Unavailable evidence | `moving_final_snapshot` |

The cycle cases exercise the closure gates independently of the existing
intent fence. The restart matrix in `test_exit_restart_matrix.py` also covers
naturally persisted newer intents, lost acknowledgements, partial fills, and
journal and state commit failures.

| Persistence cut or control | Test |
| --- | --- |
| Initial fence write fails before any observation, or recording the contradiction fails after the fence commits | `test_fence_write_failure_before_read_or_after_contradiction` |
| Process exits during broker observation, followed by a failed tick state save and restart | `test_interrupted_read_leaves_fence` |
| Complete fresh agreement after a contradiction or unavailable evidence permits once-only risk recovery | `test_fresh_agreement_clears_fence_and_applies_once` |
| Clean exact match preserves zero broker reads on replay and applies risk once | `test_exact_match_preserves_zero_read_once_only_replay` |

The initial fence is committed before contacting the broker. An interrupted
read, unavailable evidence, or failed follow-up write leaves verification
required on disk. A corroborated contradiction records its own status on the
same fence. Both statuses preserve the saved executions for comparison with
fresh proof.
