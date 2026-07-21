# Crash-recovery tests

These tests will kill the runtime around journal commits, projection writes,
checkpoints, worker claims, leases, and completions, then verify safe recovery.

