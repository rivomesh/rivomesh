# Intermediate maintenance candidate: native failure retained

SHA256 `463994bca812ea4add976f9db8aaa9499704aac777d0b20cefb2383679011714`.

Build338 and actual-JAR-first338 passed. FiveJDK routed controls passed133cases each using the then single-target HTTP fixtures. The real two-target workflow failed: validation completed and paused the pipeline, but receipt comparison reported UNKNOWN/OWNER_RECEIPT_MISMATCH. [Details](failure-details.json) preserve the distinction between actual business result and command certainty. The request Set was normalized at admission but not receipt inspection. Newer source tests now use multiple targets; the fix belongs to a later candidate. No native maintenance PASS is claimed here.
