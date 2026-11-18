# First-release configuration baseline

No first release has been published. In-place upgrades between development candidates,old package aliases and automatic control-mode migration are unsupported. Internal format numbers are not released versions.

The release builds and runs on Eclipse Temurin 21 LTS. Controllers, Agents and Workers use the same Java major version.

Choose the control store,access mode,roles,keys and persistent directories before first deployment. Enable full managed recovery consistently across all members:

```yaml
cluster:
  sharedAccess: true
  sharedTasks: true
  recoveryReview: true
  recoveryCommit: true
  recoverySkip: true
```

Namespaces are created directly in the configured mode. Mismatches refuse immediately,even after every member stops. Existing development environments require a separately assessed new test baseline;deleting code history does not authorize clearing business data or moving only cursors.

Linux/Docker use the same role configuration and independent persistent directories. Helm overlays controller/Agent values with`values-recovery-skip.yaml`;manual Kubernetes can reference`rivomesh-skip-*.yaml`. Examples do not establish liveK8s/CSI acceptance.

Restart recovery,leases/epochs,authorization revocation and damaged/stale-backup refusal are current behavior rather than obsolete development compatibility.

Replace superseded development implementations directly. Do not retain branches,configuration aliases or upgrade tests solely for old prerelease candidates. Documents describe current behavior;duplicate evidence/build snapshots converge on one [verification baseline](../verification/README.md). Keep tests that validate current data correctness,recovery and security boundaries. Remove temporary containers,obsolete project images and intermediate builds once unused.

## Back up and restore a LOCAL journal

Stop the task and close every process connection to its H2 journal before copying the LOCAL directory. Create a coordinated recovery point that includes the task configuration, journal, and destination business data, snapshot metadata and transaction receipts. Include applicable Agent and control-store state for managed tasks. Use each destination's consistent backup procedure and retain the source log history needed by the saved checkpoint. [H2 backup procedure](https://h2database.com/html/tutorial.html#backup_restore)

Restore into isolated storage first. Check the pipeline and journal generation, snapshot IDs, source position, target checkpoint and receipt before resuming. A divergent target checkpoint pauses delivery. A pending journal transaction with a damaged payload checksum fails on read. Preserve the original backup while investigating either failure.

The current-package fixture copies a closed LOCAL journal and an H2 JDBC test destination, feeds synthetic MySQL GTIDs and verifies restore, deduplication, mixed-time checkpoint refusal and pending-payload corruption refusal.

```bash
python3 tests/local-state-restore.py --artifact rivomesh.jar \
  --java "$JAVA_HOME/bin/java" --javac "$JAVA_HOME/bin/javac" \
  --output target/local-state-restore/manifest.json
```

V2/V3 execution consolidation is a separate pending batch. See[current status](implementation-status.md)and[SDD](../sdd/001-control-recovery-compatibility/spec.en.md);cleanup does not close unrelated feature gaps.
