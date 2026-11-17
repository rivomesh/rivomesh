# First-release configuration baseline

No first release has been published. In-place upgrades between development candidates,old package aliases and automatic control-mode migration are unsupported. Internal format numbers are not released versions.

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

V2/V3 execution consolidation is a separate pending batch. See[current status](implementation-status.md)and[SDD](../sdd/001-control-recovery-compatibility/spec.en.md);cleanup does not close unrelated feature gaps.
