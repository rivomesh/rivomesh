# Capture recovery

[中文](../zh-CN/capture-recovery.md)

When a V3 task enters `ERROR` during source connection, reading, decoding, snapshot, discovery, DDL or normalization, an administrator can open **Review capture recovery** in the failure card. In a fixed-Agent deployment either controller routes the read to the owning Agent. The report lists potentially affected subscriptions, the original checkpoint hash, source admission and the recovery steps.

```sh
java -jar rivomesh.jar review-source /etc/rivomesh/tasks/pipeline.yml
```

The review checks the pipeline identity, original position and snapshot dependencies. It performs admission reads against the source. Refresh and repeat the review if the task generation, checkpoint or configuration changes during the check.

| Observation | Recovery step |
|---|---|
| `SOURCE_LOG_GAP` | Preserve the GTID and any retained binlogs. Restore the missing logs or have the database operator restore an eligible primary with complete history. Retry the original checkpoint after review. |
| `POSTGRESQL_REQUIRED_WAL_UNAVAILABLE` | Preserve the original slot, required WAL, journal and LSN evidence. Evaluate whether the original slot and WAL can be recovered. Review a new baseline and the affected destinations when recovery is unavailable. |
| `SOURCE_CHECKPOINT_AVAILABLE` after decode, DDL or snapshot failure | Preserve the original event, resolve the decoder, schema or read permissions, then retry. Admission confirms the original source position meets the recovery checks. |
| `UNPROVEN_SOURCE_RANGE` | Verify transaction boundaries and contents against the original logs and checkpoint. Include every subscription in the potential impact. |

### FAQ

**Why does the destination have data while capture remains paused?** The destination may retain data delivered before the failure. Check the source checkpoint and each subscription acknowledgment to establish its progress.

**When can I skip a transaction?** Use the [transaction skip](transaction-skip.md) workflow when a complete destination transaction is available for review, the target state is established and the approval requirements are met. RC-04 tracks subsequent source-range quarantine and approved exclusion.
