# Initial preparation and snapshot diagnostics / 首次准备及快照诊断

2026-11-17 · SHA256 `662d2b0815bf7bfcdf96856dfae6525c85107098933c1e3ba4667aef4a068a5a` ·187940067bytes.

| Check | Accepted scope | Evidence |
|---|---|---|
| App build / actual JAR first |364pass each,zero failures/errors/skips;conditional native integration classes explicitly excluded |[build](app-build.log),[unit](release-unit.log)|
| UI / artifact |79DOM,Chinese/English initial-stage guidance,no invented incremental sequence;JAR resource/class-version check |[UI](ui.log),[artifact](artifact.log)|
| Native journal |PostgreSQL17 and MySQL8.4 each10paused-head contracts,including refusing snapshot/preparation as failed incremental transactions |[PG](postgresql-manifest.json),[MySQL](mysql-manifest.json)|
| Five JREs |Java8/11/17/21/25,33H2/foundation cases each=165,arm64/256MiB |[manifest](runtime-manifest.json)|
| Real business |2controllers,independent Agent/Worker,LOCAL/H2,MySQL8.4 two databases→PostgreSQL17/MySQL8.4,PG17control;19checks |[business](business.log),[manifest](business-manifest.json)|

Real acceptance first denies business-schema CREATE while metadata initialization remains allowed. Both entries show TARGET_PREPARE/SQL_42501 with no failed incremental sequence,while healthy MySQL snapshots complete. Grant CREATE and install a CHECK blocking the first snapshot row:both entries show TARGET_SNAPSHOT/SQL_23514 under the next error generation. Stop/start Worker preserves that pause. Remove CHECK and resume with the same generation;exact snapshot and incremental contents catch up. Existing management/store outages,offline durable STOP,SQL apply fault/restart,configuration/history and common-cut validation/repair also pass.

`SnapshotDispatcher` now tracks activation,projection,DDL preparation,snapshot delivery,journal part reads and ACKs. Unit contracts verify native part checkpoint replay after a lost reply,including no duplicate rows;corrupt source parts and failed journal ACKs remain global storage failures rather than destination SQL pauses. Journal ACK failure after native snapshot completion replays safely. Health/epoch failures preserve global fencing.

This is RC-01/02 partial progress. Complete source connection/read/decode/DDL diagnostics,raw UI review,manual skip/compensation,gaps and cross-node evidence workflows remain open. Five-JRE foundations are not a full source/target/JDK/SDK/TLS matrix. No new command kind or checkpoint semantics were introduced. Old software may display new diagnostic phases as Unknown;actual mixed-binary upgrade is not accepted here.

[Candidate](candidate-manifest.json) binds the frozen JAR,compiled tests and [source manifest](source-manifest.json). App-only build reused unchanged SDKs. The subsequent durable-evidence source work is not part of this JAR. [Harness snapshots](harness/) preserve this exact phase scope. Temporary containers/volumes/networks were cleaned;no permanent preview/image was created.

本轮不增加DONE项；首次准备与快照故障诊断及恢复已验收，完整人工恢复仍未完成。后续证据存储实现需独立验收。
