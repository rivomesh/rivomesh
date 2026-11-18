# Release inventory and Apache preparation

[中文](../zh-CN/release-inventory.md)

Generate the dependency inventory from the repository root after building a release candidate. This produces one CycloneDX Maven SBOM and a comparison against the actual shaded JAR. [CycloneDX Maven plugin](https://cyclonedx.github.io/cyclonedx-maven-plugin/)

```sh
mvn -B -s build/settings.xml \
  org.cyclonedx:cyclonedx-maven-plugin:2.9.1:makeAggregateBom \
  -DoutputFormat=json -DoutputName=rivomesh-cyclonedx \
  -DincludeTestScope=false -Dcyclonedx.skipAttach=true
python3 tests/release-inventory.py \
  --artifact sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar \
  --maven-bom target/rivomesh-cyclonedx.json \
  --output target/release-inventory.json
```

The comparison records both SHA-256 digests, Maven coordinates inside the JAR, and hashes for recognized license files, native libraries and nested JARs. The `requiredComponentReviews` map lists bundled third-party coordinates and JDBC markers, alongside the license names declared by the Maven BOM. An empty list requires investigation of the upstream package. Review BOM omissions and shaded dependencies even when the package does not expose their Maven metadata. For the private Hive SDK, record its bundled dependencies as well as the nested JAR hash. Oracle source preflight uses an operator-provided driver.

The Temurin 21 candidate at SHA-256 `cf103fa235ea3fca9b67d0100b3328b0e219426a775a1c337258b8e4cf25bf40` has 109 embedded coordinates. Six are absent from the 175-component Maven BOM. It has 111 recognized native binaries and nested JARs and 20 recognized license and notice files. Its `META-INF/LICENSE` and `META-INF/NOTICE` are copies from individual dependencies. The current source preserves the exact upstream GaussDB and GBase8c notices under `META-INF/rivomesh-licenses/`. The inventory compares each of those files against the corresponding upstream bytes when the driver marker appears.

## ASF distribution review

The development JAR bundles MySQL Connector/J 8.4.0 Community under GPLv2 with the Universal FOSS Exception. The current ASF Category X policy covers GPL and exceptions it does not specifically allow. The review check reports `BUNDLED_MYSQL_CONNECTOR_J` while this driver is present. Apache Flink CDC documents a separate operator-installed driver. Rivomesh's external JDBC candidate follows that packaging pattern and still needs ASF review of its integration model and all other distributed components. [Connector/J 8.4.0 license](https://github.com/mysql/mysql-connector-j/blob/8.4.0/LICENSE) · [ASF third-party license policy](https://www.apache.org/legal/resolved.html#category-x) · [Flink CDC MySQL documentation](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/docs/connectors/flink-sources/mysql-cdc/)

### Candidate with an external MySQL JDBC driver

Build the distinct JAR and check its contents against the aggregate Maven BOM.

```sh
mvn -B -s build/settings.xml -Pexternal-mysql-jdbc -pl sync-app -am -DskipTests package
candidate=sync-app/target/rivomesh-app-0.1.0-SNAPSHOT-external-mysql-jdbc.jar
python3 tests/release-inventory.py \
  --artifact "$candidate" \
  --maven-bom target/rivomesh-cyclonedx.json \
  --output target/external-mysql-inventory.json \
  --expect-external-mysql
```

The external profile excludes Connector/J from this JAR, including its service provider and bundled notices. The Maven BOM still describes build dependencies. Review `mavenComponentsWithoutEmbeddedCoordinates` alongside the binary inventory to distinguish the remaining build components from packaged components. `--expect-external-mysql` checks the candidate and its nested JARs. The binlog reader remains packaged separately from the JDBC driver.

On a manual installation, obtain a supported Connector/J JAR separately under its applicable terms. Provide the driver from an operator-controlled file wherever the configuration uses a MySQL-protocol JDBC connection, including the source, a target or the control database. Launch with the explicit classpath and main class.

```sh
mysql_driver=/opt/rivomesh-drivers/mysql-connector-j-8.4.0.jar
java -cp "$candidate:$mysql_driver" io.github.rivomesh.Main serve /etc/rivomesh/platform.yml
```

An execution Agent uses `agent` in place of `serve` and inherits the same JDBC classpath into its Workers. For a systemd service that uses this candidate, set its `ExecStart` to the classpath command with absolute paths. The repository's standard Docker build continues to package the development JAR.

Check a MySQL pipeline using the same classpath and `check` with its pipeline configuration. `JDBC_DRIVER_UNAVAILABLE` indicates that the JVM could not load the driver class. Verify the operator JAR path and the process's read permissions.

Run a disposable MySQL 8.4 acceptance profile to exercise the external JAR through Controllers, an Agent and its Worker. This uses the MySQL control store, PostgreSQL and MySQL targets, and a rolling restart.

```sh
python3 tests/global-control-pipeline.py --artifact "$candidate" \
  --external-mysql-driver "$mysql_driver" \
  --output target/external-mysql-native \
  --separate-execution --rolling-restart --control-product MYSQL
```

### Candidate with external vendor JDBC drivers

Build the `external-vendor-jdbc` profile on its own. The resulting application JAR keeps the binlog reader, PostgreSQL and H2 drivers. It excludes MySQL Connector/J, the GaussDB and GBase8c wrappers, Kingbase and Dameng JDBC. The separate `external-mysql-jdbc` profile continues to bundle the four target connectors. Maven's build BOM still includes dependencies used to build and test the application, and the reactor may produce driver JARs separately. [Maven Shade artifact selection](https://maven.apache.org/plugins/maven-shade-plugin/examples/includes-excludes.html)

```sh
mvn -B -s build/settings.xml -Pexternal-vendor-jdbc -pl sync-app -am -DskipTests package
candidate=sync-app/target/rivomesh-app-0.1.0-SNAPSHOT-external-vendor-jdbc.jar
python3 tests/verify-artifact.py "$candidate" --jdbc-profile external-vendor-jdbc
python3 tests/release-inventory.py --artifact "$candidate" \
  --maven-bom target/rivomesh-cyclonedx.json \
  --output target/external-vendor-inventory.json --expect-external-vendor-jdbc
```

The inventory scans the candidate and nested JARs for known classes, JDBC service providers, Maven metadata and identifiable upstream notices. The external profile check covers the binary application candidate. Source archives and deployment images have their own contents and review evidence.

Supply only the driver files needed by a deployment, from locations controlled by the operator. For GaussDB or openGauss, use the project's separately built relocated GaussDB wrapper for the supported profile. Its class name lets it coexist with the bundled PostgreSQL driver. GBase8c can use the verified upstream `gbase8c-jdbc-3.0.0B02.jar`. The existing checked Kingbase and Dameng versions are `kingbase8-9.0.1.jar` and `DmJdbcDriver18-8.1.3.140.jar`.

```sh
driver_dir=/opt/rivomesh-drivers
mysql_driver="$driver_dir/mysql-connector-j-8.4.0.jar"
gauss_driver="$driver_dir/rivomesh-gaussdb-driver-0.1.0-SNAPSHOT.jar"
gbase_driver="$driver_dir/gbase8c-jdbc-3.0.0B02.jar"
kingbase_driver="$driver_dir/kingbase8-9.0.1.jar"
dameng_driver="$driver_dir/DmJdbcDriver18-8.1.3.140.jar"
classpath="$candidate:$mysql_driver:$gauss_driver:$gbase_driver:$kingbase_driver:$dameng_driver"
java -cp "$classpath" io.github.rivomesh.Main serve /etc/rivomesh/platform.yml
```

Use the same launch classpath for the Agent or standalone pipeline. The Agent passes that classpath to its Workers. The offline acceptance checks the exact application JAR with an empty vendor classpath and again with all five operator files. It checks class origins, URL matching, the typed missing-driver error and the Dameng error classification.

```sh
java -cp "$candidate" tests/roles/ExternalVendorJdbcAcceptance.java
java -cp "$classpath" tests/roles/ExternalVendorJdbcAcceptance.java \
  "$mysql_driver" "$gauss_driver" "$gbase_driver" "$kingbase_driver" "$dameng_driver"
```

### Distribution review record

Run the inventory with `--asf-review target/candidate/distribution-review.json` to check an exact JAR against a completed manual review. Use the following fields in that record.

| Field | Review evidence |
| --- | --- |
| `formatVersion` | Integer `1` |
| `artifactSha256`, `mavenBomSha256` | Corresponding SHA-256 values from this candidate's inventory |
| `sourceRightsEvidence` | Reference to the source provenance, rights and contribution review |
| `packagedContentsEvidence` | Reference to the review of the exact package, including nested or shaded dependencies, native libraries, notices and other assets |
| `packageLicenseFiles` | Object mapping `META-INF/LICENSE` and `META-INF/NOTICE` to their reviewed SHA-256 values in the JAR |
| `componentReviews` | Object keyed exactly by `requiredComponentReviews`. Each value has a `license` string for the selected terms and an `evidence` string for the corresponding review record |

The inventory writes `ASF_DISTRIBUTION_REVIEW_BLOCKED` and exits with code 1 on an incomplete record, digest mismatch, missing package licensing file, missing or changed vendor notice, or bundled Connector/J. `ASF_DISTRIBUTION_REVIEW_RECORDED` confirms the record matches the inspected JAR and that the scanner found no known blocker. Reviewers determine the applicable terms and verify all content. Repeat the review for each source archive, binary and image that will be distributed. [ASF distribution licensing requirements](https://www.apache.org/legal/release-policy.html#licensing)

The project-level LICENSE and NOTICE depend on the source rights review. Record the contribution rights and provenance of code, assets and vendor components before preparing those files. For a new podling, follow the initial codebase process in the Incubator's podling IP guide. [Podling IP clearance](https://incubator.apache.org/guides/ip_clearance.html)

Use the project name **Rivomesh** and namespace `io.github.rivomesh` during preparation. The ASF proposal, naming and branding guides define the later review and adoption process. [Proposal guide](https://incubator.apache.org/guides/proposal.html) · [Name search](https://incubator.apache.org/guides/names.html) · [Branding](https://incubator.apache.org/guides/branding.html)

## Candidate functionality gate

Keep an immutable copy of the candidate JAR and pass it to the [Chrome](../../tests/ui/browser-smoke.mjs), [native CDC](../../tests/global-control-pipeline.py), and [Kubernetes](../../tests/kubernetes-csi-runtime.py) fixtures. Set `--artifact-sha256` on the Kubernetes fixture to verify the JAR inside both running workloads. Pass the exact candidate and each generated manifest to the gate.

```sh
python3 tests/release-evidence-gate.py \
  --artifact target/candidate/rivomesh.jar \
  --browser target/candidate/browser/manifest.json \
  --kubernetes target/candidate/kubernetes/manifest.json \
  --native-cdc target/candidate/native-cdc/manifest.json
```

The gate also checks SDD status and evidence, local links, bilingual API and connector coverage, packaged resources, and all shipped YAML examples using synthetic credentials. `--examples-only` runs these checks by themselves. The full command requires a real browser, successful native recovery with separate Agents, and both live Kubernetes deployment profiles on the same artifact. Record separate native results for each additional adapter or deployment profile on the [verification baseline](../verification/README.md). Record the distribution review separately using the same candidate digest.
