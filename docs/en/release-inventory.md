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

The comparison records both SHA-256 digests, Maven coordinates inside the JAR, embedded license and notice hashes and known vendor JDBC markers. Review coordinates present only in the JAR and dependencies without declared license metadata. The GBase8c ZIP/JAR fetched by its build module and the shaded GaussDB driver both require review against the actual packaged distribution. Oracle source preflight uses an operator-provided driver.

The current run found 175 Maven components, 109 embedded coordinates, six embedded coordinates outside the aggregate list, nine components without a declared license and two vendor markers. These are inventory results. Distribution review covers source ownership, the actual contents of each distribution, and the applicable LICENSE and NOTICE text. [ASF third-party licensing policy](https://www.apache.org/legal/resolved)

Use the project name **Rivomesh** and namespace `io.github.rivomesh` during preparation. The ASF proposal, naming and branding guides define the later review and adoption process. [Proposal guide](https://incubator.apache.org/guides/proposal.html) · [Name search](https://incubator.apache.org/guides/names.html) · [Branding](https://incubator.apache.org/guides/branding.html)

## Candidate evidence gate

Keep an immutable copy of the candidate JAR and pass it to the [Chrome](../../tests/ui/browser-smoke.mjs), [native CDC](../../tests/global-control-pipeline.py), and [Kubernetes](../../tests/kubernetes-csi-runtime.py) fixtures. Set `--artifact-sha256` on the Kubernetes fixture to verify the JAR inside both running workloads. Pass the exact candidate and each generated manifest to the gate.

```sh
python3 tests/release-evidence-gate.py \
  --artifact target/candidate/rivomesh.jar \
  --browser target/candidate/browser/manifest.json \
  --kubernetes target/candidate/kubernetes/manifest.json \
  --native-cdc target/candidate/native-cdc/manifest.json
```

The gate also checks SDD status and evidence, local links, bilingual API and connector coverage, packaged resources, and all shipped YAML examples using synthetic credentials. `--examples-only` runs these checks by themselves. The full command requires a real browser, successful native recovery with separate Agents, and both live Kubernetes deployment profiles on the same artifact. Record separate native results for each additional adapter or deployment profile on the [verification baseline](../verification/README.md).
