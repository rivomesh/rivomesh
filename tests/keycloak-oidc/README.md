# Rivomesh and Keycloak browser interoperability

This acceptance fixture uses an ephemeral Keycloak 26.7.4 realm, a PostgreSQL 17 control store, two actual Temurin 21 Controllers, a shared HTTPS ingress and Chromium. The test starts with empty storage. The provider imports a dedicated confidential client with one exact callback, standard authorization code flow, PKCE S256 and RS256. The provider signs an administrator-looking test claim while Rivomesh binds the subject to a local Viewer with one task and one node grant. [Keycloak startup realm import](https://www.keycloak.org/server/importExport) · [Keycloak administration guide](https://www.keycloak.org/docs/latest/server_admin/index.html)

The Keycloak container serves TLS directly and advertises a fixed HTTPS issuer. Both origins have separate DNS aliases in the disposable Compose network and certificates from one short-lived fixture CA. For readiness only, the provider and ingress publish random ports on `127.0.0.1`. Chromium imports the root into NSS in its disposable container and uses its normal certificate verification. Each Controller imports that root in a fixture JVM trust store and verifies Keycloak's endpoint hostname. The fixture uses no system trust changes or browser certificate bypass. [Keycloak container configuration](https://www.keycloak.org/server/containers) · [Keycloak hostname configuration](https://www.keycloak.org/server/hostname) · [Chromium Linux certificate management](https://chromium.googlesource.com/chromium/src/+/HEAD/docs/linux/cert_management.md)

The provider and console URLs use distinct hostnames so the browser isolates the cookies. The test ingress sends the authorization start to Controller A and the callback to Controller B. Browser fetches can explicitly address either Controller behind that same HTTPS origin. The fixture records only route names and status counts. It keeps generated credentials, private TLS material and persisted state in an owner-only temporary directory and passes Chromium credentials via stdin. Docker Compose removes its own containers, network and volumes after every run.

## Reproduce

Use Docker Compose, OpenSSL supporting `-addext`, Python 3, a local JDK 21 for the fixture trust store, and an existing Rivomesh release candidate JAR. Run from the fixture directory.

```bash
python3 keycloak-oidc.py --prepare-only \
  --artifact /absolute/path/to/rivomesh.jar --jdk-home /absolute/path/to/jdk21 \
  --output /absolute/path/to/preflight
python3 keycloak-oidc.py --build-browser --artifact /absolute/path/to/rivomesh.jar \
  --jdk-home /absolute/path/to/jdk21 --output /absolute/path/to/native-evidence
```

Preflight creates and verifies the test certificates and topology description without invoking Docker. Native verification starts the actual Keycloak, PostgreSQL, Controllers and browser. The build option tags the browser image for this run and removes that tag after checking its ownership label. To use an existing prepared image instead, supply `--browser-image`. Compose emits a manifest with the exact JAR digest, image IDs, safe check names and cleanup result. Reuse explicit image digest overrides when repeating an accepted profile.

## Acceptance boundaries

| Check | Evidence |
| --- | --- |
| TLS and fixed issuer | Trusted HTTPS discovery and provider JWKS, issuer and endpoint match, S256 and HTTP Basic client auth advertised |
| Local account binding | Browser admin session requires CSRF on Controller B, existing local Viewer gains only the exact provider subject binding |
| Native code flow | Chromium reaches the real Keycloak form over HTTPS, observes the authorization code request with S256 PKCE, and succeeds via the callback on Controller B |
| Authorization and refresh | An administrator-looking provider claim grants only the local Viewer role and scoped task and node, with one Secure HttpOnly session cookie across both entries and reload |
| Revocation | Revoke the local binding on Controller B and observe 401 for the same former session on both entries, followed by denial from the still-authenticated provider browser |
| Masking | The shared audit identifies binding, callback and revocation without exposing callback codes, token credentials or the raw subject |

The native profile tests local account binding and revocation. Automatic provider trust discovery, signing-key rotation, third-party logout notifications and production identity deployment remain separate profiles. The focused `OidcBrowserLoginTest` supplies malformed-claim, one-time-state and callback-replay cases.
