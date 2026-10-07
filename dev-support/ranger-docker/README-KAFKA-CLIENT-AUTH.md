<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Kafka client authentication

The audit ingestor, the audit dispatchers and tagsync authenticate to Kafka through the shared module `ranger-kafka-client-auth`

## Mechanisms

| `kafka.security.protocol` | `kafka.sasl.mechanism` | Credential |
|---|---|---|
| `PLAINTEXT` (default), `SSL` | none | none |
| `SASL_PLAINTEXT`, `SASL_SSL` | `OAUTHBEARER` (default when empty) | JWT from `kafka.sasl.oauthbearer.token.endpoint.url` |
| `SASL_PLAINTEXT`, `SASL_SSL` | `GSSAPI` | `service.kerberos.principal` and `service.kerberos.keytab` |
| `SASL_PLAINTEXT`, `SASL_SSL` | other (`PLAIN`, `SCRAM-*`) | `kafka.sasl.jaas.config` from the deployment |

- OAUTHBEARER with a `file://` URL uses `RangerKafkaOAuthBearerLoginCallbackHandler`, which re-reads the
  token on every login and refresh, so a token rotated in place (for example a Kubernetes projected
  service-account token) is picked up without a restart. An `http(s)://` URL uses Kafka's own
  client-credentials handler. The broker validates the token; its `sub` is the Kafka principal.
- Any `kafka.sasl.*` or `kafka.ssl.*` property under the service prefix is passed to the Kafka client as is.

## Docker Setup

With `KERBEROS_ENABLED=true` the broker accepts both GSSAPI and OAUTHBEARER, and the clients use OAUTHBEARER:
`ranger-kafka` runs `scripts/kafka/kafka-oauth-token-minter.sh`, which writes unsigned dev JWTs for
`rangerauditserver` and `rangertagsync` to the `ranger-kafka-oauth-tokens` volume (`/etc/kafka-oauth`) and
rewrites them every two minutes with a ten minute lifetime; the broker accepts them with Kafka's unsecured
validator. Dev only, never use this outside docker.

To switch a client to GSSAPI set its mechanism to `GSSAPI` in `scripts/audit-ingestor/configs/ranger-audit-ingestor-site.yaml`,
`scripts/audit-dispatcher/ranger-audit-dispatcher-<type>-site.xml` or `scripts/tagsync/ranger-tagsync-install.properties`;
the service keytabs are already configured.
