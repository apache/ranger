<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# Ranger PDP Kubernetes Notes

This document captures Kubernetes-oriented runtime recommendations for `ranger-pdp`.

## Health Probes

Use:

- Liveness: `GET /health/live`
- Readiness: `GET /health/ready`

The readiness endpoint reports NOT_READY until:

- server is started
- authorizer is initialized
- PDP is accepting requests

## Metrics

PDP exposes Prometheus-style text metrics at:

- `GET /metrics`

Current metrics include request counts, auth failures, average latency and policy-cache age.

## Runtime Tuning

Thread/connection controls can be set in `ranger-pdp-site.xml` or via `JAVA_OPTS -D...`:

- `ranger.pdp.http.connector.maxThreads`
- `ranger.pdp.http.connector.minSpareThreads`
- `ranger.pdp.http.connector.acceptCount`
- `ranger.pdp.http.connector.maxConnections`

## Logging

For container-native logging, logback can emit to stdout.
If file logging is needed, pass:

`-Dlogdir=/path/to/log/dir`

and use a file appender that resolves `${logdir}`.

## Security Context

Recommended:

- run as non-root user
- readOnlyRootFilesystem: true
- mount writable volumes only for required paths (cache/log/temp if file logging is enabled)

## Config/Secrets

Recommended:

- `ranger-pdp-site.xml` from ConfigMap
- keytabs/JWT keys/credentials from Secrets

## Authentication Config Keys

When configuring inbound PDP authentication in `ranger-pdp-site.xml`, use the
`ranger.pdp.authn.*` property names:

- `ranger.pdp.authn.types`
- `ranger.pdp.authn.header.enabled`
- `ranger.pdp.authn.header.username`
- `ranger.pdp.authn.jwt.enabled`
- `ranger.pdp.authn.jwt.provider.url`
- `ranger.pdp.authn.jwt.public.key`
- `ranger.pdp.authn.jwt.cookie.name`
- `ranger.pdp.authn.jwt.audiences`
- `ranger.pdp.authn.kerberos.enabled`
- `ranger.pdp.authn.kerberos.spnego.principal`
- `ranger.pdp.authn.kerberos.spnego.keytab`
- `ranger.pdp.authn.kerberos.token.valid.seconds`
- `ranger.pdp.authn.kerberos.name.rules`

## Network Policy

Allow egress only to dependencies:

- Ranger Admin
- Solr (if audit destination enabled)
- KDC (if Kerberos is used)

