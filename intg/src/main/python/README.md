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

# Apache Ranger Python Client

`apache-ranger` is the official Python client package for Apache Ranger.
It provides typed helpers for Ranger Admin, user/group management, KMS,
Governed Data Sharing (GDS), and Policy Decision Point (PDP) APIs.

## Requirements

- Python 3.13 or later
- Apache Ranger Admin, KMS, or PDP service reachable from your Python process

## Installation

Install the client from PyPI:

```bash
pip install apache-ranger
```

For Kerberos/SPNEGO authentication, install the optional Kerberos dependency:

```bash
pip install requests-kerberos
```

Verify the installed package:

```bash
python -m pip show apache-ranger
```

## Supported Clients

- `RangerClient`: Ranger Admin APIs for service definitions, services, policies,
  roles, security zones, plugin information, and policy delta maintenance.
- `RangerUserMgmtClient`: user, group, and user-group management APIs.
- `RangerKMSClient`: Ranger KMS key management APIs.
- `RangerGdsClient`: Governed Data Sharing APIs for datasets, projects,
  datashares, shared resources, and GDS policies.
- `RangerPDPClient`: PDP authorization APIs for single authorization, batch
  authorization, and effective resource permissions.

## Quick Start

### Ranger Admin

```python
from apache_ranger.client.ranger_client import RangerClient

ranger = RangerClient("http://localhost:6080", ("admin", "rangerR0cks!"))

services = ranger.find_services()
print(f"{len(services.list)} services found")
```

### User And Group Management

```python
from apache_ranger.client.ranger_client import RangerClient
from apache_ranger.client.ranger_user_mgmt_client import RangerUserMgmtClient

ranger = RangerClient("http://localhost:6080", ("admin", "rangerR0cks!"))
user_mgmt = RangerUserMgmtClient(ranger)

users = user_mgmt.find_users()
groups = user_mgmt.find_groups()

print(f"{len(users.list)} users found")
print(f"{len(groups.list)} groups found")
```

### Ranger KMS

```python
from apache_ranger.client.ranger_client import HadoopSimpleAuth
from apache_ranger.client.ranger_kms_client import RangerKMSClient

kms = RangerKMSClient("http://localhost:9292", HadoopSimpleAuth("keyadmin"))

print(kms.kms_status())
```

### Governed Data Sharing

```python
from apache_ranger.client.ranger_client import RangerClient
from apache_ranger.client.ranger_gds_client import RangerGdsClient

ranger = RangerClient("http://localhost:6080", ("admin", "rangerR0cks!"))
gds = RangerGdsClient(ranger)

datasets = gds.find_datasets()
print(f"{len(datasets.list)} datasets found")
```

### Policy Decision Point

```python
from apache_ranger.client.ranger_pdp_client import RangerPDPClient
from apache_ranger.model.ranger_authz import (
    RangerAccessContext,
    RangerAccessInfo,
    RangerAuthzRequest,
    RangerResourceInfo,
    RangerUserInfo,
)

pdp = RangerPDPClient(
    "http://localhost:6500",
    auth=None,
    headers={"X-Forwarded-User": "hive"},
)

request = RangerAuthzRequest({
    "requestId": "req-1",
    "user": RangerUserInfo({"name": "alice"}),
    "access": RangerAccessInfo({
        "resource": RangerResourceInfo({"name": "table:default/test_tbl1"}),
        "permissions": ["select"],
    }),
    "context": RangerAccessContext({
        "serviceType": "hive",
        "serviceName": "dev_hive",
    }),
})

result = pdp.authorize(request)
print(result.decision)
```

## Authentication

Use the authentication mechanism configured for the Ranger service you are
calling:

- Basic auth: pass a `(username, password)` tuple to `RangerClient`.
- Kerberos/SPNEGO: pass `requests_kerberos.HTTPKerberosAuth()` after installing
  `requests-kerberos`.
- KMS Hadoop simple auth: use `HadoopSimpleAuth("user")` when simple auth is
  enabled for Ranger KMS.
- Custom headers or query parameters: pass `headers` or `query_params` to the
  client constructor when the deployment requires them.
- PDP trusted-header or JWT auth: pass the configured trusted caller header or
  `Authorization: Bearer <token>` header to `RangerPDPClient`.

Example Kerberos setup:

```python
from requests_kerberos import HTTPKerberosAuth
from apache_ranger.client.ranger_client import RangerClient

ranger = RangerClient("https://ranger.example.com:6182", HTTPKerberosAuth())
```

## PDP Request Notes

`RangerPDPClient` sends requests to the PDP REST APIs exposed under
`/authz/v1`.

- `authorize(request)` calls `POST /authz/v1/authorize`.
- `authorize_multi(request)` calls `POST /authz/v1/authorizeMulti`.
- `get_resource_permissions(request)` calls `POST /authz/v1/permissions`.

PDP authorization requests should include:

- `context.serviceType`, for example `hive`, `hdfs`, or `kafka`.
- `context.serviceName`, the Ranger service name.
- `user.name` for `authorize` and `authorize_multi` calls.

If the authenticated caller is different from `request.user.name`, the caller
must be allowed to delegate for that service. Without delegation permission, PDP
returns `403 FORBIDDEN`.

## Examples and Code References

The quick starts above cover the basics. For day-to-day API usage — creating
services and policies, managing users, calling KMS or PDP, or working with GDS
— use the runnable samples and client sources below.

### Running sample scripts

After `pip install apache-ranger`, run a sample directly:

```bash
python ranger-examples/sample-client/src/main/python/sample_client.py
```

When working from a Ranger source checkout without installing the package, set
`PYTHONPATH` to the client sources first:

```bash
# from the repository root
PYTHONPATH=intg/src/main/python \
  python ranger-examples/sample-client/src/main/python/sample_client.py
```

Edit the `ranger_url`, `ranger_auth`, and other connection settings at the top
of each sample before running it against your environment.

### Runnable examples by task

| Task | Sample script | What it demonstrates |
| --- | --- | --- |
| Ranger Admin — services, policies, roles, tags | [`sample_client.py`](https://github.com/apache/ranger/blob/master/ranger-examples/sample-client/src/main/python/sample_client.py) | Service-def and service CRUD, access/data-masking/row-filter policies, roles, service tags |
| User and group management | [`user_mgmt.py`](https://github.com/apache/ranger/blob/master/ranger-examples/sample-client/src/main/python/user_mgmt.py) | List/create/delete users and groups, group-user mappings |
| Ranger KMS | [`sample_kms_client.py`](https://github.com/apache/ranger/blob/master/ranger-examples/sample-client/src/main/python/sample_kms_client.py) | Key lifecycle, encrypt/decrypt/reencrypt, metadata and status |
| PDP authorization | [`sample_pdp_client.py`](https://github.com/apache/ranger/blob/master/ranger-examples/sample-client/src/main/python/sample_pdp_client.py) | `authorize`, `authorize_multi`, `get_resource_permissions` |
| Governed Data Sharing (GDS) | [`sample_gds_client.py`](https://github.com/apache/ranger/blob/master/ranger-examples/sample-client/src/main/python/sample_gds_client.py) | Datasets, projects, datashares, shared resources, GDS policies |
| Security zones (v2) | [`security_zone_v2.py`](https://github.com/apache/ranger/blob/master/ranger-examples/sample-client/src/main/python/security_zone_v2.py) | Create/update security zones and zone resources |

All samples live under
[`ranger-examples/sample-client/src/main/python`](https://github.com/apache/ranger/tree/master/ranger-examples/sample-client/src/main/python).

### Client API source code

Each public client is implemented under
[`intg/src/main/python/apache_ranger/client/`](https://github.com/apache/ranger/tree/master/intg/src/main/python/apache_ranger/client):

| Client | Source file | REST base path |
| --- | --- | --- |
| `RangerClient` | [`ranger_client.py`](https://github.com/apache/ranger/blob/master/intg/src/main/python/apache_ranger/client/ranger_client.py) | `service/public/v2/api` |
| `RangerUserMgmtClient` | [`ranger_user_mgmt_client.py`](https://github.com/apache/ranger/blob/master/intg/src/main/python/apache_ranger/client/ranger_user_mgmt_client.py) | user/group APIs on Ranger Admin |
| `RangerKMSClient` | [`ranger_kms_client.py`](https://github.com/apache/ranger/blob/master/intg/src/main/python/apache_ranger/client/ranger_kms_client.py) | KMS REST APIs |
| `RangerGdsClient` | [`ranger_gds_client.py`](https://github.com/apache/ranger/blob/master/intg/src/main/python/apache_ranger/client/ranger_gds_client.py) | GDS APIs on Ranger Admin |
| `RangerPDPClient` | [`ranger_pdp_client.py`](https://github.com/apache/ranger/blob/master/intg/src/main/python/apache_ranger/client/ranger_pdp_client.py) | `/authz/v1` |

Request and response models are under
[`intg/src/main/python/apache_ranger/model/`](https://github.com/apache/ranger/tree/master/intg/src/main/python/apache_ranger/model).

### Unit tests

[`intg/src/test/python/test_ranger_client.py`](https://github.com/apache/ranger/blob/master/intg/src/test/python/test_ranger_client.py)
contains mocked examples for Admin, GDS, and PDP client wiring. Run from
`intg/`:

```bash
PYTHONPATH=src/main/python python -B src/test/python/test_ranger_client.py
```

## Troubleshooting

- `ModuleNotFoundError: requests_kerberos`: install `requests-kerberos`.
- `401 Unauthorized`: verify the credentials, Kerberos ticket, or auth headers
  used by the target Ranger service.
- `403 Forbidden`: verify Ranger permissions and PDP delegation configuration
  when authorizing on behalf of another user.
- SSL certificate errors: configure a valid trust store for production. For
  local testing only, `ranger.session.verify = False` can disable certificate
  verification.
- Connection timeouts: verify the Ranger Admin, KMS, or PDP URL and confirm the
  service is reachable from the client host.

## Release 0.0.13 Highlights

- Python 3.13+ support.
- New GDS client coverage for datasets, projects, datashares, shared resources,
  and GDS policy APIs.
- New PDP client coverage for Ranger authorization APIs.
- New authorization request and response models.
- User/group management, KMS, Ranger Admin, and model updates.
