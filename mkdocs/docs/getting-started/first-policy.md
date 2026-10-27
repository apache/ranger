<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Your first policy

In this tutorial you create a Ranger policy for Hive, run queries as a user who is first denied and
then allowed, and read the resulting audit records. You then add a deny rule and a row filter to see
two more kinds of policy in action. Everything runs in the development setup from
[Run Ranger with Docker](docker.md#build-from-source), which starts Hive with the Ranger Hive plugin
already enabled and a Ranger service named `dev_hive` already created.

Allow about 15 minutes once HiveServer2 is up, which takes three to four minutes after `docker compose up`.
No Hive or Ranger experience is needed.

!!! note

    This tutorial cannot be followed with the Docker Hub images alone: only Ranger Admin, its database
    and Solr are published there, and no Hadoop or Hive image with the Ranger plugin. The Hive container
    comes from `docker-compose.ranger-hadoop.yml` and `docker-compose.ranger-hive.yml` in
    `dev-support/ranger-docker`, which are built from a source build of Ranger.

## Before you start

1. Give Docker at least 8 GB of memory (Docker Desktop: **Settings → Resources → Memory**). This
   tutorial runs Hadoop, Hive, Kafka, OpenSearch and the Ranger services side by side; with the default
   4 GB, OpenSearch and HiveServer2 are killed shortly after they start.
2. Clone the repository and build Ranger into `dev-support/ranger-docker/dist`, as described in
   [Prepare the development setup](docker.md#build-from-source).
3. Download the archives that the Hadoop, Hive and Kafka images are built from (the `hive` argument
   also fetches Tez, which both the Hadoop and the Hive image need), and start Ranger Admin, the
   audit server, Hadoop and Hive:

    ```bash
    cd dev-support/ranger-docker
    chmod +x download-archives.sh
    ./download-archives.sh hadoop hive kafka

    export RANGER_DB_TYPE=postgres
    export AUDIT_INDEX_STORE=opensearch
    export AUDIT_DESTINATIONS=audit-store-${AUDIT_INDEX_STORE}

    docker compose --profile ${AUDIT_DESTINATIONS} -f docker-compose.ranger.yml -f docker-compose.ranger-audit-service.yml -f docker-compose.ranger-hadoop.yml -f docker-compose.ranger-hive.yml up -d
    ```

    Kafka is needed because the audit server (not yet part of a Ranger release) uses it to carry
    audits from the ingestor to the dispatcher that indexes them in OpenSearch.

HiveServer2 needs three to four minutes to start. Wait until `docker logs ranger-hive` reports
`HiveServer2 is ready and listening on port 10000`, for example with:

```bash
until docker logs ranger-hive 2>&1 | grep -q 'HiveServer2 is ready and listening on port 10000'; do sleep 10; done
```

If the line does not appear after several minutes and `docker ps -a` shows `ranger-hive` or
`ranger-opensearch` as exited, Docker has too little memory (see above). The environment you now have:

- **Ranger Admin**: <http://localhost:6080>, user `admin`, password `rangerR0cks!`.
- **Hive service in Ranger**: `dev_hive`, created by `scripts/admin/create-ranger-services.py`.
- **HiveServer2**: the `ranger-hive` container, port 10000, Kerberos authentication,
  `hive.server2.enable.doAs=false`.
- **Kerberos realm**: `EXAMPLE.COM`; keytabs are in `/etc/keytabs` inside each container.
- **Test identities**: `hive/ranger-hive.rangernw` (the Hive service user) and
  `testuser1/ranger-hive.rangernw` (an ordinary user).

Because HiveServer2 authenticates with Kerberos and `doAs` is off, the user Ranger sees is the short
name of the Kerberos principal that connected: `hive` or `testuser1`.

## Step 1: log in and look at the Hive service

1. Open <http://localhost:6080> and log in as `admin` / `rangerR0cks!`.
2. The landing page is **Service Manager**. Under **Hadoop SQL** (the Hive service type) you see
   `dev_hive`. Click it.
3. The policy list shows the default policies that Ranger created with the service:
   `all - database`, `all - database, table`, `all - database, table, column`, `all - database, udf`,
   `all - url`, `all - hiveservice` and `all - global`. They grant every access type to the user
   `hive`, which is why HiveServer2 itself can work, and **read** and **select** to `rangerlookup`, the
   account Ranger Admin uses to look up database and table names while you edit a policy. The database,
   table, UDF and column policies also grant everything to `{OWNER}`, the owner of a database or table,
   and `all - database` lets the group `public` create databases. Two more default policies,
   `default database tables columns` and `Information_schema database tables columns`, give the group
   `public` **create** in the `default` database and **select** on `information_schema`. No policy lets
   an ordinary user read a table owned by someone else.

## Step 2: create a table as the Hive service user

Open a shell in the Hive container. Keep this shell open: every Beeline command in this tutorial runs
from it.

```bash
docker exec -it ranger-hive bash
```

Inside the container, write a small data file, get a ticket for the `hive` principal and start Beeline:

```bash
printf '1,Ana,US,120000\n2,Bob,US,95000\n3,Chen,CN,105000\n4,Dana,DE,99000\n' > /tmp/employees.csv
kinit -kt /etc/keytabs/hive.keytab hive/ranger-hive.rangernw@EXAMPLE.COM
beeline -u "jdbc:hive2://localhost:10000/default;principal=hive/ranger-hive.rangernw@EXAMPLE.COM"
```

In Beeline, create the table and load the file into it:

```sql
CREATE TABLE employees (id INT, name STRING, country STRING, salary INT)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH '/tmp/employees.csv' INTO TABLE employees;
SELECT * FROM employees;
!quit
```

The `SELECT` prints the four rows. Loading a file and reading a small table do not need a YARN job,
so every query in this tutorial answers within seconds. The only waits are the plugin's 30-second policy
poll after you save a policy, and the audit pipeline before rows show up in Ranger Admin.

## Step 3: try the query as an ordinary user

Still inside the container, switch to `testuser1` and run the same `SELECT`:

```bash
kdestroy
kinit -kt /etc/keytabs/testuser1.keytab testuser1/ranger-hive.rangernw@EXAMPLE.COM
beeline -u "jdbc:hive2://localhost:10000/default;principal=hive/ranger-hive.rangernw@EXAMPLE.COM" \
  -e "SELECT * FROM employees"
```

The query fails with a `HiveAccessControlException`:

```text
Permission denied: user [testuser1] does not have [SELECT] privilege on [default/employees/*]
```

This is the Ranger Hive plugin rejecting the request because no policy allows it.

## Step 4: create the policy

Ranger needs to know the user before you can put it in a policy. In Ranger Admin open
**Settings → Users** (the page is headed **Users/Groups/Roles**); if `testuser1` is not listed, click
**Add New User** and enter:

- **User Name**: `testuser1`
- **New Password** and **Password Confirm**: any password of at least 8 characters with an uppercase
  letter, a lowercase letter and a digit. It is never used in this tutorial, because Hive authenticates
  `testuser1` with Kerberos.
- **First Name**: `testuser1`
- **Select Role**: change it to **User**. The form defaults to **Admin**, which would make `testuser1` a
  Ranger administrator.

Save. (In your own deployments UserSync creates these entries for you.)

Now create the policy:

1. Go back to **Service Manager → dev_hive** and click **Add New Policy**.
2. Fill in:
    - **Policy Name**: `employees - read`
    - **Hive Database**: `default`
    - **Hive Table**: `employees`
    - **Hive Column**: `*`

    Each resource row is a drop-down that already shows the right level (the first one also offers
    URL, Hive Service and Global), so only the values need typing.
3. Under **Allow Rules**, add `testuser1` in **Select Users**, then in the **Permissions** column click
   **Add Permissions** and tick **select**.
4. Click **Save**.

The policy appears in the list with **Audit Logging** on. The plugin in HiveServer2 polls Ranger Admin
for changes every 30 seconds (`ranger.plugin.hive.policy.pollIntervalMs`), so wait up to half a minute
before the next step. Nothing in Ranger Admin shows when the plugin has picked the policy up; if the next
query is still denied, wait ten seconds and run it again. Each refresh is logged in the container as
`Switched policy engine to [N]` in `/opt/hive/logs/hiveserver2.log`.

??? example "The same user and policy through the REST API"

    Run these on the host, not in the container shell: `localhost:6080` reaches Ranger Admin only from
    the host. Ranger Admin rejects a policy that names a user it does not know, so create the user first.

    ```bash
    curl -u admin:rangerR0cks! -H 'Content-Type: application/json' \
      -X POST http://localhost:6080/service/xusers/secure/users -d '{
        "name": "testuser1", "firstName": "testuser1", "lastName": "",
        "password": "TestUser1pw", "userRoleList": ["ROLE_USER"],
        "status": 1, "isVisible": 1, "groupIdList": [], "description": ""
      }'

    curl -u admin:rangerR0cks! -H 'Content-Type: application/json' \
      -X POST http://localhost:6080/service/public/v2/api/policy -d '{
        "service": "dev_hive",
        "name": "employees - read",
        "resources": {
          "database": { "values": ["default"] },
          "table":    { "values": ["employees"] },
          "column":   { "values": ["*"] }
        },
        "policyItems": [
          { "users": ["testuser1"], "accesses": [ { "type": "select", "isAllowed": true } ] }
        ]
      }'
    ```

## Step 5: run the query again

```bash
beeline -u "jdbc:hive2://localhost:10000/default;principal=hive/ranger-hive.rangernw@EXAMPLE.COM" \
  -e "SELECT name, country FROM employees"
```

The rows come back. Anything the policy does not cover is still refused; for example
`INSERT INTO employees VALUES (5, 'Eve', 'US', 1)` fails with
`does not have [UPDATE] privilege on [default/employees]`: Hive maps `INSERT` to Ranger's `update`
permission, and `testuser1` only has `select`.

## Step 6: see the audit entries

In Ranger Admin open **Audits → Access**. Audits from the plugin travel through the audit ingestor and
Kafka into OpenSearch. The first event after HiveServer2 starts can take about half a minute to show up,
later ones a few seconds, so refresh until the rows appear. Filter on **User** = `testuser1`. You see one
row per access check (plus a Denied row with access type `INSERT` and resource `default/employees` if you
tried the `INSERT` in step 5):

| Column | Denied query (step 3) | Allowed query (step 5) |
|---|---|---|
| Result | Denied | Allowed |
| Policy ID | `--` (no policy matched) | The id of `employees - read`; click it to open the policy |
| Resource (Name / Type) | `default/employees/country`: the plugin checks the columns of a query in alphabetical order and reports only the first one that was denied | `default/employees/country,name`: the columns the query read, in alphabetical order |
| Access Type | `SELECT` | `SELECT` |
| Service (Name / Type) | `dev_hive` | `dev_hive` |

Each row also carries the client IP and the event time.
Click the row to see the full event, including the Hive query text. **Audits → Admin** shows the
creation of the policy, and **Audits → Login Sessions** shows your login.

## Step 7: add a deny rule

Suppose `testuser1` may read everything about employees except salaries. Deny rules are evaluated
before allow rules, so a deny on the `salary` column overrides the allow on `*` from the first policy.

1. In `dev_hive` click **Add New Policy** again and fill in:
    - **Policy Name**: `employees - no salary`
    - **Hive Database**: `default`, **Hive Table**: `employees`, **Hive Column**: `salary`
2. Leave **Allow Rules** empty. Under **Deny Rules** add `testuser1` in **Select Users** and, in the
   **Permissions** column, click **Add Permissions** and tick **select**.
3. Click **Save**, wait for the poll interval, then run:

```bash
beeline -u "jdbc:hive2://localhost:10000/default;principal=hive/ranger-hive.rangernw@EXAMPLE.COM" \
  -e "SELECT name FROM employees"          # allowed
beeline -u "jdbc:hive2://localhost:10000/default;principal=hive/ranger-hive.rangernw@EXAMPLE.COM" \
  -e "SELECT name, salary FROM employees"  # denied
```

The second query is denied. Beeline only reports `does not have [SELECT] privilege on
[default/employees/*]`; **Audits → Access** is where you see that the resource was
`default/employees/salary` and that the id of `employees - no salary` is the policy that made the
decision. The evaluation order (deny, deny exceptions, allow, allow exceptions)
is explained in [Policy model](../arch/policy-model.md). Instead of denying the column you could also
mask it: a policy on the **Masking** tab for `default.employees.salary` with a mask type such as
*Redact* or *Hash* returns masked values to `testuser1` while other users see the real data.

## Step 8: add a row filter

Row filters let a user query a table but only see the rows that match a predicate. Restrict
`testuser1` to US employees:

1. In `dev_hive` switch to the **Row Level Filter** tab and click **Add New Policy**.
2. Fill in **Policy Name** `employees - us only`, **Hive Database** `default`, **Hive Table** `employees`
   (row-filter policies take exactly one database and one table, no wildcards).
3. Under **Row Filter Rules** add `testuser1` in **Select Users**, tick **select** under **Permissions**,
   and in the **Row Level Filter** column click the cell and enter the filter expression `country = 'US'`.
4. Save, wait for the poll interval, then run:

```bash
beeline -u "jdbc:hive2://localhost:10000/default;principal=hive/ranger-hive.rangernw@EXAMPLE.COM" \
  -e "SELECT name, country FROM employees"
```

Only Ana and Bob are returned. The filter is applied by HiveServer2 as a rewrite of the query, so it
also applies to joins and aggregates over the table. In **Audits → Access** this query produces two
rows: one with access type `ROW_FILTER` and the id of `employees - us only`, and the usual `SELECT` row
with the id of `employees - read`.

## What you have seen

- Enforcement happens inside HiveServer2, using policies the plugin pulled from Ranger Admin and
  cached locally (`/etc/ranger/dev_hive/policycache` in the container).
- Access is closed by default: a user without a matching allow policy is denied.
- Deny rules win over allow rules; masking and row filters change what a query returns rather than
  whether it runs.
- Every decision is audited with the policy that made it.
