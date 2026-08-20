##### Ozone action-matcher feature flag

The flag is controlled at runtime by
`ranger.servicedef.ozone.enableActionMatcherInPoliciesCondition` in
`ranger-admin-site.xml`. Docker maps it from
`FF_ENABLE_OZONE_ACTION_MATCHES_CONDITION` in
`scripts/admin/ranger-admin-install-*.properties` during Admin setup.

**Prerequisite:** the admin distribution must include an uncommented property
entry in `conf.dist/ranger-admin-site.xml`. Rebuild dist before building the
Admin image:

~~~
docker compose -f docker-compose.ranger-build.yml build
docker compose -f docker-compose.ranger-build.yml up
~~~

**First-time setup:** set `FF_ENABLE_OZONE_ACTION_MATCHES_CONDITION=true` in
`scripts/admin/ranger-admin-install-<db>.properties`, then bring up services.

**Changing the flag later** (choose one):

1. Edit `FF_ENABLE_OZONE_ACTION_MATCHES_CONDITION` in
   `scripts/admin/ranger-admin-install-<db>.properties` and recreate Admin so
   setup runs again:
   ~~~
   export AUDIT_INDEX_STORE=opensearch
   docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-kafka.yml -f docker-compose.ranger-audit-service.yml -f docker-compose.ranger-ozone.yml \
     up -d --build --force-recreate ranger
   ~~~
   Works even if the DB already exists; setup re-runs on a fresh container.

2. Edit the live value in the container's
   `ranger-admin-site.xml` and restart Admin.

`install.properties` is not re-read on restart alone — only when setup runs.
`RangerServiceDefService` applies the site.xml value when serving the ozone
service-def.

~~~
./scripts/ozone/ozone-plugin-docker-setup.sh
export AUDIT_INDEX_STORE=opensearch
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-kafka.yml -f docker-compose.ranger-audit-service.yml -f docker-compose.ranger-ozone.yml up -d
~~~

Verify (after login):

~~~
curl -s -u admin:rangerR0cks! http://localhost:6080/service/plugins/definitions/name/ozone \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print('options', d.get('options')); print('conditions', [c['name'] for c in d.get('policyConditions',[])])"
~~~