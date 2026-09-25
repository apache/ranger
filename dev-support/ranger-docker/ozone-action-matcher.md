##### Ozone action-matcher feature flag

The flag is controlled at runtime by
`ranger.servicedef.ozone.enableActionMatcherInPoliciesCondition` in
`ranger-admin-site.xml`. Docker Admin renders this file from
`scripts/admin/configs/ranger-admin-site-<db>.yaml` on every container start.

**Prerequisite:** the admin distribution must include an uncommented property
entry in `conf.dist/ranger-admin-site.xml`. Rebuild dist before building the
Admin image:

~~~
docker compose -f docker-compose.ranger-build.yml build
docker compose -f docker-compose.ranger-build.yml up
~~~

**Enabling the flag:** add
`ranger.servicedef.ozone.enableActionMatcherInPoliciesCondition: true` to
`scripts/admin/configs/ranger-admin-site-<db>.yaml`, then bring up services, or
restart Admin if it is already running:
~~~
docker restart ranger
~~~
`RangerServiceDefService` applies the site.xml value when serving the ozone
service-def.

~~~
./scripts/ozone/ozone-plugin-docker-setup.sh
export AUDIT_INDEX_STORE=opensearch
export AUDIT_DESTINATIONS=audit-store-opensearch
docker compose --profile ${AUDIT_DESTINATIONS} -f docker-compose.ranger.yml -f docker-compose.ranger-audit-service.yml -f docker-compose.ranger-ozone.yml up -d
~~~

Verify (after login):

~~~
curl -s -u admin:rangerR0cks! http://localhost:6080/service/plugins/definitions/name/ozone \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print('options', d.get('options')); print('conditions', [c['name'] for c in d.get('policyConditions',[])])"
~~~