#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Prepares Ranger Admin runtime using configuration YAML and env variables: renders conf/ranger-admin-site.xml, stores the
# DB password in the credential store, imports the core schema, applies pending SQL/Java patches and sets user passwords.

import glob
import logging
import os
import re
import shlex
import socket
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET

import yaml

RANGER_ADMIN_DIR = os.path.join(os.environ.get('RANGER_HOME', '/opt/ranger'), 'admin')
RANGER_CONFIGS   = os.environ.get('RANGER_ADMIN_CONFIGS', os.path.join(RANGER_ADMIN_DIR, 'configs'))
RANGER_CONF_DIR  = os.environ.get('RANGER_CONF_DIR', os.path.join(RANGER_ADMIN_DIR, 'ews/webapp/WEB-INF/classes/conf'))
RANGER_LOG_DIR   = os.environ.get('RANGER_ADMIN_LOG_DIR', '/var/log/ranger')
WEBAPP_DIR       = os.path.join(RANGER_ADMIN_DIR, 'ews/webapp')

# colors only when attached to a terminal (e.g. docker compose with tty), plain text otherwise (e.g. log collectors)
COLOR = sys.stdout.isatty() and 'NO_COLOR' not in os.environ


def style(text, code):
    return f'\033[{code}m{text}\033[0m' if COLOR else text


class ConsoleFormatter(logging.Formatter):
    LEVELS = {logging.INFO: ('INFO ', '32'), logging.WARNING: ('WARN ', '33'), logging.ERROR: ('ERROR', '1;31')}

    def format(self, record):
        label, code = self.LEVELS.get(record.levelno, (record.levelname[:5], '0'))

        return f"{style(self.formatTime(record, '%H:%M:%S'), '2')} {style(label, code)} {record.getMessage()}"


console_handler = logging.StreamHandler(sys.stdout)
file_handler    = logging.FileHandler(os.path.join(RANGER_LOG_DIR, 'dba.log'))

console_handler.setFormatter(ConsoleFormatter())
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)-5s %(message)s'))
logging.basicConfig(level=logging.INFO, handlers=(console_handler, file_handler))

LOG = logging.getLogger('dba')


def print_section(title):
    print(style(f'━━━ ▶ {title} '.ljust(110, '━'), '1;36'), flush=True)


def indent(text):
    return '\n'.join(style('    │ ', '2') + line for line in text.splitlines())


VERSION_TABLE = 'x_db_version_h'
JAVA_PATCH_RE = re.compile(r'^(Patch\w*_J(\d{5}))\.class$')
JISQL_ERR_RE  = re.compile(r'^(SQLException|Cannot (find|instantiate) the driver)', re.M)

# login_id, default password seeded by core schema, x_db_version_h marker used by db_setup.py, env variable
BUILTIN_USERS = (('admin',          'admin',          'DEFAULT_ADMIN_UPDATE',           'RANGER_ADMIN_PASSWORD'),
                 ('rangerusersync', 'rangerusersync', 'DEFAULT_RANGER_USERSYNC_UPDATE', 'RANGER_USERSYNC_PASSWORD'),
                 ('rangertagsync',  'rangertagsync',  'DEFAULT_RANGER_TAGSYNC_UPDATE',  'RANGER_TAGSYNC_PASSWORD'),
                 ('keyadmin',       'keyadmin',       'DEFAULT_KEYADMIN_UPDATE',        'RANGER_KEYADMIN_PASSWORD'))

# keyed by JDBC sub-protocol: db script directory, readiness query, table-exists query, x_db_version_h insert
FLAVORS = {
    'postgresql': ('postgres', 'select 1;',
                   "select table_name from information_schema.tables where table_catalog=current_database() and table_name='{table}';",
                   "insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by, active) values ('{version}', current_timestamp, '{inst_by}', current_timestamp, '{host}', 'Y');"),
    'mysql':      ('mysql', 'select 1;',
                   "show tables like '{table}';",
                   "insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by, active) values ('{version}', current_timestamp, '{inst_by}', current_timestamp, '{host}', 'Y');"),
    'oracle':     ('oracle', 'select 1 from dual;',
                   "select table_name from user_tables where table_name=upper('{table}');",
                   "insert into x_db_version_h (id, version, inst_at, inst_by, updated_at, updated_by, active) values (X_DB_VERSION_H_SEQ.nextval, '{version}', sysdate, '{inst_by}', sysdate, '{host}', 'Y');"),
}


class BootstrapError(Exception):
    def __init__(self, message, details=''):
        super().__init__(message)

        self.details = details

    def describe(self):
        return f'{self}\n{indent(self.details)}' if self.details else str(self)


def to_property_value(value):
    if isinstance(value, bool):
        return str(value).lower()

    return '' if value is None else str(value)


def render_site_xml():
    # a mounted ranger-admin-site.xml is the configuration, as ranger.sh copies it to conf; nothing is rendered over it
    site_xml = os.path.join(RANGER_CONFIGS, 'ranger-admin-site.xml')

    if os.path.isfile(site_xml):
        if os.environ.get('AUDIT_INDEX_STORE'):
            LOG.warning(f'env AUDIT_INDEX_STORE is ignored: ranger.audit.source.type is taken from {site_xml}')

        LOG.info(f'✔ Using {site_xml} as it is, conf/ranger-admin-site.xml is not rendered from YAML')

        return

    db_type = os.environ.get('RANGER_DB_TYPE')

    if not db_type:
        raise BootstrapError(f'env RANGER_DB_TYPE is not set and {site_xml} does not exist')

    site_yaml = os.path.join(RANGER_CONFIGS, f'ranger-admin-site-{db_type}.yaml')

    if not os.path.isfile(site_yaml):
        raise BootstrapError(f'configuration not found: neither {site_xml} nor {site_yaml} exists')

    with open(site_yaml) as f:
        overrides = yaml.safe_load(f) or {}

    if not isinstance(overrides, dict) or any(isinstance(value, (dict, list)) for value in overrides.values()):
        raise BootstrapError(f'{site_yaml} must be a flat map of Ranger Admin property names to values')

    # the YAML file is the complete Ranger Admin configuration; nothing is taken from conf.dist
    props = {str(name): to_property_value(value) for name, value in overrides.items()}

    if os.environ.get('AUDIT_INDEX_STORE'):
        props['ranger.audit.source.type'] = os.environ['AUDIT_INDEX_STORE']

        LOG.info(f"ranger.audit.source.type: {os.environ['AUDIT_INDEX_STORE']} (from env AUDIT_INDEX_STORE)")

    root = ET.Element('configuration')

    root.append(ET.Comment(f' generated by dba.py from {os.path.basename(site_yaml)} '))

    for name, value in props.items():
        prop = ET.SubElement(root, 'property')

        ET.SubElement(prop, 'name').text  = name
        ET.SubElement(prop, 'value').text = value

    ET.indent(root)
    ET.ElementTree(root).write(os.path.join(RANGER_CONF_DIR, 'ranger-admin-site.xml'), encoding='utf-8', xml_declaration=True)

    LOG.info(f'✔ Rendered conf/ranger-admin-site.xml with {len(props)} properties from {site_yaml}')


def load_site_properties():
    props = {}

    for file_name in ('ranger-admin-default-site.xml', 'ranger-admin-site.xml'):
        for prop in ET.parse(os.path.join(RANGER_CONF_DIR, file_name)).getroot().iter('property'):
            props[prop.findtext('name', '').strip()] = prop.findtext('value', '').strip()

    return props


def java_cmd(*args):
    java = os.path.join(os.environ['JAVA_HOME'], 'bin', 'java') if os.environ.get('JAVA_HOME') else 'java'

    return [java, *args]


class RangerDB:
    def __init__(self, props):
        self.url       = props.get('ranger.jpa.jdbc.url', '')
        self.driver    = props.get('ranger.jpa.jdbc.driver', '')
        self.user      = props.get('ranger.jpa.jdbc.user', '')
        self.jar       = props.get('ranger.jdbc.sqlconnectorjar', '')
        self.host      = socket.gethostname()
        self.inst_by   = f"Ranger {os.environ.get('RANGER_VERSION', '')}".strip()

        match = re.match(r'^jdbc:(?:log4jdbc:)?(\w+):', self.url)

        if not match or match.group(1) not in FLAVORS:
            raise BootstrapError(f'unsupported ranger.jpa.jdbc.url: {self.url}')
        if not all((self.driver, self.user, self.jar, os.environ.get('RANGER_ADMIN_DB_PASSWORD'))):
            raise BootstrapError('ranger.jpa.jdbc.driver, ranger.jpa.jdbc.user, ranger.jdbc.sqlconnectorjar and env RANGER_ADMIN_DB_PASSWORD are required')
        if not os.path.isfile(self.jar):
            raise BootstrapError(f'JDBC driver {self.jar} (ranger.jdbc.sqlconnectorjar) not found')

        self.db_dir, self.ready_query, self.table_query, self.insert_version_query = FLAVORS[match.group(1)]

        LOG.info(f'Database: flavor={self.db_dir}, url={self.url}, user={self.user}, driver={self.driver}, jar={self.jar}')

        # Jisql reads the password from a file (-pf), which keeps it off the process command line
        with tempfile.NamedTemporaryFile('w', prefix='jisql-', delete=False) as pwd_file:
            pwd_file.write(os.environ['RANGER_ADMIN_DB_PASSWORD'])

        self.password_file = pwd_file.name

    def jisql(self, *args):
        jvm_args = ['-Djava.security.egd=file:///dev/urandom'] if self.db_dir == 'oracle' else []
        cmd      = java_cmd(*jvm_args, '-cp', f'{self.jar}:{RANGER_ADMIN_DIR}/jisql/lib/*', 'org.apache.util.sql.Jisql',
                            '-driver', self.driver, '-cstring', self.url, '-u', self.user, '-pf', self.password_file, '-noheader', '-trim', *args)
        ret      = subprocess.run(cmd, capture_output=True, text=True)

        # Jisql exits with 0 when connection fails, hence errors are detected from stderr as well
        if ret.returncode != 0 or JISQL_ERR_RE.search(ret.stderr):
            raise BootstrapError(f'Jisql exited with code {ret.returncode}', '\n'.join(dict.fromkeys(ret.stderr.strip().splitlines())))

        return ret.stdout

    def query(self, sql):
        return [line.split('|')[0].strip() for line in self.jisql('-c', ';', '-query', sql).splitlines() if line.strip()]

    def wait_until_ready(self, timeout):
        deadline = time.time() + timeout

        while True:
            try:
                if '1' in self.query(self.ready_query):
                    LOG.info(f'✔ Connected to {self.url}')
                    return
            except BootstrapError as err:
                LOG.info(f'⏳ Waiting for database: {(err.details or str(err)).splitlines()[-1][:120]}')

            if time.time() > deadline:
                raise BootstrapError(f'database {self.url} is not reachable after {timeout}s')

            time.sleep(5)

    def applied_versions(self):
        if not any(VERSION_TABLE in row.lower() for row in self.query(self.table_query.format(table=VERSION_TABLE))):
            LOG.info(f'Table {VERSION_TABLE} not found: database is empty')
            return set()

        return set(self.query(f"select version from {VERSION_TABLE} where active='Y';"))

    def mark_applied(self, version):
        self.query(self.insert_version_query.format(version=version, inst_by=self.inst_by, host=self.host))

    def import_file(self, file_name):
        self.jisql('-c', ';', '-input', file_name)

    def run_java(self, class_name, *args):
        classpath = ':'.join((f'{WEBAPP_DIR}/WEB-INF/classes/conf', f'{WEBAPP_DIR}/WEB-INF/classes/lib/*', f'{WEBAPP_DIR}/WEB-INF/', f'{WEBAPP_DIR}/META-INF/',
                              f'{WEBAPP_DIR}/WEB-INF/lib/*', f'{WEBAPP_DIR}/WEB-INF/classes/', f'{WEBAPP_DIR}/WEB-INF/classes/META-INF', self.jar))
        cmd       = java_cmd(*shlex.split(os.environ.get('JAVA_OPTS', '')), f"-Xmx{os.environ.get('RANGER_ADMIN_MAX_HEAP', '1g')}",
                             f'-Dlogdir={RANGER_LOG_DIR}', f'-Dlogback.configurationFile=file:{RANGER_CONF_DIR}/logback.xml',
                             f"-Duser={os.environ.get('USER', 'ranger')}", f'-Dhostname={self.host}', '-cp', classpath, class_name, *args)

        # logback.xml in the container conf logs to console as well; output is returned to be shown under the operation result
        ret = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        return ret.returncode, ret.stdout.rstrip()


def create_credential_store(props):
    provider = props.get('ranger.credential.provider.path')
    alias    = props.get('ranger.jpa.jdbc.credential.alias')

    if not (provider and alias):
        LOG.info('ranger.credential.provider.path or ranger.jpa.jdbc.credential.alias is not set: DB password is not stored in credential store')
        return

    # cred/lib lacks hadoop-common, which buildks needs; it is available in webapp libs
    ret = subprocess.run(java_cmd('-cp', f'{RANGER_ADMIN_DIR}/cred/lib/*:{WEBAPP_DIR}/WEB-INF/lib/*', 'org.apache.ranger.credentialapi.buildks', 'create', alias,
                                  '-value', os.environ['RANGER_ADMIN_DB_PASSWORD'], '-provider', f'jceks://file{provider}'), capture_output=True, text=True)

    if ret.returncode != 0:
        raise BootstrapError(f'failed to store DB password in {provider}\n{ret.stdout.strip()}\n{ret.stderr.strip()}')

    LOG.info(f'✔ Stored DB password with alias {alias} in credential store {provider}')


def import_core_schema(db, applied):
    if 'CORE_DB_SCHEMA' in applied:
        LOG.info(f'Core schema is already imported ({len(applied)} active entries in {VERSION_TABLE})')
        return applied

    schema_file = os.path.join(RANGER_ADMIN_DIR, 'db', db.db_dir, 'optimized/current', f'ranger_core_db_{db.db_dir}.sql')
    start       = time.time()

    LOG.info(f'Importing core schema {schema_file}')

    try:
        db.import_file(schema_file)
    except BootstrapError as err:
        LOG.error(f'✖ Import of core schema {os.path.basename(schema_file)} failed: {err.describe()}')
        raise BootstrapError('core schema import failed') from None

    # as db_setup.py does: entries inserted by the core schema script are attributed to the installed version
    db.query(f"update {VERSION_TABLE} set inst_by='{db.inst_by}' where active='Y' and updated_by='localhost';")

    applied = db.applied_versions()

    if 'CORE_DB_SCHEMA' not in applied:
        raise BootstrapError(f'core schema import did not record CORE_DB_SCHEMA in {VERSION_TABLE}')

    LOG.info(f"✔ Imported core schema in {time.time() - start:.1f}s: {len(applied)} active entries in {VERSION_TABLE}, attributed to '{db.inst_by}'")

    return applied


def apply_java_patch(db, applied, class_file, progress):
    class_name, version = JAVA_PATCH_RE.match(os.path.basename(class_file)).groups()

    if f'J{version}' in applied:
        return

    start       = time.time()
    ret, output = db.run_java(f'org.apache.ranger.patch.{class_name}')

    if ret != 0:
        raise BootstrapError(f'Java patch {class_name} exited with code {ret}', output)

    db.mark_applied(f'J{version}')
    applied.add(f'J{version}')

    # output of the patch is in ranger_db_patch.log; it is shown on console only when the patch fails
    LOG.info(f'{progress} ✔ Applied Java patch {class_name} in {time.time() - start:.1f}s')


def java_patches(prefix):
    classes = glob.glob(os.path.join(WEBAPP_DIR, 'WEB-INF/classes/org/apache/ranger/patch', f'{prefix}*.class'))

    return sorted((c for c in classes if JAVA_PATCH_RE.match(os.path.basename(c))), key=lambda c: JAVA_PATCH_RE.match(os.path.basename(c)).group(2))


def mark_patches_status(db, marker):
    # as db_setup.py does: records that all patches of the installed version have been applied
    if not db.query(f"select version from {VERSION_TABLE} where version='{marker}' and inst_by='{db.inst_by}' and active='Y';"):
        db.mark_applied(marker)

        LOG.info(f"✔ Recorded {marker} for '{db.inst_by}' in {VERSION_TABLE}")


def apply_patches(db, applied):
    sql_patches = sorted(glob.glob(os.path.join(RANGER_ADMIN_DIR, 'db', db.db_dir, 'patches', '*.sql')))
    pending_sql = [patch for patch in sql_patches if os.path.basename(patch).split('-')[0] not in applied]

    LOG.info(f'SQL patches: {len(sql_patches)} available, {len(pending_sql)} pending')

    for idx, patch_file in enumerate(pending_sql, 1):
        name     = os.path.basename(patch_file)
        version  = name.split('-')[0]
        progress = f'[{idx}/{len(pending_sql)}]'
        start    = time.time()

        try:
            for pre_patch in java_patches(f'PatchPreSql_{version}_'):
                apply_java_patch(db, applied, pre_patch, f'{progress}   ↳ pre-SQL')

            db.import_file(patch_file)
            db.mark_applied(version)
            applied.add(version)

            for post_patch in java_patches(f'PatchPostSql_{version}_'):
                apply_java_patch(db, applied, post_patch, f'{progress}   ↳ post-SQL')
        except BootstrapError as err:
            LOG.error(f'{progress} ✖ SQL patch {name} failed, after {idx - 1} of {len(pending_sql)} pending SQL patches were applied: {err.describe()}')
            raise BootstrapError(f'SQL patch {name} failed ({idx}/{len(pending_sql)})') from None

        LOG.info(f'{progress} ✔ Applied SQL patch {name} in {time.time() - start:.1f}s')

    mark_patches_status(db, 'DB_PATCHES')

    patches         = java_patches('Patch')
    pending_patches = [patch for patch in patches if f'J{JAVA_PATCH_RE.match(os.path.basename(patch)).group(2)}' not in applied]

    LOG.info(f'Java patches: {len(patches)} available, {len(pending_patches)} pending')

    for idx, patch in enumerate(pending_patches, 1):
        progress = f'[{idx}/{len(pending_patches)}]'

        try:
            apply_java_patch(db, applied, patch, progress)
        except BootstrapError as err:
            LOG.error(f'{progress} ✖ {err}, after {idx - 1} of {len(pending_patches)} pending Java patches were applied' + (f':\n{indent(err.details)}' if err.details else ''))
            raise BootstrapError(f'Java patch {os.path.basename(patch)[:-6]} failed ({idx}/{len(pending_patches)})') from None

    mark_patches_status(db, 'JAVA_PATCHES')


def update_builtin_user_passwords(db, applied):
    if 'DEFAULT_ALL_ADMIN_UPDATE' in applied:
        LOG.info('Default passwords of built-in users are already updated (DEFAULT_ALL_ADMIN_UPDATE)')
        return

    users = [user for user in BUILTIN_USERS if user[2] not in applied and os.environ.get(user[3])]

    for login_id, _, marker, env in BUILTIN_USERS:
        if marker in applied:
            LOG.info(f'Default password of {login_id} is already updated ({marker})')
        elif not os.environ.get(env):
            LOG.warning(f'env {env} is not set: default password of {login_id} is not updated')

    if not users:
        return

    start = time.time()
    names = ', '.join(login_id for login_id, _, _, _ in users)

    # arguments: <login_id> <current password> <new password> ... -default
    args        = [arg for login_id, default_pwd, _, env in users for arg in (login_id, default_pwd, os.environ[env])]
    ret, output = db.run_java('org.apache.ranger.patch.cliutil.ChangePasswordUtil', *args, '-default')

    if ret not in (0, 2):
        LOG.error(f'✖ Update of default passwords of {names} failed, ChangePasswordUtil exited with code {ret}:\n{indent(output)}')
        raise BootstrapError('update of default passwords failed')

    # as db_setup.py does: a single marker when passwords of all built-in users are updated together
    markers = ['DEFAULT_ALL_ADMIN_UPDATE'] if len(users) == len(BUILTIN_USERS) else [marker for _, _, marker, _ in users]

    for marker in markers:
        db.mark_applied(marker)

    # output of ChangePasswordUtil is in ranger_db_patch.log; it is shown on console only when it fails
    if ret == 2:
        LOG.warning(f"Update of default passwords of {names} skipped: current password of a user is not the default one, i.e. it was already changed; recorded {', '.join(markers)}")
    else:
        LOG.info(f"✔ Updated default passwords of {names} in {time.time() - start:.1f}s, recorded {', '.join(markers)}")


def main():
    start = time.time()

    print_section('dba.py · Ranger Admin configuration and database bootstrap')

    LOG.info(f"Preparing Ranger Admin {os.environ.get('RANGER_VERSION', '')} (RANGER_DB_TYPE={os.environ.get('RANGER_DB_TYPE', '')})")

    render_site_xml()

    props = load_site_properties()
    db    = RangerDB(props)

    try:
        create_credential_store(props)

        db.wait_until_ready(int(os.environ.get('RANGER_DB_WAIT_TIMEOUT', '300')))

        applied = import_core_schema(db, db.applied_versions())

        apply_patches(db, applied)
        update_builtin_user_passwords(db, applied)
    finally:
        os.remove(db.password_file)

    LOG.info(f'✔ Ranger Admin configuration and database are ready in {time.time() - start:.1f}s')


if __name__ == '__main__':
    try:
        main()
    except (BootstrapError, OSError, ET.ParseError, yaml.YAMLError) as err:
        LOG.error(f'✖ Ranger Admin bootstrap failed: {err}')
        sys.exit(1)
