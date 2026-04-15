#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#

import os
import re
import shlex
import shutil
import subprocess
import sys
import hashlib
from dataclasses import dataclass

from log_config import configure_logging, get_logger
from ranger_admin_xml_config import load_ranger_admin_site_properties, parse_jdbc_url

logger = get_logger(__name__)

JISQL_DEBUG = True

# Regex used to extract the database name from a JDBC override URL.
_JDBC_DB_NAME_RE = re.compile(r"jdbc:postgresql://[^/]+:\d+/(\w+)")

# Allowlist for sequence names interpolated into SQL commands.
_SAFE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

RANGER_HOME = os.getenv("RANGER_HOME")

# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------
class ConfigError(RuntimeError):
    """Raised when required configuration is missing or invalid."""


class ConnectionError(RuntimeError):  # noqa: A001 — shadows builtin intentionally
    """Raised when the database connection check fails."""


class SchemaImportError(RuntimeError):
    """Raised when a schema file import fails."""


# ---------------------------------------------------------------------------
# SSL configuration dataclass
# ---------------------------------------------------------------------------
@dataclass
class SSLConfig:
    """Holds all SSL-related settings with sensible defaults."""

    enabled: bool = False
    required: bool = False
    verify_server_certificate: bool = False
    auth_type: str = "2-way"
    certificate_file: str = ""
    key_store: str = ""
    key_store_password: str = ""
    key_store_type: str = "bcfks"
    trust_store: str = ""
    trust_store_password: str = ""
    trust_store_type: str = "bcfks"
    override_jdbc: bool = False
    override_jdbc_connection_string: str = ""


def _is_true(config: dict, key: str) -> bool:
    """Return True if *config[key]* is the string 'true' (case-insensitive)."""
    return config.get(key, "false").lower() == "true"


def _extract_ssl_config(config: dict) -> SSLConfig:
    """Build an SSLConfig from the raw config dictionary."""

    ssl = SSLConfig()
    ssl.override_jdbc = _is_true(config, "is_override_db_connection_string")
    ssl.override_jdbc_connection_string = config.get("db_override_connection_string", "").strip()
    ssl.enabled = _is_true(config, "db_ssl_enabled")

    if not ssl.enabled:
        return ssl

    ssl.required = _is_true(config, "db_ssl_required")
    ssl.verify_server_certificate = _is_true(config, "db_ssl_verifyServerCertificate")
    ssl.auth_type = config.get("db_ssl_auth_type", "2-way").lower()
    ssl.certificate_file = config.get("db_ssl_certificate_file", "")
    ssl.trust_store = config.get("javax_net_ssl_trustStore", "")
    ssl.trust_store_password = config.get("javax_net_ssl_trustStorePassword", "")
    ssl.trust_store_type = config.get("javax_net_ssl_trustStore_type", "bcfks")
    ssl.key_store = config.get("javax_net_ssl_keyStore", "")
    ssl.key_store_password = config.get("javax_net_ssl_keyStorePassword", "")
    ssl.key_store_type = config.get("javax_net_ssl_keyStore_type", "bcfks")

    return ssl


def _validate_ssl_files(ssl: SSLConfig) -> None:
    """Validate that required SSL files and passwords exist.

    Raises ConfigError instead of calling sys.exit so callers
    can handle the error or let it propagate to main.
    """
    if not ssl.enabled or not ssl.verify_server_certificate:
        return

    if ssl.certificate_file:
        if not os.path.exists(ssl.certificate_file):
            raise ConfigError(f"SSL certificate file not found: {ssl.certificate_file}")
    elif ssl.auth_type == "1-way":
        if not os.path.exists(ssl.trust_store):
            raise ConfigError(f"SSL truststore file not found: {ssl.trust_store}")
        if not ssl.trust_store_password:
            raise ConfigError("SSL truststore password is not set")

    if ssl.auth_type == "2-way":
        if not os.path.exists(ssl.key_store):
            raise ConfigError(f"SSL keystore file not found: {ssl.key_store}")
        if not ssl.key_store_password:
            raise ConfigError("SSL keystore password is not set")


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------
def _run_command(query: str) -> str:
    """Run a shell command and return its stdout."""
    result = subprocess.run(shlex.split(query), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
    return result.stdout


def _log_jisql(query: str, db_password: str) -> None:
    """Log a Jisql command with the password masked."""
    if JISQL_DEBUG:
        logger.info("JISQL %s", query.replace(f" -p '{db_password}'", " -p '********'"))


def _validate_identifier(name: str) -> str:
    """Return *name* if it matches a strict SQL identifier pattern.

    Prevents accidental SQL injection when sequence/table names are
    interpolated into command strings.
    """
    if not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(f"Unsafe SQL identifier rejected: {name!r}")
    return name


def load_runtime_config() -> dict:
    """Load runtime config using XML for JDBC metadata and env for secrets.

    Raises ConfigError if required settings are missing.
    """
    props = load_ranger_admin_site_properties()
    if not props:
        raise ConfigError(f"ranger-admin-site.xml not found or unreadable, RANGER_HOME={RANGER_HOME}")

    jdbc_url = props.get("ranger.jpa.jdbc.url", "")
    jdbc_info = parse_jdbc_url(jdbc_url)

    db_flavor = (os.environ.get("RANGER_DB_TYPE") or jdbc_info.get("flavor") or "POSTGRES").upper()
    if db_flavor == "POSTGRESQL":
        db_flavor = "POSTGRES"

    db_host = os.environ.get("RANGER_ADMIN_DB_HOSTNAME") or jdbc_info.get("host", "")
    db_port = os.environ.get("RANGER_ADMIN_DB_PORT") or jdbc_info.get("port", "")
    db_name = os.environ.get("RANGER_ADMIN_DB_DATABASE") or jdbc_info.get("database", "")
    db_user = os.environ.get("RANGER_ADMIN_DB_USERNAME") or props.get("ranger.jpa.jdbc.user", "")
    db_password = os.environ.get("RANGER_ADMIN_DB_PASSWORD", "")

    if not all([db_host, db_name, db_user]):
        raise ConfigError("Required JDBC settings (host/name/user) missing from ranger-admin-site.xml")
    if not db_password:
        raise ConfigError("Required env var RANGER_ADMIN_DB_PASSWORD is not set")

    config = dict(props)
    config["DB_FLAVOR"] = db_flavor
    config["SQL_CONNECTOR_JAR"] = props.get("ranger.jdbc.sqlconnectorjar", "/usr/share/java/postgresql.jar")
    config["db_name"] = db_name
    config["db_host"] = f"{db_host}:{db_port}" if db_port else db_host
    config["db_user"] = db_user
    config["db_password"] = db_password
    config["postgres_core_file"] = "db/postgres/optimized/current/ranger_core_db_postgres.sql"
    return config


# ---------------------------------------------------------------------------
# Database abstraction
# ---------------------------------------------------------------------------
class BaseDB:
    """Interface that every DB flavour must implement."""

    def check_connection(self, db_name, db_user, db_password):
        logger.info("---------- Verifying DB connection ----------")

    def check_table(self, db_name, db_user, db_password, table_name):
        logger.info("---------- Verifying table ----------")

    def import_db_file(self, db_name, db_user, db_password, file_name):
        logger.info("---------- Importing db schema ----------")


class PostgresDB(BaseDB):
    """PostgreSQL-specific implementation of the DB bootstrap interface."""

    def __init__(self, *, host: str, sql_connector_jar: str, java_bin: str, ssl: SSLConfig):
        self.host = host
        self.sql_connector_jar = sql_connector_jar
        self.java_bin = java_bin.strip("'")
        self.ssl = ssl

    # -- private helpers ---------------------------------------------------

    def _resolve_db_name(self, db_name: str) -> str:
        """Extract the actual DB name from a JDBC override URL if active."""
        if not (self.ssl.override_jdbc and self.ssl.override_jdbc_connection_string):
            return db_name
        match = _JDBC_DB_NAME_RE.search(self.ssl.override_jdbc_connection_string)
        return match.group(1) if match else db_name

    def _build_jisql_classpath(self) -> str:
        """Locate Jisql lib directories and build a Java classpath."""
        candidates = [os.path.join(RANGER_HOME, "admin", "jisql", "lib"), os.path.join(RANGER_HOME, "jisql", "lib")]
        found = [d for d in candidates if os.path.isdir(d)]
        if not found:
            found = [candidates[0]]  # Fall back to the most common layout.
        return os.pathsep.join(os.path.join(d, "*") for d in found)

    def _build_ssl_params(self) -> tuple[str, str]:
        """Return (url_param, jvm_cert_flags) for SSL."""
        if not self.ssl.enabled:
            return "", ""

        ssl = self.ssl

        if ssl.certificate_file:
            return f"?ssl=true&sslmode=verify-full&sslrootcert={ssl.certificate_file}", ""

        if ssl.verify_server_certificate or ssl.required:
            url_param = "?ssl=true&sslmode=verify-full&sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory"
            if ssl.auth_type == "1-way":
                cert_flags = (
                    f" -Djavax.net.ssl.trustStore={ssl.trust_store}"
                    f" -Djavax.net.ssl.trustStorePassword={ssl.trust_store_password}"
                    f" -Djavax.net.ssl.trustStoreType={ssl.trust_store_type}"
                )
            else:
                cert_flags = (
                    f" -Djavax.net.ssl.keyStore={ssl.key_store}"
                    f" -Djavax.net.ssl.keyStorePassword={ssl.key_store_password}"
                    f" -Djavax.net.ssl.trustStore={ssl.trust_store}"
                    f" -Djavax.net.ssl.trustStorePassword={ssl.trust_store_password}"
                    f" -Djavax.net.ssl.trustStoreType={ssl.trust_store_type}"
                    f" -Djavax.net.ssl.keyStoreType={ssl.key_store_type}"
                )
            return url_param, cert_flags

        return "?ssl=true", ""

    def _get_jisql_cmd(self, user: str, password: str, db_name: str) -> str:
        """Build the full Jisql invocation command string."""
        classpath = self._build_jisql_classpath()
        ssl_url, ssl_cert = self._build_ssl_params()
        cp = f"{self.sql_connector_jar}{os.pathsep}{classpath}"

        if self.ssl.override_jdbc and self.ssl.override_jdbc_connection_string:
            cstring = self.ssl.override_jdbc_connection_string
        else:
            cstring = f"jdbc:postgresql://{self.host}/{db_name}{ssl_url}"

        return (
            f"{self.java_bin} {ssl_cert} -cp {cp} org.apache.util.sql.Jisql"
            f" -driver postgresql -cstring '{cstring}'"
            f" -u {user} -p '{password}' -noheader -trim -c \\;"
        )

    # -- public interface --------------------------------------------------

    def check_connection(self, db_name: str, db_user: str, db_password: str) -> bool:
        """Verify that we can reach the database. Raises ConnectionError on failure."""
        logger.info("Checking connection to database %s", db_name)
        cmd = self._get_jisql_cmd(db_user, db_password, db_name)
        query = f'{cmd} -query "SELECT 1;"'
        _log_jisql(query, db_password)

        output = _run_command(query)
        if "1" in output:
            logger.info("Connection successful")
            return True
        raise ConnectionError(f"Cannot establish connection to database {db_name}")

    def import_db_file(self, db_name: str, db_user: str, db_password: str, file_name: str) -> None:
        """Import a SQL schema file into the database.

        Raises FileNotFoundError if the file is missing, or
        SchemaImportError if the import subprocess fails.
        """
        display_name = os.path.basename(file_name)
        if not os.path.isfile(file_name):
            raise FileNotFoundError(f"DB schema file not found: {display_name}")

        logger.info("Importing schema to %s from file: %s", db_name, display_name)
        cmd = self._get_jisql_cmd(db_user, db_password, db_name)
        query = f"{cmd} -input {file_name}"
        _log_jisql(query, db_password)

        ret = subprocess.call(shlex.split(query))
        if ret != 0:
            raise SchemaImportError(f"Schema import failed for {display_name}")
        logger.info("%s imported successfully", display_name)

    def check_table(self, db_name: str, db_user: str, db_password: str, table_name: str) -> bool:
        """Return True if *table_name* exists in *db_name*."""
        db_name = self._resolve_db_name(db_name)
        logger.info("Verifying table %s in database %s", table_name, db_name)

        cmd = self._get_jisql_cmd(db_user, db_password, db_name)
        query = (
            f'{cmd} -query "SELECT table_name FROM information_schema.tables'
            f" WHERE table_catalog='{db_name}' AND table_name='{table_name}';\""
        )
        _log_jisql(query, db_password)

        try:
            output = _run_command(query)
            if output and table_name.lower() in output.lower():
                logger.info("Table %s exists in %s", table_name, db_name)
                return True
            logger.info("Table %s does not exist in %s", table_name, db_name)
            return False
        except (subprocess.SubprocessError, OSError) as exc:
            logger.error("Error checking table: %s", exc)
            return False

    def check_sequence(self, db_name: str, db_user: str, db_password: str, sequence_name: str) -> bool:
        """Return True if *sequence_name* exists in *db_name*."""
        db_name = self._resolve_db_name(db_name)
        seq = _validate_identifier(sequence_name).lower()
        logger.info("Verifying sequence %s in database %s", seq, db_name)

        cmd = self._get_jisql_cmd(db_user, db_password, db_name)
        query = (
            f'{cmd} -query "SELECT sequence_name FROM information_schema.sequences'
            f" WHERE sequence_schema='public' AND sequence_name='{seq}';\""
        )
        _log_jisql(query, db_password)

        try:
            output = _run_command(query)
            if output and seq in output.lower():
                logger.info("Sequence %s exists in %s", seq, db_name)
                return True
            logger.info("Sequence %s does not exist in %s", seq, db_name)
            return False
        except (subprocess.SubprocessError, OSError) as exc:
            logger.error("Error checking sequence: %s", exc)
            return False

    def ensure_sequence(self, db_name: str, db_user: str, db_password: str, sequence_name: str) -> bool:
        """Ensure a sequence exists, creating it if necessary.

        Some Ranger distributions create sequences via patch SQL; when
        bootstrapping with only the core schema, we may need to create
        them explicitly.
        """
        if self.check_sequence(db_name, db_user, db_password, sequence_name):
            return True

        seq = _validate_identifier(sequence_name).lower()
        logger.warning("Attempting to create missing sequence: %s", seq)
        cmd = self._get_jisql_cmd(db_user, db_password, db_name)

        for sql in (f"CREATE SEQUENCE {seq};", f"CREATE SEQUENCE IF NOT EXISTS {seq};"):
            query = f'{cmd} -query "{sql}"'
            _log_jisql(query, db_password)
            try:
                subprocess.call(shlex.split(query))
            except (subprocess.SubprocessError, OSError) as exc:
                logger.warning("Sequence create raised: %s", exc)

            if self.check_sequence(db_name, db_user, db_password, sequence_name):
                logger.info("Sequence %s is present", seq)
                return True

        logger.error("Failed to ensure required sequence: %s", seq)
        return False

    def update_portal_user_password(self, db_name: str, db_user: str, db_password: str, login_id: str, plain_password: str) -> None:
        """Set a portal user's password using Ranger's legacy seed encoding.

        Fresh schema imports seed admin/usersync/tagsync with fixed hashes in SQL.
        Rewrite those seeded hashes from env before Ranger Admin starts so first
        login matches the container configuration.
        """
        encoded_password = hashlib.md5(f"{plain_password}{{{login_id}}}".encode("utf-8")).hexdigest()
        cmd = self._get_jisql_cmd(db_user, db_password, db_name)
        query = (
            f'{cmd} -query "UPDATE x_portal_user '
            f"SET password='{encoded_password}' "
            f"WHERE login_id='{login_id}';\""
        )
        logger.info("Setting initial password for Ranger user %s from environment", login_id)
        _log_jisql(query, db_password)

        ret = subprocess.call(shlex.split(query))
        if ret != 0:
            raise SchemaImportError(f"Failed to seed password for Ranger user {login_id}")


# ---------------------------------------------------------------------------
# Java binary resolution
# ---------------------------------------------------------------------------
def _resolve_java_bin() -> str:
    """Determine the path to the java binary."""
    java_home = os.environ.get("JAVA_HOME", "").strip()
    if java_home:
        return os.path.join(java_home, "bin", "java")
    java_bin = shutil.which("java") or "java"
    logger.warning("JAVA_HOME not set; using JAVA_BIN=%s", java_bin)
    return java_bin


# ---------------------------------------------------------------------------
# Schema file resolution
# ---------------------------------------------------------------------------
def _resolve_schema_file(core_file_rel: str) -> str:
    """Return the absolute path to the core schema SQL file.

    Raises ConfigError if the file does not exist.
    """
    if os.getenv("RANGER_HOME"):
        path = os.path.join(RANGER_HOME, "admin", core_file_rel)
    else:
        path = os.path.join(RANGER_HOME, core_file_rel)

    logger.info("Schema file path: %s", path)
    if not os.path.isfile(path):
        raise ConfigError(f"Schema file not found: {path} (RANGER_HOME={RANGER_HOME})")
    return path


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
VERSION_TABLE = "x_db_version_h"
CRITICAL_SEQUENCE = "X_TRX_LOG_SEQ"


def seed_initial_user_passwords(db: PostgresDB, db_name: str, db_user: str, db_password: str) -> None:
    for login_id, env_var in (("admin", "RANGER_ADMIN_PASSWORD"), ("rangerusersync", "RANGER_USERSYNC_PASSWORD"), ("rangertagsync", "RANGER_TAGSYNC_PASSWORD")):
        plain_password = os.environ.get(env_var, "")
        if plain_password:
            db.update_portal_user_password(db_name, db_user, db_password, login_id, plain_password)


def main(argv: list[str]) -> None:

    configure_logging()
    config = load_runtime_config()

    db_flavor = config["DB_FLAVOR"]
    if db_flavor != "POSTGRES":
        raise ConfigError("Ranger Admin docker currently supports only PostgreSQL")

    java_bin = _resolve_java_bin()
    logger.info("DB FLAVOR: %s", db_flavor)

    ssl = _extract_ssl_config(config)
    _validate_ssl_files(ssl)

    db_name, db_user, db_password = config["db_name"], config["db_user"], config["db_password"]

    db = PostgresDB(host=config["db_host"], sql_connector_jar=config["SQL_CONNECTOR_JAR"], java_bin=java_bin, ssl=ssl)
    schema_file = _resolve_schema_file(config["postgres_core_file"])

    logger.info("--------- Verifying Ranger DB connection ---------")
    db.check_connection(db_name, db_user, db_password)

    if len(argv) > 1:
        return  # Extra CLI arguments present — skip schema initialisation.

    logger.info("--------- Verifying Ranger DB tables ---------")
    if db.check_table(db_name, db_user, db_password, VERSION_TABLE):
        logger.info("Database schema already initialised")
        if not db.ensure_sequence(db_name, db_user, db_password, CRITICAL_SEQUENCE):
            logger.warning("Critical sequence %s still missing, but schema appears initialised. Service creation may fail.", CRITICAL_SEQUENCE)
        return

    logger.info("--------- Importing Ranger Core DB Schema ---------")
    db.import_db_file(db_name, db_user, db_password, schema_file)
    seed_initial_user_passwords(db, db_name, db_user, db_password)

    if not db.check_table(db_name, db_user, db_password, VERSION_TABLE):
        raise SchemaImportError(f"Schema import completed but {VERSION_TABLE} table not found")

    if not db.ensure_sequence(db_name, db_user, db_password, CRITICAL_SEQUENCE):
        raise SchemaImportError( f"Sequence {CRITICAL_SEQUENCE} required for service creation. Schema import/patch may be incomplete.")

    logger.info("Database schema imported successfully")


if __name__ == "__main__":
    try:
        main(sys.argv)
    except (ConfigError, ConnectionError, SchemaImportError) as exc:
        logger.error("%s", exc)
        sys.exit(1)
