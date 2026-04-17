import os
import re
import xml.etree.ElementTree as ET

DEFAULT_RANGER_ADMIN_SITE_CANDIDATES = (
    os.environ.get("RANGER_ADMIN_SITE_XML"),
    os.path.join(
        os.environ.get(
            "RANGER_ADMIN_CONF",
            "/opt/ranger/admin/ews/webapp/WEB-INF/classes/conf",
        ),
        "ranger-admin-site.xml",
    ),
    "/opt/ranger/admin/ews/webapp/WEB-INF/classes/conf/ranger-admin-site.xml",
)
DEFAULT_RANGER_ADMIN_BASE_URL = os.environ.get("RANGER_ADMIN_BASE_URL", "http://127.0.0.1:6080").rstrip("/")


def _find_ranger_admin_site_xml(property_file=None):
    candidates = []
    if property_file:
        candidates.append(property_file)
    for candidate in DEFAULT_RANGER_ADMIN_SITE_CANDIDATES:
        if candidate:
            candidates.append(candidate)

    for candidate in candidates:
        if os.path.isfile(candidate):
            return candidate
    return ""


def load_ranger_admin_site_properties(property_file=None):
    property_file = _find_ranger_admin_site_xml(property_file)
    properties = {}
    if not property_file:
        return properties

    try:
        tree = ET.parse(property_file)
        root = tree.getroot()
        for child in root.findall("property"):
            name_elem = child.find("name")
            value_elem = child.find("value")
            if name_elem is None or value_elem is None or not name_elem.text:
                continue
            properties[name_elem.text.strip()] = (value_elem.text or "").strip()
    except (ET.ParseError, AttributeError, OSError):
        pass
    return properties


def parse_jdbc_url(jdbc_url):
    info = {"flavor": "", "host": "", "port": "", "database": ""}
    if not jdbc_url:
        return info

    match = re.match(r"jdbc:(postgresql|mysql)://([^/:;]+)(?::(\d+))?/([^?;]+)", jdbc_url)
    if match:
        flavor = match.group(1).upper()
        info["flavor"] = "POSTGRES" if flavor == "POSTGRESQL" else flavor
        info["host"] = match.group(2)
        info["port"] = match.group(3) or ("5432" if flavor == "POSTGRESQL" else "3306")
        info["database"] = match.group(4)
        return info

    match = re.match(r"jdbc:sqlserver://([^:;]+)(?::(\d+))?(?:;|$)", jdbc_url)
    if match:
        info["flavor"] = "MSSQL"
        info["host"] = match.group(1)
        info["port"] = match.group(2) or "1433"
        db_match = re.search(r"(?:^|;)databaseName=([^;]+)", jdbc_url)
        if db_match:
            info["database"] = db_match.group(1)
        return info

    match = re.match(r"jdbc:oracle:thin:@//([^/:]+)(?::(\d+))?/([^?;]+)", jdbc_url)
    if match:
        info["flavor"] = "ORACLE"
        info["host"] = match.group(1)
        info["port"] = match.group(2) or "1521"
        info["database"] = match.group(3)
        return info

    match = re.match(r"jdbc:oracle:thin:@([^:]+):(\d+):([^?;]+)", jdbc_url)
    if match:
        info["flavor"] = "ORACLE"
        info["host"] = match.group(1)
        info["port"] = match.group(2)
        info["database"] = match.group(3)
    return info


def get_ranger_client(base_url=None):
    from apache_ranger.client.ranger_client import RangerClient

    admin_pass = os.environ.get("RANGER_ADMIN_PASSWORD", "")
    return RangerClient((base_url or DEFAULT_RANGER_ADMIN_BASE_URL), ("admin", admin_pass))
