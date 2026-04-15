import argparse
import os
import re
import sys
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
    "/opt/ranger/admin/configs/ranger-admin-site.xml",
    "/opt/ranger/admin/ews/webapp/WEB-INF/classes/conf/ranger-admin-site.xml",
)


class RangerAdminXmlConfig:
    def __init__(self, property_file=None):
        self.property_file = property_file

    def find_ranger_admin_site_xml(self):
        candidates = []
        if self.property_file:
            candidates.append(self.property_file)
        for candidate in DEFAULT_RANGER_ADMIN_SITE_CANDIDATES:
            if candidate:
                candidates.append(candidate)

        for candidate in candidates:
            if os.path.isfile(candidate):
                return candidate
        return ""

    def load_properties(self):
        property_file = self.find_ranger_admin_site_xml()
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

    def get_property(self, property_name):
        return self.load_properties().get(property_name, "")

    def get_config_value(self, env_var, xml_property, default=""):
        return os.environ.get(env_var) or self.get_property(xml_property) or default

    @staticmethod
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

    def get_db_field(self, field):
        props = self.load_properties()
        jdbc_info = self.parse_jdbc_url(props.get("ranger.jpa.jdbc.url", ""))
        values = {
            "flavor": jdbc_info.get("flavor", ""),
            "host": jdbc_info.get("host", ""),
            "port": jdbc_info.get("port", ""),
            "database": jdbc_info.get("database", ""),
            "user": props.get("ranger.jpa.jdbc.user", ""),
            "password": os.environ.get("RANGER_ADMIN_DB_PASSWORD", ""),
        }
        return values.get(field, "")

    def set_property(self, name, value, required, create=False):
        property_file = self.find_ranger_admin_site_xml()
        try:
            tree = ET.parse(property_file)
            root = tree.getroot()
        except (ET.ParseError, OSError) as exc:
            raise SystemExit(f"ERROR: failed to parse {property_file}: {exc}")

        updated = False
        for prop in root.findall("property"):
            name_elem = prop.find("name")
            value_elem = prop.find("value")
            if name_elem is None or value_elem is None:
                continue
            if (name_elem.text or "").strip() != name:
                continue
            value_elem.text = value
            updated = True
            break

        if create and not updated:
            prop = ET.SubElement(root, "property")
            name_elem = ET.SubElement(prop, "name")
            name_elem.text = name
            value_elem = ET.SubElement(prop, "value")
            value_elem.text = value
            updated = True

        if required and not updated:
            raise SystemExit(f"ERROR: {name} missing in {property_file}")

        if updated:
            tree.write(property_file, encoding="unicode")


def load_ranger_admin_site_properties(property_file=None):
    return RangerAdminXmlConfig(property_file).load_properties()


def parse_jdbc_url(jdbc_url):
    return RangerAdminXmlConfig.parse_jdbc_url(jdbc_url)


def get_ranger_client():
    from apache_ranger.client.ranger_client import RangerClient

    admin_pass = os.environ.get("RANGER_ADMIN_PASSWORD", "")
    return RangerClient("http://localhost:6080", ("admin", admin_pass))


def build_cli_parser():
    parser = argparse.ArgumentParser(description="Read and update Ranger admin XML properties")
    subparsers = parser.add_subparsers(dest="command", required=True)

    get_prop_parser = subparsers.add_parser("get-property")
    get_prop_parser.add_argument("--file")
    get_prop_parser.add_argument("--name", required=True)

    get_db_field_parser = subparsers.add_parser("get-db-field")
    get_db_field_parser.add_argument("--file")
    get_db_field_parser.add_argument("--field", required=True)

    set_prop_parser = subparsers.add_parser("set-property")
    set_prop_parser.add_argument("--file")
    set_prop_parser.add_argument("--name", required=True)
    set_prop_parser.add_argument("--value", required=True)
    set_prop_parser.add_argument("--required", action="store_true")
    set_prop_parser.add_argument("--create", action="store_true")

    return parser


def main(argv=None):
    args = build_cli_parser().parse_args(argv)
    config = RangerAdminXmlConfig(args.file)

    if args.command == "get-property":
        print(config.get_property(args.name))
        return 0

    if args.command == "get-db-field":
        print(config.get_db_field(args.field))
        return 0

    if args.command == "set-property":
        config.set_property(args.name, args.value, args.required, args.create)
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
