#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import re


def set_property(text, name, value):
    pattern = rf"(<name>{re.escape(name)}</name>\s*<value>)[^<]*(</value>)"
    if re.search(pattern, text):
        return re.sub(pattern, lambda match: f"{match.group(1)}{value}{match.group(2)}", text, count=1)

    insertion = (
        f"\t<property>\n"
        f"\t\t<name>{name}</name>\n"
        f"\t\t<value>{value}</value>\n"
        f"\t</property>\n"
        f"</configuration>"
    )
    return text.replace("</configuration>", insertion, 1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("audit_xml")
    parser.add_argument("--auditserver-url")
    parser.add_argument("--spool-dir")
    parser.add_argument("--kerberos-enabled", action="store_true")
    args = parser.parse_args()

    with open(args.audit_xml, encoding="utf-8") as handle:
        text = handle.read()

    text = set_property(text, "xasecure.audit.is.enabled", "true")
    text = set_property(text, "xasecure.audit.destination.auditserver", "true")

    if args.auditserver_url:
        text = set_property(text, "xasecure.audit.destination.auditserver.url", args.auditserver_url)

    if args.spool_dir:
        text = set_property(
            text,
            "xasecure.audit.destination.auditserver.batch.filespool.dir",
            args.spool_dir,
        )

    if args.kerberos_enabled:
        text = set_property(text, "xasecure.audit.destination.auditserver.authn.type", "kerberos")

    with open(args.audit_xml, "w", encoding="utf-8") as handle:
        handle.write(text)


if __name__ == "__main__":
    main()
