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

import os
import time
from dotenv import load_dotenv
from apache_ranger.client.ranger_client import RangerClient

# Load environment variables from .env
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Path to the .env file in the parent directory
ENV_PATH = os.path.join(SCRIPT_DIR, "..", ".env")

# Load it
load_dotenv(dotenv_path=ENV_PATH)

RANGER_ADMIN_USER = os.getenv("RANGER_ADMIN_USER")
RANGER_ADMIN_PASS = os.getenv("RANGER_ADMIN_PASS")
KNOX_USER = os.getenv("KNOX_USER")
KNOX_PASS = os.getenv("KNOX_PASS")

# Ranger Admin URL and credentials
ranger_url = "http://localhost:6080"
credentials = (RANGER_ADMIN_USER, RANGER_ADMIN_PASS)

# Initialize the Ranger client
ranger = RangerClient(ranger_url, credentials)

PLUGIN_INFO_ENDPOINT = "/service/public/v2/api/plugins/info"
KNOX_ENDPOINT = "https://localhost:8443/gateway/sandbox/webhdfs/v1/?op=LISTSTATUS"

RETRY_COUNT = int(os.getenv("PLUGIN_RETRY_COUNT", 4))
RETRY_INTERVAL = int(os.getenv("PLUGIN_RETRY_INTERVAL", 30))

expected_services = ["hdfs", "hbase", "kms", "yarn", "kafka", "ozone", "knox", "hive"]


def init_knox_plugin():
    print("\nTriggering Knox activity to ensure plugin status is updated...")
    try:
        response = ranger.session.get(
            KNOX_ENDPOINT,
            auth=(KNOX_USER,KNOX_PASS),
            verify=False,
            timeout=10
        )
        print("Knox activity triggered.")
    except Exception as e:
        print(f"Failed to trigger Knox activity: {e}")

def fetch_plugin_info():
    print(f"\nFetching plugin info from {PLUGIN_INFO_ENDPOINT} ...")
    try:
        response = ranger.session.get(ranger_url + PLUGIN_INFO_ENDPOINT)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching plugin info: {e}")
        exit(1)

def check_plugins(plugin_data):

    failed_services = []
    print("\n<---------  Plugin Status  ---------->")
    for svc in expected_services:
        print(f"\nChecking service type: {svc}")
        entries = [entry for entry in plugin_data if entry.get("serviceType") == svc]

        if not entries:
            print(f"MISSING: No plugins found for service type '{svc}'.")
            failed_services.append(svc)
            continue

        active_plugins = [
            entry for entry in entries
            if entry.get("info", {}).get("policyActiveVersion")
        ]
        print(f"🟢 Active plugins: {len(active_plugins)} / {len(entries)} total plugins found.")

        if not active_plugins:
            print(f"WARNING: Plugins present but NONE are active for '{svc}'.")
            failed_services.append(svc)

        print("Details:")
        for entry in entries:
            host = entry.get("hostName", "unknown")
            app_type = entry.get("appType", "unknown")
            version = entry.get("info", {}).get("policyActiveVersion", "null")
            print(f"- Host: {host}, AppType: {app_type}, PolicyActiveVersion: {version}")
    return failed_services


def main():

    print("Checking Ranger plugin status via Ranger Admin API")
    
    # Trigger knox activity
    init_knox_plugin()
    
    # wait for status update
    for i in range(RETRY_COUNT):         #retrying upto RETRY_COUNT * RETRY_INTERVAL seconds
        plugin_data = fetch_plugin_info()
        if all(any(entry.get("info", {}).get("policyActiveVersion") for entry in plugin_data if entry["serviceType"] == svc) for svc in expected_services):
           break
        print(f"Some plugins not active yet, retrying in {RETRY_INTERVAL}s...")
        time.sleep(RETRY_INTERVAL)

    else:
        print("Timed out waiting for plugins to become active.")
    
    # fetch plugin info through admin API
    plugin_data = fetch_plugin_info()

    if not plugin_data:
        print("No plugin info returned from API.")
        exit(1)
    
    # get plugin details
    failed_services = check_plugins(plugin_data)

    print()
    if failed_services:
        print(f"❌ The following plugins are missing or inactive: {', '.join(failed_services)}")
        exit(1)
    else:
        print("✅ All expected plugins are present and active.")

if __name__ == "__main__":
    main()

