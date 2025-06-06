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
import requests
import time
import json
from dotenv import load_dotenv

# Load environment variables from .env
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Path to the .env file in the parent directory
ENV_PATH = os.path.join(SCRIPT_DIR, "..", ".env")

# Load it
load_dotenv(dotenv_path=ENV_PATH)

RANGER_HOST = "http://localhost:6080"
ENDPOINT = f"{RANGER_HOST}/service/public/v2/api/plugins/info"
KNOX_ENDPOINT = "https://localhost:8443/gateway/sandbox/webhdfs/v1/?op=LISTSTATUS"

RANGER_ADMIN_USER = os.getenv("RANGER_ADMIN_USER")
RANGER_ADMIN_PASS = os.getenv("RANGER_ADMIN_PASS")
KNOX_USER = os.getenv("KNOX_USER")
KNOX_PASS = os.getenv("KNOX_PASS")

expected_services = ["hdfs", "hbase", "kms", "yarn", "kafka", "ozone", "knox", "hive"]


def trigger_knox_activity():
    print("\nTriggering Knox activity to ensure plugin status is updated...")
    try:
        response = requests.get(
            KNOX_ENDPOINT,
            auth=(KNOX_USER,KNOX_PASS),
            verify=False,
            timeout=10
        )
        print("Knox activity triggered.")
    except requests.RequestException as e:
        print(f"Failed to trigger Knox activity: {e}")

def fetch_plugin_info():
    print(f"\nFetching plugin info from {ENDPOINT} ...")
    try:
        response = requests.get(
            ENDPOINT,
            auth=(RANGER_ADMIN_USER,RANGER_ADMIN_PASS),
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching plugin info: {e}")
        exit(1)

def check_plugins(plugin_data):

    failed = False
    print("\n<---------  Plugin Status  ---------->")
    for svc in expected_services:
        print(f"\nChecking service type: {svc}")
        entries = [entry for entry in plugin_data if entry.get("serviceType") == svc]

        if not entries:
            print(f"MISSING: No plugins found for service type '{svc}'.")
            failed = True
            continue

        active_plugins = [
            entry for entry in entries
            if entry.get("info", {}).get("policyActiveVersion")
        ]
        print(f"🟢 Active plugins: {len(active_plugins)} / {len(entries)} total plugins found.")

        if not active_plugins:
            print(f"WARNING: Plugins present but NONE are active for '{svc}'.")
            failed = True

        print("Details:")
        for entry in entries:
            host = entry.get("hostName", "unknown")
            app_type = entry.get("appType", "unknown")
            version = entry.get("info", {}).get("policyActiveVersion", "null")
            print(f"- Host: {host}, AppType: {app_type}, PolicyActiveVersion: {version}")
    return failed


def main():

    print("Checking Ranger plugin status via Ranger Admin API")
    
    # Trigger knox activity
    trigger_knox_activity()
    
    # wait for status update
    time.sleep(60)
    
    # fetch plugin info through admin API
    plugin_data = fetch_plugin_info()

    if not plugin_data:
        print("No plugin info returned from API.")
        exit(1)
    
    # get plugin details
    failed = check_plugins(plugin_data)

    print()
    if failed:
        print("❌ One or more plugins are missing or inactive.")
        exit(1)
    else:
        print("✅ All expected plugins are present and active.")

if __name__ == "__main__":
    main()

