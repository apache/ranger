#!/bin/bash

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

# Script to create alice and abram users in ranger containers
# This script is designed to be run during container initialization

# Function to create a user for testing.
create_user_if_not_exists() {
    local username=$1
    local home_dir=$2

    if ! id "$username" &>/dev/null; then
        echo "Creating user: $username"
        useradd -m -d "$home_dir" -s /bin/bash "$username"

        # Set a default password
        echo "$username:$username" | chpasswd

        echo "User $username created successfully"
    else
        echo "User $username already exists"
    fi
}

# Create alice user
create_user_if_not_exists "alice" "/home/alice"
# Create abram user
create_user_if_not_exists "abram"  "/home/abram"
