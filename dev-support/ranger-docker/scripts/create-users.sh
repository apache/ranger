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
    local uid=$2
    local gid=$3
    local home_dir=$4

    if ! id "$username" &>/dev/null; then
        echo "Creating user: $username (uid:$uid, gid:$gid)"
        useradd -u "$uid" -g "$gid" -m -d "$home_dir" -s /bin/bash "$username"

        # Set a default password (same as username for demo purposes)
        echo "$username:$username" | chpasswd

        # Add user to hadoop group for HDFS access
        if getent group hadoop &>/dev/null; then
            usermod -a -G hadoop "$username"
        fi

        # Create .ssh directory and set proper permissions
        mkdir -p "$home_dir/.ssh"
        chmod 700 "$home_dir/.ssh"
        chown "$username:$gid" "$home_dir/.ssh"

        echo "User $username created successfully"
    else
        echo "User $username already exists"
    fi
}

# Ensure hadoop group exists (gid 1001 is used by hdfs, yarn, hive users)
if ! getent group hadoop &>/dev/null; then
    groupadd -g 1001 hadoop
    echo "Created hadoop group"
fi

# Create alice user (uid: 2001, gid: 1001 - hadoop group)
create_user_if_not_exists "alice" 2001 1001 "/home/alice"

# Create abram user (uid: 2002, gid: 1001 - hadoop group)
create_user_if_not_exists "abram" 2002 1001 "/home/abram"
