#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from apache_ranger.client.ranger_kms_client import RangerKMSClient
from apache_ranger.client.ranger_client     import HadoopSimpleAuth
from apache_ranger.model.ranger_kms         import RangerKey
import time


##
## Step 1: create a client to connect to Ranger KMS
##
kms_url  = "http://localhost:9292"
kms_auth = HadoopSimpleAuth("keyadmin")

# For Kerberos authentication
#
# from requests_kerberos import HTTPKerberosAuth
#
# kms_auth = HTTPKerberosAuth()
#
# For HTTP Basic authentication
#
# kms_auth = ("keyadmin", "rangerR0cks!")

print(f"\nUsing Ranger KMS at {kms_url}")

kms_client = RangerKMSClient(kms_url, kms_auth)

##
## Step 2: call KMS APIs
##
kms_status = kms_client.kms_status()
print("kms_status():", kms_status)
print()

key_name = "test_" + str(int(time.time() * 1000))

key = kms_client.create_key(RangerKey({"name": key_name}))
print("create_key(" + key_name + "):", key)
print()

rollover_key = kms_client.rollover_key(key_name, key.material)
print("rollover_key(" + key_name + "):", rollover_key)
print()

kms_client.invalidate_cache_for_key(key_name)
print("invalidate_cache_for_key(" + key_name + ")")
print()

key_metadata = kms_client.get_key_metadata(key_name)
print("get_key_metadata(" + key_name + "):", key_metadata)
print()

current_key = kms_client.get_current_key(key_name)
print("get_current_key(" + key_name + "):", current_key)
print()

encrypted_keys = kms_client.generate_encrypted_key(key_name, 2)
print("generate_encrypted_key(" + key_name + ", 2):")
for i in range(len(encrypted_keys)):
    encrypted_key   = encrypted_keys[i]
    decrypted_key   = kms_client.decrypt_encrypted_key(key_name, encrypted_key.versionName, encrypted_key.iv, encrypted_key.encryptedKeyVersion.material)
    reencrypted_key = kms_client.reencrypt_encrypted_key(key_name, encrypted_key.versionName, encrypted_key.iv, encrypted_key.encryptedKeyVersion.material)
    print("  encrypted_keys[" + str(i) + "]: ", encrypted_key)
    print("  decrypted_key[" + str(i) + "]:  ", decrypted_key)
    print("  reencrypted_key[" + str(i) + "]:", reencrypted_key)
print()

kms_client.delete_key(key_name)
print("delete_key(" + key_name + ")")
