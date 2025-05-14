import requests     
import pytest  
import time
from utils import fetch_logs     

BASE_URL = "http://localhost:9292/kms/v1"  

BASE_URL_RANGER = "http://localhost:6080/service/public/v2/api/policy"
PARAMS={"user.name":"keyadmin"}  

RANGER_AUTH = ('keyadmin', 'rangerR0cks!')  # Ranger key admin user
KMS_SERVICE_NAME = "dev_kms"

# create base policy
@pytest.fixture(scope="function", autouse=True)
def create_initial_kms_policy():
    policy_data = {
        "policyName": "pytest-policy",
        "service": KMS_SERVICE_NAME,
        "resources": {
            "keyname": {
                "values": ["pytest-*"],  # All keys starting with 'pytest-'
                "isExcludes": False,
                "isRecursive": False
            }
        },
        "policyItems": []
    }

    # Create policy
    response = requests.post(BASE_URL_RANGER, auth=RANGER_AUTH, json=policy_data)
    time.sleep(30)
    if response.status_code != 200 and response.status_code != 201:
        raise Exception(f"Failed to create initial policy: {response.text}")

    created_policy = response.json()
    policy_id = created_policy["id"]
    yield policy_id

    # Optionally delete policy after tests
    requests.delete(f"{BASE_URL_RANGER}/{policy_id}", auth=RANGER_AUTH)

# method to update policy
def update_kms_policy(policy_id, username, accesses):
    update_url = f"{BASE_URL_RANGER}/{policy_id}"

    # Fetch existing policy
    response = requests.get(update_url, auth=RANGER_AUTH)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch policy: {response.text}")
    
    policy_data = response.json()

    # Ensure policyItems key exists
    if "policyItems" not in policy_data:
        policy_data["policyItems"] = []

    # Only add policy item if accesses are provided
    if accesses:
        policy_data["policyItems"].append({
            "accesses": [{"type": access, "isAllowed": True} for access in accesses],
            "users": [username],
            "delegateAdmin": False
        })

    # Update the policy
    response = requests.put(update_url, auth=RANGER_AUTH, json=policy_data)
    time.sleep(60)  # Reduced wait time; increase only if propagation is slow
    if response.status_code != 200:
        raise Exception(f"Failed to update policy: {response.text}")

    


# ****** ********************Test Case 01 ********************************************
# ***** user has "create" access only
# ***********************************************************************************
def test_policy_01(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = "nobody"


    # Update policy for this test
    update_kms_policy(policy_id, username, accesses=["create"])

    key_name = "pytest-key-01" 

    # create key
    response = requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    #get current version
    response = requests.get(f"{BASE_URL}/key/{key_name}/_currentversion",params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Get current version failed: {response.text}"

    # Try getting key metadata
    response = requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    # Try rollover 
    response = requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"
     
    #generate DEK
    response = requests.get(f"{BASE_URL}/key/{key_name}/_dek",params={"user.name": username})
    assert response.status_code == 500, f"Expected 403 but got {response.status_code}: {response.text}"

    #delete key
    response= requests.delete(f"{BASE_URL}/key/{key_name}",params={"user.name": username})
    assert response.status_code == 403, f"Expected 403 but got :{response.text}"

    #cleanup
    requests.delete(f"{BASE_URL}/key/{key_name}",params=PARAMS)



# ****** ********************Test Case 02 ********************************************
# ***** user has "create, delete" access only
# ***********************************************************************************
def test_policy_02(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = "nobody"


    #Update policy for this test
    update_kms_policy(policy_id, username, accesses=["create","delete"])

    key_name = "pytest-key-02" 

    # create key
    response = requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    #get current version
    response = requests.get(f"{BASE_URL}/key/{key_name}/_currentversion",params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Get current version failed: {response.text}"

    # Try getting key metadata 
    response = requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    # Try rollover 
    response = requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"
     
    #generate DEK
    response = requests.get(f"{BASE_URL}/key/{key_name}/_dek",params={"user.name": username})
    assert response.status_code == 500, f"Expected 500 but got {response.status_code}: {response.text}"

    #delete key
    response= requests.delete(f"{BASE_URL}/key/{key_name}",params={"user.name": username})
    assert response.status_code == 200, f"Key deletion failed :{response.text}"
    

# ****** ********************Test Case 03 ********************************************
# ***** user has "create, rollover, delete" access only
# ***********************************************************************************
def test_policy_03(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = "nobody"


    #Update policy for this test
    update_kms_policy(policy_id, username, accesses=["create","delete","rollover"])

    key_name = "pytest-key-03"

    # create key
    response = requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    # Try rollover 
    response = requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"

    #get current version
    response = requests.get(f"{BASE_URL}/key/{key_name}/_currentversion",params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Get current version failed: {response.text}"

    # Try getting key metadata 
    response = requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"
   
    #generate DEK
    response = requests.get(f"{BASE_URL}/key/{key_name}/_dek",params={"user.name": username})
    assert response.status_code == 500, f"Expected 403 but got {response.status_code}: {response.text}"

    #delete key
    response= requests.delete(f"{BASE_URL}/key/{key_name}",params={"user.name": username})
    assert response.status_code == 200, f"Key deletion failed :{response.text}"
    

# ****** ********************Test Case 04 ********************************************
# ***** user has "create, rollover, getKeyVersion, delete" access only
# ***********************************************************************************
def test_policy_04(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = "nobody"


    #Update policy for this test
    update_kms_policy(policy_id, username, accesses=["create","delete","rollover","get"])

    key_name = "pytest-key-04"

    # create key
    response = requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    # Try rollover 
    response = requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"

    #get current version
    response = requests.get(f"{BASE_URL}/key/{key_name}/_currentversion",params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Get current version failed: {response.text}"

    # Try getting key metadata 
    response = requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"
   
    #generate DEK
    response = requests.get(f"{BASE_URL}/key/{key_name}/_dek",params={"user.name": username})
    assert response.status_code == 500, f"Expected 403 but got {response.status_code}: {response.text}"

    #delete key
    response= requests.delete(f"{BASE_URL}/key/{key_name}",params={"user.name": username})
    assert response.status_code == 200, f"Key deletion failed :{response.text}"



# ****** ********************Test Case 05 ********************************************
# ***** user has "create, rollover, getKeyVersion, getMetadata, delete" access only
# ***********************************************************************************
def test_policy_05(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = "nobody"


    #Update policy for this test
    update_kms_policy(policy_id, username, accesses=["create","delete","rollover","get","getmetadata"])

    key_name = "pytest-key-05"

    # create key
    response = requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    # Try rollover 
    response = requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"

    #get current version
    response = requests.get(f"{BASE_URL}/key/{key_name}/_currentversion",params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Get current version failed: {response.text}"

    # Try getting key metadata 
    response = requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 403 but got {response.status_code}: {response.text}"
   
    #generate DEK
    response = requests.get(f"{BASE_URL}/key/{key_name}/_dek",params={"user.name": username})
    assert response.status_code == 500, f"Expected 403 but got {response.status_code}: {response.text}"

    #delete key
    response= requests.delete(f"{BASE_URL}/key/{key_name}",params={"user.name": username})
    assert response.status_code == 200, f"Key deletion failed :{response.text}"



# ****** ********************Test Case 06 ********************************************
# ***** user has "create, rollover, getKeyVersion, getMetadata, generateeek, delete" access only
# ***********************************************************************************
def test_policy_06(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = "nobody"


    #Update policy for this test
    update_kms_policy(policy_id, username, accesses=["create","delete","rollover","get","getmetadata","generateeek"])

    key_name = "pytest-key-06"

    # create key
    response = requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    # Try rollover 
    response = requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"

    #get current version
    response = requests.get(f"{BASE_URL}/key/{key_name}/_currentversion",params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Get current version failed: {response.text}"

    # Try getting key metadata 
    response = requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"
   
    #generate DEK
    DEK_PARAMS= {"eek_op":"generate","num_keys":1,"user.name":username}
    response = requests.get(f"{BASE_URL}/key/{key_name}/_eek",params=DEK_PARAMS)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"

    #delete key
    response= requests.delete(f"{BASE_URL}/key/{key_name}",params={"user.name": username})
    assert response.status_code == 200, f"Key deletion failed :{response.text}"



# ****** ********************Test Case 07 ********************************************
# ***** user has all access "create, rollover, getKeyVersion, getMetadata, generateeek, decrypteek, delete" access
# ***********************************************************************************
def test_policy_07(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = "nobody"


    #Update policy for this test
    update_kms_policy(policy_id, username, accesses=["create","delete","rollover","get","getmetadata","generateeek","decrypteek"])

    key_name = "pytest-key-07"

    # create key
    response = requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    # Try rollover 
    response = requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"

    #get current version
    response = requests.get(f"{BASE_URL}/key/{key_name}/_currentversion",params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Get current version failed: {response.text}"

    # Try getting key metadata
    response = requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 403 but got {response.status_code}: {response.text}"
   
    #generate DEK
    DEK_PARAMS= {"eek_op":"generate","num_keys":1,"user.name":username}
    response = requests.get(f"{BASE_URL}/key/{key_name}/_eek",params=DEK_PARAMS)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"
    
    #decrypt generated EDEK
    eek_response= response.json()[0]

    material = eek_response["encryptedKeyVersion"]["material"]
    name = eek_response["encryptedKeyVersion"]["name"]
    iv = eek_response["iv"]
    version_name = eek_response["versionName"]

    decrypt_payload = {
                  
                    "name":name,
                    "iv": iv,
                    "material": material,
        }
    
    DECRYPT_PARAMS= {"eek_op":"decrypt","user.name":username}
    decrypt_response= requests.post(f"{BASE_URL}/keyversion/{version_name}/_eek",params=DECRYPT_PARAMS,headers=headers,json=decrypt_payload)
    assert decrypt_response.status_code == 200, f"Decryption of EDEK got failed {decrypt_response.status_code}: {decrypt_response.text}"
    
    
    #delete key
    response= requests.delete(f"{BASE_URL}/key/{key_name}",params={"user.name": username})
    assert response.status_code == 200, f"Key deletion failed :{response.text}"



# ****** ********************Test Case 08 ********************************************
# ***** user has no access
# ***********************************************************************************
def test_policy_08(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = "nobody"


    #Update policy for this test
    update_kms_policy(policy_id, username, accesses=None)

    key_name = "pytest-key-08"

    # create key
    response = requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Creation of key, Expected 403 but got {response.text}"

    # Try rollover 
    response = requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Rollover of key, Expected 403 but got {response.status_code}: {response.text}"

    #get current version
    response = requests.get(f"{BASE_URL}/key/{key_name}/_currentversion",params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Get current version, Expected 403 but got: {response.text}"

    # Try getting key metadata
    response = requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Get keyMetaData, Expected 403 but got {response.status_code}: {response.text}"
   
    #generate DEK
    DEK_PARAMS= {"eek_op":"generate","num_keys":1,"user.name":username}
    response = requests.get(f"{BASE_URL}/key/{key_name}/_eek",params=DEK_PARAMS)
    assert response.status_code == 500, f"Generate DEK, Expected 500 but got {response.status_code}: {response.text}"
    

    #delete key
    response= requests.delete(f"{BASE_URL}/key/{key_name}",params={"user.name": username})
    assert response.status_code == 403, f"Delete key, Expected 403 but got :{response.text}"
