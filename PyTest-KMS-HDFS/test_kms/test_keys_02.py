import requests     
import pytest  
from utils import fetch_logs     

BASE_URL = "http://localhost:9292/kms/v1"  
PARAMS={"user.name":"keyadmin"}   

# ***********************************************************************************
# Test to check after key roll over ->  new version= old version+1
# ***********************************************************************************
def test_versionIncrement_after_rollover(headers):
    
    key_name="key_roll"
    key_data={
        "name":key_name
    }
    #create key
    response=requests.post(f"{BASE_URL}/keys",json=key_data,params=PARAMS,headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    #check version before roll over
    response_before= requests.get(f"{BASE_URL}/key/{key_name}/_currentversion", headers=headers, params=PARAMS)
    assert response_before.status_code == 200, f"Failed to get current version. Response: {response_before.text}"
    
       #extract version number
    version_before = response_before.json().get("versionName")  # e.g "test-key@0"
    version_num_before = int(version_before.split("@")[1])
    print(f"version before: {version_num_before}" )

    #roll over
    response = requests.post(f"{BASE_URL}/key/{key_name}", json={}, headers=headers, params=PARAMS)
    assert response.status_code==200, f"failed to perform roll over . Response:{response.text}"

    #check version after roll over
    response_after= requests.get(f"{BASE_URL}/key/{key_name}/_currentversion", headers=headers, params=PARAMS)
    assert response_after.status_code == 200, f"Failed to get current version. Response: {response_after.text}"
    
       #extract new version number
    version_after = response_after.json().get("versionName") 
    version_num_after = int(version_after.split("@")[1])
    print(f"version after: {version_num_after}")

    assert version_num_after == version_num_before + 1 , (
        f"Expected version to increment. Before: {version_before}, After: {version_after}"
    )
    
    # Cleanup key after test
    requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)


# ***********************************************************************************
# Test to check if material which is used to create key matches material from get key material
# ***********************************************************************************
def test_key_material(headers):

    key_name="test-key"
    key_material="G90ZtTKOWIICXG_wpqx0tA"

    key_data={
        "name":key_name,
        "material":key_material
    }
    
    #create key
    response=requests.post(f"{BASE_URL}/keys",json=key_data,params=PARAMS,headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"
    
    #check material from currentversion
    version_response= requests.get(f"{BASE_URL}/key/{key_name}/_currentversion", headers=headers, params=PARAMS)
    assert version_response.status_code == 200, f"Failed to get current version. Response: {version_response.text}"
    
    response_keyMaterial= version_response.json()
    response_keyMaterial=response_keyMaterial["material"]

    assert key_material== response_keyMaterial, f"Key material not matching. Passed key material: {key_material}, Got Key material: {response_keyMaterial}"
    
    # Cleanup key after test
    requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)



# ***********************************************************************************
# Tests key is not present after deletion
# ***********************************************************************************
def test_deleted_key_not_in_list(headers):

    key_name="Delete-key"

    key_data={
        "name":key_name,
    }
    
    #create key
    response=requests.post(f"{BASE_URL}/keys",json=key_data,params=PARAMS,headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"
    
    # Delete key
    requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)

    list_response= requests.get(f"{BASE_URL}/keys/names",params=PARAMS)

    key_list= list_response.json()

    assert key_name not in key_list, f"Deleted key still exists, Deletion might have failed"



# ***********************************************************************************
# Test to check key operations in bulk 
# ***********************************************************************************
def test_bulk_key_operation(headers):

    key_names = [f"key{i}" for i in range(5)]
    created_keys = []

    # Create 5 EZ keys
    for name in key_names:
        key_data = {
            "name": name,
        }

        response = requests.post(f"{BASE_URL}/keys", json=key_data, params=PARAMS, headers=headers)
        assert response.status_code == 201, f"Failed to create key {name}: {response.text}"
        created_keys.append(name)

    # Get all keys and verify they exist
    list_response = requests.get(f"{BASE_URL}/keys/names", headers=headers, params=PARAMS)
    assert list_response.status_code == 200, f"Fetching key list failed: {list_response.text}"

    all_keys = list_response.json()
  
    for name in created_keys:
        assert name in all_keys, f"Key '{name}' not found in key list."

    # Get metadata for each key
    for name in created_keys:
        meta_response = requests.get(f"{BASE_URL}/key/{name}", headers=headers, params=PARAMS)
        assert meta_response.status_code == 200, f"Failed to get metadata for key {name}"

    # Delete all 5 keys
    for name in created_keys:
        del_response = requests.delete(f"{BASE_URL}/key/{name}", params=PARAMS)
        assert del_response.status_code==200, f"Failed to delete key {name}: {del_response.text}"

    # Verify keys are deleted
    final_list_response = requests.get(f"{BASE_URL}/keys/names", headers=headers, params=PARAMS)
    assert final_list_response.status_code == 200, f"Fetching key list after deletion failed"
    final_keys = final_list_response.json()

    for name in created_keys:
        assert name not in final_keys, f"Deleted key '{name}' still found in key list"
