import xml.etree.ElementTree as ET
import requests
import pytest
import time
import docker
import tarfile
import io

DBKS_SITE_PATH = "/opt/ranger/ranger-3.0.0-SNAPSHOT-kms/ews/webapp/WEB-INF/classes/conf/dbks-site.xml"
KMS_SERVICE_NAME = "dev_kms"
BASE_URL = "http://localhost:9292/kms/v1"   
RANGER_AUTH = ('keyadmin', 'rangerR0cks!')  # Ranger key admin user
BASE_URL_RANGER = "http://localhost:6080/service/public/v2/api/policy"
HEADERS={"Content-Type": "application/json","Accept":"application/json"}
KMS_CONTAINER_NAME = "ranger-kms"

user1="nobody"

client = docker.from_env()
container = client.containers.get(KMS_CONTAINER_NAME)


# **************** create KMS policy for user1 --------------------------------------
@pytest.fixture(scope="module", autouse=True)
def create_initial_kms_policy():
    policy_data = {
        "policyName": "blacklist-policy",
        "service": KMS_SERVICE_NAME,
        "resources": {
            "keyname": {
                "values": ["blacklist-*"],  # Match any key starting with 'blacklist-'
                "isExcludes": False,
                "isRecursive": False
            }
        },
        "policyItems": [{
            "accesses": [
                {"type": "CREATE", "isAllowed": True},
                {"type": "ROLLOVER", "isAllowed": True},
                {"type": "DELETE", "isAllowed": True}
            ],
            "users": [user1]
        }]
    }

    response = requests.post(BASE_URL_RANGER, auth=RANGER_AUTH, json=policy_data)
    time.sleep(30)  # Wait for policy propagation
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to create policy: {response.text}")
    
    created_policy = response.json()
    policy_id = created_policy["id"]
    yield policy_id

    # Optionally delete policy after tests
    requests.delete(f"{BASE_URL_RANGER}/{policy_id}", auth=RANGER_AUTH)


# ************************** Main Function to add or remove blacklist property--------------------------------
   
def modify_blacklist_property(operation, users, action="add"):
    dbks_site_path = "/opt/ranger/ranger-3.0.0-SNAPSHOT-kms/ews/webapp/WEB-INF/classes/conf/dbks-site.xml"

    # Step 1: Read the current XML content
    result = container.exec_run(f"cat {dbks_site_path}", user='root')
    xml_content = result.output.decode('utf-8')

    # Step 2: Parse and modify
    tree = ET.ElementTree(ET.fromstring(xml_content))
    root = tree.getroot()
    prop_name = f"hadoop.kms.blacklist.{operation}"

    prop = None
    for elem in root.findall("property"):
        name = elem.find("name")
        if name is not None and name.text == prop_name:
            prop = elem
            break

    if prop is None:
        print(f"Property {prop_name} does not exist. Creating it.")
        prop = ET.SubElement(root, "property")
        ET.SubElement(prop, "name").text = prop_name
        ET.SubElement(prop, "value").text = ""

    val_elem = prop.find("value")
    current = val_elem.text.split(",") if val_elem.text else []
    updated = set(current)

    if action == "add":
        updated.update(users)
    elif action == "remove":
        updated -= set(users)

    val_elem.text = ",".join(sorted(updated))

    # Step 3: Convert XML back to string
    modified_xml = ET.tostring(root, encoding='utf-8', method='xml').decode()

    # Step 4: Package XML file into a tarball for `put_archive`
    tarstream = io.BytesIO()
    with tarfile.open(fileobj=tarstream, mode='w') as tar:
        file_data = modified_xml.encode()
        tarinfo = tarfile.TarInfo(name="dbks-site.xml")
        tarinfo.size = len(file_data)
        tar.addfile(tarinfo, io.BytesIO(file_data))
    tarstream.seek(0)

    # Step 5: Upload and replace the file inside the container
    container.put_archive(
        path="/opt/ranger/ranger-3.0.0-SNAPSHOT-kms/ews/webapp/WEB-INF/classes/conf/",
        data=tarstream
    )

    print(f"✅ Successfully {'added' if action == 'add' else 'removed'} {users} in {prop_name}")

#-------------------------------------------------------------------------------------------------------


# Blacklist a user operation
def blacklist_op_users(operation, users=[]):
    modify_blacklist_property(operation, users, action="add")

# Unblacklist a user operation
def unblacklist_op_users(operation, users=[]):
    modify_blacklist_property(operation, users, action="remove")




# ****** ******************** Test Case 01 ********************************************
# ***** check creation, rollover, deletion of key before applying blacklist
# ***** user1 has permission for above operation so will pass
# ***********************************************************************************

def test_user_keyOperation_before_blacklist(headers):
    key_name = "blacklist-key1"
    key_data = {
            "name": key_name
        }
    PARAMS={"user.name":user1}  

    #create key
    create_response = requests.post(f"{BASE_URL}/keys",headers=headers, json=key_data,params=PARAMS)
    assert create_response.status_code == 201, f"key creation failed"
    
    #roll over
    rollover_response = requests.post(f"{BASE_URL}/key/{key_name}", json={}, headers=headers, params=PARAMS)
    assert rollover_response.status_code == 200 , f"roll over failed"
    
    #delete
    delete_response = requests.delete(f"{BASE_URL}/key/{key_name}",params=PARAMS)
    assert delete_response.status_code == 200 , f"deletion of key got failed"



# ****** ******************** Test Case 02 ********************************************
# ***** Test to blacklist a user for CREATE operation
# ***** user1 will be blacklisted from CREATE so cant create keys 
# ***** Then unblacklist that operation and now should succeed
# ***********************************************************************************

def test_blacklist_create(headers):
    # Blacklist the user for CREATE operation
    blacklist_op_users('CREATE', [user1])
    container.restart()
    time.sleep(30)

    key_name = "blacklist-key2"
    key_data = {
            "name": key_name
        }
    PARAMS = {"user.name": user1}  
    response = requests.post(f"{BASE_URL}/keys", headers=headers, json=key_data, params=PARAMS)
    
    # Assert that the user is blocked from creating the key
    assert response.status_code == 403, f"User {user1} should be blocked from creating the key but got succeeded"

    # Remove blacklist after test
    unblacklist_op_users('CREATE', [user1])
    container.restart()
    time.sleep(30)

    # Retry key creation after unblacklisting
    response = requests.post(f"{BASE_URL}/keys", headers=headers, json=key_data, params=PARAMS)
    assert response.status_code == 201, f"User {user1} should be able to create the key after unblacklisting"

    requests.delete(f"{BASE_URL}/key/{key_name}",params=PARAMS)


# ****** ******************** Test Case 03 ********************************************
# ***** Test to blacklist a user for ROLLOVER operation
# ***** user1 will be blacklisted from ROLLOVER so cant roll over keys 
# ***** Then unblacklist that operation and now should succeed
# ***********************************************************************************

def test_blacklist_rollOver(headers):
    # Blacklist the user for rollover operation
    blacklist_op_users('ROLLOVER', [user1])
    container.restart()
    time.sleep(30)

    key_name = "blacklist-key3"
    key_data = {
            "name": key_name
        }
    PARAMS = {"user.name": user1}  

    #create key
    requests.post(f"{BASE_URL}/keys", headers=headers, json=key_data, params=PARAMS)

    #roll over
    response_after_blacklist = requests.post(f"{BASE_URL}/key/{key_name}", json={}, headers=headers, params=PARAMS)
    
    # Assert that the user is blocked from rolling over the key
    assert response_after_blacklist.status_code == 403, f"User {user1} should be blocked from rolling over the key but got succeeded"

    # Remove blacklist after test
    unblacklist_op_users('ROLLOVER', [user1])
    container.restart()
    time.sleep(30)

    # Retry key rollover after unblacklisting
    response_after_unblacklist = requests.post(f"{BASE_URL}/key/{key_name}", headers=headers, json={}, params=PARAMS)
    assert response_after_unblacklist.status_code == 200, f"User {user1} should be able to roll over the key but failed"

    requests.delete(f"{BASE_URL}/key/{key_name}",params=PARAMS)




# ****** ******************** Test Case 04 ********************************************
# ***** Test to blacklist a user for DELETE operation
# ***** user1 will be blacklisted from DELETE so cant delete keys 
# ***** Then unblacklist that operation and now should succeed
# ***********************************************************************************

def test_blacklist_delete(headers):
    # Blacklist the user for rollover operation
    blacklist_op_users('DELETE', [user1])
    container.restart()
    time.sleep(30)

    key_name = "blacklist-key4"
    key_data = {
            "name": key_name
        }
    PARAMS = {"user.name": user1}  
    response = requests.post(f"{BASE_URL}/keys", headers=headers, json=key_data, params=PARAMS)
    
    #try deleting key after blacklisting
    delete_response_before= requests.delete(f"{BASE_URL}/key/{key_name}",params=PARAMS)
    assert delete_response_before.status_code == 403, f"User {user1} should be blocked from deleting the key but got succeeded"

    # Remove blacklist after test
    unblacklist_op_users('DELETE', [user1])
    container.restart()
    time.sleep(30)

    # Retry deletion now
    delete_response_after = requests.delete(f"{BASE_URL}/key/{key_name}",params=PARAMS)
    assert delete_response_after.status_code == 200, f"Deletion of key got failed"

