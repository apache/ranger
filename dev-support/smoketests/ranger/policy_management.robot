*** Settings ***
Library        policy_management.TestPolicyManagement               http://localhost:6080    admin    rangerR0cks!
Library        policy_management.TestServiceManagement              http://localhost:6080    admin    rangerR0cks!
Library        Collections
Library        JSONLibrary

*** Variables ***
${HIVE_DEFAULT_SVC}        dev_hive
${HIVE_DEFAULT_POLICY}     all - global
${HIVE_TEST_SVC}           test_hive
${HIVE_TEST_POLICY}        test_policy
@{TEST_DB}                 test_db

*** Test Cases ***
Create Hive Test Service
    [Documentation]    Create a test Hive service with specific configurations.
    ${configs}         Create Dictionary    username=hive    password=hive    jdbc.driverClassName=org.apache.hive.jdbc.HiveDriver    jdbc.url=jdbc:hive2://ranger-hadoop:10000    hadoop.security.authorization=${true}
    ${response}        Create Service       test_hive        hive             ${configs}
    Log                ${response}


Delete Hive Test Service
    [Documentation]    Delete the test Hive service.
    ${response}        Delete Service    ${HIVE_TEST_SVC}


Get All Policies
    [Documentation]    Get all policies in Ranger.
    ${response}        Get All Policies
    ${service}         Get Value From Json    ${response}    $[0].service
    Should Be Equal    ${service}[0]          dev_hdfs


Validate Response - Get Default Hive Policy
    [Documentation]    Validate Response from the default hive policy: all - global
    ${response}        Get Hive Policy        ${HIVE_DEFAULT_SVC}   ${HIVE_DEFAULT_POLICY}

    ${service}         Get Value From Json    ${response}    $.service
    ${name}            Get Value From Json    ${response}    $.name
    ${isEnabled}       Get Value From Json    ${response}    $.isEnabled
    ${users}           Get Value From Json    ${response}    $.policyItems[0].users

    Should Be Equal              ${service}[0]    ${HIVE_DEFAULT_SVC}
    Should Be Equal              ${name}[0]       ${HIVE_DEFAULT_POLICY}
    Should Be True               ${isEnabled}
    List Should Contain Value    ${users}[0]      hive
    Log    ${response}


Create Hive Test Policy
    [Documentation]         Create a test policy in Default Hive Service
    ${response}             Create Hive Policy    dev_hive    test_policy_78     test_db    test_table

    ${id}                   Get Value From Json    ${response}    $.id
    Set Suite Variable      ${POLICY_ID}    ${id}
    Log                     ${response}


Delete Hive Test Policy
    ${response}     Delete Hive Policy       dev_hive    test_policy_78
    Log             ${response}

# Depends on an earlier Test
Validate Successive Hive Test Policy
    [Documentation]     Create the test policy again in Default Hive Service
    ${response}         Create Hive Policy    dev_hive    test_policy_78     test_db    test_table

    ${id}               Get Value From Json    ${response}    $.id
    ${result}           Evaluate    ${POLICY_ID}[0] + 1
    Should Be Equal     ${id}[0]    ${result}
    Log                 ${response}


Delete Successive Hive Test Policy
    ${response}     Delete Hive Policy          dev_hive    test_policy_78
    Log             ${response}


Create 100 Policies
    [Documentation]         Creates 100 test policies in dev_hive service
    Create Policies In Bulk     dev_hive    100


Delete 100 Policies
    [Documentation]         Deletes 100 test policies in dev_hive service
    Delete Policies In Bulk     dev_hive    100

