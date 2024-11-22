*** Settings ***
Library        policy_management.TestPolicyManagement       http://localhost:6080    admin           rangerR0cks!      WITH NAME   admin_p
Library        policy_management.TestPolicyManagement       http://localhost:6080    test_user_1     Welcome1          WITH NAME   user_t
Library        policy_management.TestPolicyManagement       http://localhost:6080    finance_user    Welcome1          WITH NAME   user_f
Library        Collections
Library        JSONLibrary

*** Variables ***


*** Test Cases ***
Admin User Succeeds To Create Policy Regular User Fails
    [Documentation]         A regular user fails to create hive policy whereas an admin user succeeds.
    ${response}             admin_p.Create Hive Policy    dev_hive    test_policy_custom_1     test_db_custom_1     test_table_custom_1
    Log                     ${response}
    Run Keyword And Expect Error    RangerServiceException*       user_t.Create Hive Policy     dev_hive    test_policy_custom_2     test_db_custom_2   test_table_custom_2


Regular User With Delegate-Admin Succeeds To Delete Policy Where Regular User Fails
    [Documentation]         A regular user with delegated-admin succeeds to delete hive policy whereas a regular user w/o delegated-admin fails
    Run Keyword And Expect Error    RangerServiceException*       user_f.Delete Hive Policy    dev_hive    test_policy_custom_1
    ${response}             user_t.Delete Hive Policy    dev_hive    test_policy_custom_1
    Log                     ${response}