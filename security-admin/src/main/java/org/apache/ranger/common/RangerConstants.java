/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 *
 */

package org.apache.ranger.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RangerConstants extends RangerCommonEnums {
    // Default Roles
    public static final String ROLE_SYS_ADMIN         = "ROLE_SYS_ADMIN";
    public static final String ROLE_ADMIN             = "ROLE_ADMIN";
    public static final String ROLE_INTEGRATOR        = "ROLE_INTEGRATOR";
    public static final String ROLE_DATA_ANALYST      = "ROLE_DATA_ANALYST";
    public static final String ROLE_BIZ_MGR           = "ROLE_BIZ_MGR";
    public static final String ROLE_KEY_ADMIN         = "ROLE_KEY_ADMIN";
    public static final String ROLE_USER              = "ROLE_USER";
    public static final String ROLE_ANON              = "ROLE_ANON";
    public static final String ROLE_OTHER             = "ROLE_OTHER";
    public static final String GROUP_PUBLIC           = "public";
    public static final String ROLE_ADMIN_AUDITOR     = "ROLE_ADMIN_AUDITOR";
    public static final String ROLE_KEY_ADMIN_AUDITOR = "ROLE_KEY_ADMIN_AUDITOR";
    public static final String ROLE_FIELD             = "Roles";

    // Action constants
    public static final String ACTION_EDIT            = "edit";
    public static final String ACTION_CHANGE          = "change";
    public static final String ACTION_DELETE          = "delete";
    public static final String ACTION_MARK_SPAM       = "mark_spam";
    public static final String ACTION_RATE            = "rate";
    public static final String ACTION_SELECT          = "select";
    public static final String ACTION_UNSELECT        = "unselect";
    public static final String ACTION_HIDE            = "hide";
    public static final String ACTION_UNHIDE          = "unhide";
    public static final String ACTION_SHARE           = "share";
    public static final String ACTION_UNSHARE         = "unshare";
    public static final String ACTION_BOOKMARK        = "bookmark";
    public static final String ACTION_UNBOOKMARK      = "unbookmark";

    // Sendgrid email API constants
    public static final String SENDGRID_API_USER   = "api_user";
    public static final String SENDGRID_API_KEY    = "api_key";
    public static final String SENDGRID_TO         = "to";
    public static final String SENDGRID_TO_NAME    = "toname";
    public static final String SENDGRID_SUBJECT    = "subject";
    public static final String SENDGRID_TEXT       = "text";
    public static final String SENDGRID_HTML       = "html";
    public static final String SENDGRID_FROM_EMAIL = "from";
    public static final String SENDGRID_FROM_NAME  = "fromname";
    public static final String SENDGRID_BCC        = "bcc";
    public static final String SENDGRID_CC         = "cc";
    public static final String SENDGRID_REPLY_TO   = "replyto";

    //Permission Names
    public static final String MODULE_RESOURCE_BASED_POLICIES = "Resource Based Policies";
    public static final String MODULE_USER_GROUPS             = "Users/Groups";
    public static final String MODULE_REPORTS                 = "Reports";
    public static final String MODULE_AUDIT                   = "Audit";
    public static final String MODULE_PERMISSION              = "Permissions";
    public static final String MODULE_KEY_MANAGER             = "Key Manager";
    public static final String MODULE_TAG_BASED_POLICIES      = "Tag Based Policies";
    public static final String MODULE_SECURITY_ZONE           = "Security Zone";
    public static final String MODULE_GOVERNED_DATA_SHARING   = "Governed Data Sharing";
    public static final String USER_PENDING_APPROVAL_MSG      = "User is yet not reviewed by Administrator. Please contact at <number>.";

    // these constants will be used in setting GjResponse object.
    public static final int    USER_PENDING_APPROVAL_STATUS_CODE       = 0;
    public static final String USER_APPROVAL_MSG                       = "User is approved";
    public static final int    USER_APPROVAL_STATUS_CODE               = 1;
    public static final String USER_REJECTION_MSG                      = "User is rejected";
    public static final int    USER_REJECTION_STATUS_CODE              = 1;
    public static final String USER_STATUS_ALREADY_CHANGED_MSG         = "Can not change user status. it is either already activated/approved/rejected";
    public static final int    USER_STATUS_ALREADY_CHANGED_STATUS_CODE = 0;
    public static final String USER_ALREADY_ACTIVATED_MSG              = "Your account is already activated. If you have forgotten your password, then from the login page, select 'Forgot Password'";
    public static final int    USER_ALREADY_ACTIVATED_STATUS_CODE      = 0;
    public static final String USER_STATUS_NOT_ACTIVE_MSG              = "User is not in active status. Please activate your account first.";
    public static final int    USER_STATUS_NOT_ACTIVE_STATUS_CODE      = 0;
    public static final String INVALID_EMAIL_ADDRESS_MSG               = "Invalid email address";
    public static final int    INVALID_EMAIL_ADDRESS_STATUS_CODE       = 0;
    public static final String WRONG_ACTIVATION_CODE_MSG               = "Wrong activation code";
    public static final int    WRONG_ACTIVATION_CODE_STATUS_CODE       = 0;
    public static final String VALID_EMAIL_ADDRESS_MSG                 = "Valid email address";
    public static final int    VALID_EMAIL_ADDRESS_STATUS_CODE         = 1;
    public static final String NO_ACTIVATION_RECORD_FOR_USER_ERR_MSG   = "No activation record found for user:";
    public static final String NO_ACTIVATION_ENTRY                     = "activation entry not found";
    public static final String VALIDATION_INVALID_DATA_DESC            = "Invalid value for";
    public static final int    VALIDATION_INVALID_DATA_CODE            = 0;
    public static final String GROUP_MODERATORS                        = "GROUP_MODERATORS";
    public static final String PWD_RESET_FAILED_MSG                    = "Invalid password reset request";

    // public static final String EMAIL_WELCOME_MSG =
    // "Welcome to iSchoolCircle";
    // public static final String EMAIL_LINK_WELCOME_MSG =
    // "Welcome to iSchoolCircle ! Please verify your account by clicking on the link below: ";
    // public static final String EMAIL_EDIT_REJECTED_MSG =
    // "Your changes not approved for public sharing.";
    // public static final String EMAIL_APPROVAL_NEEDED_MSG =
    // "New objects pending approval";
    // public static final String EMAIL_PWD_RESET_CODE_MSG =
    public static final String INVALID_NEW_PASSWORD_MSG     = "Invalid new password";
    public static final String EMAIL_NEW_FEEDBACK_RECEIVED  = "New feedback from";
    public static final int    INITIAL_DOCUMENT_VERSION     = 1;
    public static final int    EMAIL_TYPE_ACCOUNT_CREATE    = 0;
    public static final int    EMAIL_TYPE_USER_CREATE       = 1;
    public static final int    EMAIL_TYPE_USER_ACCT_ADD     = 2;
    public static final int    EMAIL_TYPE_DOCUMENT_CREATE   = 3;
    public static final int    EMAIL_TYPE_DISCUSSION_CREATE = 4;
    public static final int    EMAIL_TYPE_NOTE_CREATE       = 5;
    public static final int    EMAIL_TYPE_TASK_CREATE       = 6;
    public static final int    EMAIL_TYPE_USER_PASSWORD     = 7;
    public static final int    EMAIL_TYPE_USER_ACTIVATION   = 8;
    public static final int    EMAIL_TYPE_USER_ROLE_UPDATED = 9;
    public static final int    EMAIL_TYPE_USER_GRP_ADD      = 10;

    //Constant for Tag_Service Type.
    public static final int TAG_SERVICE_TYPE = 100;

    public static final List<String> VALID_USER_ROLE_LIST = new ArrayList<>(Arrays.asList(RangerConstants.ROLE_USER,
            RangerConstants.ROLE_SYS_ADMIN, RangerConstants.ROLE_KEY_ADMIN, RangerConstants.ROLE_ADMIN_AUDITOR,
            RangerConstants.ROLE_KEY_ADMIN_AUDITOR));

    public static final String DEFAULT_SORT_ORDER = "asc";

    //HTTP STATUS code for authentication timeout
    public static final int SC_AUTHENTICATION_TIMEOUT = 419;

    // User create validation errors
    public enum ValidationUserProfile {
        NO_EMAIL_ADDR("xa.validation.userprofile.no_email_addr", "Email address not provided"),
        INVALID_EMAIL_ADDR("xa.validation.userprofile.userprofile.invalid_email_addr", "Invalid email address"),
        NO_FIRST_NAME("xa.validation.userprofile.userprofile.no_first_name", "First name not provided"),
        INVALID_FIRST_NAME("xa.validation.userprofile.invalid_first_name", "Invalid first name"),
        NO_LAST_NAME("xa.validation.userprofile.noemailaddr", "Email address not provided"),
        INVALID_LAST_NAME("xa.validation.userprofile.noemailaddr", "Email address not provided"),
        NO_PUBLIC_SCREEN_NAME("xa.validation.userprofile.noemailaddr", "Email address not provided"),
        INVALID_PUBLIC_SCREEN_NAME("xa.validation.userprofile.noemailaddr", "Email address not provided");

        final String rbKey;
        final String message;

        ValidationUserProfile(String rbKey, String message) {
            this.rbKey   = rbKey;
            this.message = message;
        }
    }

    public enum RBAC_PERM {
        ALLOW_NONE,
        ALLOW_READ,
        ALLOW_WRITE,
        ALLOW_DELETE
    }
}
