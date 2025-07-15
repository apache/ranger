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

import React from "react";

export const UserRoles = {
  ROLE_SYS_ADMIN: {
    value: 0,
    label: "Admin"
  },
  ROLE_USER: {
    value: 1,
    label: "User"
  },
  ROLE_KEY_ADMIN: {
    value: 2,
    label: "KeyAdmin"
  },
  ROLE_ADMIN_AUDITOR: {
    value: 3,
    label: "Auditor"
  },
  ROLE_KEY_ADMIN_AUDITOR: {
    value: 4,
    label: "KMSAuditor"
  }
};

export const UserSource = {
  XA_PORTAL_USER: {
    value: 0,
    label: "Allowed",
    rbkey: "xa.enum.AccessResult.ACCESS_RESULT_ALLOWED",
    tt: "lbl.AccessResult_ACCESS_RESULT_ALLOWED"
  },
  XA_USER: {
    value: 1,
    label: "Denied",
    rbkey: "xa.enum.AccessResult.ACCESS_RESULT_DENIED",
    tt: "lbl.AccessResult_ACCESS_RESULT_DENIED"
  }
};

export const UserTypes = {
  USER_INTERNAL: {
    value: 0,
    label: "Internal",
    variant: "success"
  },
  USER_EXTERNAL: {
    value: 1,
    label: "External",
    variant: "warning"
  },
  USER_FEDERATED: {
    value: 6,
    label: "Federated",
    variant: "secondary"
  }
};

export const UserSyncSource = {
  USER_SYNC_UNIX: {
    value: 0,
    label: "Unix",
    rbkey: "xa.enum.UserSyncSource.USER_SYNC_UNIX",
    tt: "lbl.USER_SYNC_UNIX"
  },
  USER_SYNC_LDAPAD: {
    value: 1,
    label: "LDAP/AD",
    rbkey: "xa.enum.UserSyncSource.USER_SYNC_LDAPAD",
    tt: "lbl.USER_SYNC_LDAPAD"
  },
  USER_SYNC_FILE: {
    value: 2,
    label: "File",
    rbkey: "xa.enum.UserSyncSource.USER_SYNC_FILE",
    tt: "lbl.USER_SYNC_FILE"
  }
};

export const VisibilityStatus = {
  STATUS_HIDDEN: {
    value: 0,
    label: "Hidden",
    rbkey: "xa.enum.VisibilityStatus.IS_HIDDEN",
    tt: "lbl.VisibilityStatus_IS_HIDDEN"
  },
  STATUS_VISIBLE: {
    value: 1,
    label: "Visible",
    rbkey: "xa.enum.VisibilityStatus.IS_VISIBLE",
    tt: "lbl.VisibilityStatus_IS_VISIBLE"
  }
};

/* For Group */
export const GroupSource = {
  XA_PORTAL_GROUP: {
    value: 0,
    label: "Allowed",
    rbkey: "xa.enum.AccessResult.ACCESS_RESULT_ALLOWED",
    tt: "lbl.AccessResult_ACCESS_RESULT_ALLOWED"
  },
  XA_GROUP: {
    value: 1,
    label: "Denied",
    rbkey: "xa.enum.AccessResult.ACCESS_RESULT_DENIED",
    tt: "lbl.AccessResult_ACCESS_RESULT_DENIED"
  }
};

export const GroupTypes = {
  GROUP_INTERNAL: {
    value: 0,
    label: "Internal",
    rbkey: "xa.enum.AccessResult.ACCESS_RESULT_ALLOWED",
    tt: "lbl.AccessResult_ACCESS_RESULT_ALLOWED"
  },
  GROUP_EXTERNAL: {
    value: 1,
    label: "External",
    rbkey: "xa.enum.AccessResult.ACCESS_RESULT_DENIED",
    tt: "lbl.AccessResult_ACCESS_RESULT_DENIED"
  }
};

/* Audit Admin */
export const ClassTypes = {
  CLASS_TYPE_NONE: {
    value: 0,
    label: "None",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_NONE",
    tt: "lbl.ClassTypes_CLASS_TYPE_NONE"
  },
  CLASS_TYPE_MESSAGE: {
    value: 1,
    label: "Message",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_MESSAGE",
    modelName: "VXMessage",
    type: "vXMessage",
    tt: "lbl.ClassTypes_CLASS_TYPE_MESSAGE"
  },
  CLASS_TYPE_USER_PROFILE: {
    value: 2,
    label: "User Profile",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_USER_PROFILE",
    modelName: "VXPortalUser",
    type: "vXPortalUser",
    tt: "lbl.ClassTypes_CLASS_TYPE_USER_PROFILE"
  },
  CLASS_TYPE_AUTH_SESS: {
    value: 3,
    label: "Authentication Session",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_AUTH_SESS",
    modelName: "VXAuthSession",
    type: "vXAuthSession",
    tt: "lbl.ClassTypes_CLASS_TYPE_AUTH_SESS"
  },
  CLASS_TYPE_DATA_OBJECT: {
    value: 4,
    label: "CLASS_TYPE_DATA_OBJECT",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_DATA_OBJECT",
    modelName: "VXDataObject",
    type: "vXDataObject",
    tt: "lbl.ClassTypes_CLASS_TYPE_DATA_OBJECT"
  },
  CLASS_TYPE_NAMEVALUE: {
    value: 5,
    label: "CLASS_TYPE_NAMEVALUE",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_NAMEVALUE",
    tt: "lbl.ClassTypes_CLASS_TYPE_NAMEVALUE"
  },
  CLASS_TYPE_LONG: {
    value: 6,
    label: "CLASS_TYPE_LONG",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_LONG",
    modelName: "VXLong",
    type: "vXLong",
    tt: "lbl.ClassTypes_CLASS_TYPE_LONG"
  },
  CLASS_TYPE_PASSWORD_CHANGE: {
    value: 7,
    label: "Password Change",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_PASSWORD_CHANGE",
    modelName: "VXPasswordChange",
    type: "vXPasswordChange",
    tt: "lbl.ClassTypes_CLASS_TYPE_PASSWORD_CHANGE"
  },
  CLASS_TYPE_STRING: {
    value: 8,
    label: "CLASS_TYPE_STRING",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_STRING",
    modelName: "VXString",
    type: "vXString",
    tt: "lbl.ClassTypes_CLASS_TYPE_STRING"
  },
  CLASS_TYPE_ENUM: {
    value: 9,
    label: "CLASS_TYPE_ENUM",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_ENUM",
    tt: "lbl.ClassTypes_CLASS_TYPE_ENUM"
  },
  CLASS_TYPE_ENUM_ELEMENT: {
    value: 10,
    label: "CLASS_TYPE_ENUM_ELEMENT",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_ENUM_ELEMENT",
    tt: "lbl.ClassTypes_CLASS_TYPE_ENUM_ELEMENT"
  },
  CLASS_TYPE_RESPONSE: {
    value: 11,
    label: "Response",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_RESPONSE",
    modelName: "VXResponse",
    type: "vXResponse",
    tt: "lbl.ClassTypes_CLASS_TYPE_RESPONSE"
  },
  CLASS_TYPE_XA_ASSET: {
    value: 1000,
    label: "Asset",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_XA_ASSET",
    modelName: "VXAsset",
    type: "vXAsset",
    tt: "lbl.ClassTypes_CLASS_TYPE_XA_ASSET"
  },
  CLASS_TYPE_XA_RESOURCE: {
    value: 1001,
    label: "Resource",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_XA_RESOURCE",
    modelName: "VXResource",
    type: "vXResource",
    tt: "lbl.ClassTypes_CLASS_TYPE_XA_RESOURCE"
  },
  CLASS_TYPE_XA_GROUP: {
    value: 1002,
    label: "Ranger Group",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_XA_GROUP",
    modelName: "VXGroup",
    type: "vXGroup",
    tt: "lbl.ClassTypes_CLASS_TYPE_XA_GROUP"
  },
  CLASS_TYPE_XA_USER: {
    value: 1003,
    label: "Ranger User",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_XA_USER",
    modelName: "VXUser",
    type: "vXUser",
    tt: "lbl.ClassTypes_CLASS_TYPE_XA_USER"
  },
  CLASS_TYPE_XA_GROUP_USER: {
    value: 1004,
    label: "XA Group of Users",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_XA_GROUP_USER",
    modelName: "VXGroupUser",
    type: "vXGroupUser",
    tt: "lbl.ClassTypes_CLASS_TYPE_XA_GROUP_USER"
  },
  CLASS_TYPE_XA_GROUP_GROUP: {
    value: 1005,
    label: "XA Group of groups",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_XA_GROUP_GROUP",
    modelName: "VXGroupGroup",
    type: "vXGroupGroup",
    tt: "lbl.ClassTypes_CLASS_TYPE_XA_GROUP_GROUP"
  },
  CLASS_TYPE_XA_PERM_MAP: {
    value: 1006,
    label: "XA permissions for resource",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_XA_PERM_MAP",
    modelName: "VXPermMap",
    type: "vXPermMap",
    tt: "lbl.ClassTypes_CLASS_TYPE_XA_PERM_MAP"
  },
  CLASS_TYPE_XA_AUDIT_MAP: {
    value: 1007,
    label: "XA audits for resource",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_XA_AUDIT_MAP",
    modelName: "VXAuditMap",
    type: "vXAuditMap",
    tt: "lbl.ClassTypes_CLASS_TYPE_XA_AUDIT_MAP"
  },
  CLASS_TYPE_XA_CRED_STORE: {
    value: 1008,
    label: "XA credential store",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_XA_CRED_STORE",
    modelName: "VXCredentialStore",
    type: "vXCredentialStore",
    tt: "lbl.ClassTypes_CLASS_TYPE_XA_CRED_STORE"
  },
  CLASS_TYPE_XA_POLICY_EXPORT_AUDIT: {
    value: 1009,
    label: "XA Policy Export Audit",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_XA_POLICY_EXPORT_AUDIT",
    modelName: "VXPolicyExportAudit",
    type: "vXPolicyExportAudit",
    tt: "lbl.ClassTypes_CLASS_TYPE_XA_POLICY_EXPORT_AUDIT"
  },
  CLASS_TYPE_TRX_LOG: {
    value: 1010,
    label: "Transaction log",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_TRX_LOG",
    tt: "lbl.ClassTypes_CLASS_TYPE_TRX_LOG"
  },
  CLASS_TYPE_XA_ACCESS_AUDIT: {
    value: 1011,
    label: "Access Audit",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_XA_ACCESS_AUDIT",
    modelName: "VXAccessAudit",
    type: "vXAccessAudit",
    tt: "lbl.ClassTypes_CLASS_TYPE_XA_ACCESS_AUDIT"
  },
  CLASS_TYPE_XA_TRANSACTION_LOG_ATTRIBUTE: {
    value: 1012,
    label: "Transaction log attribute",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_XA_TRANSACTION_LOG_ATTRIBUTE",
    tt: "lbl.ClassTypes_CLASS_TYPE_XA_TRANSACTION_LOG_ATTRIBUTE"
  },
  CLASS_TYPE_RANGER_POLICY: {
    value: 1020,
    label: "Ranger Policy",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_RANGER_POLICY",
    modelName: "VXRangerPolicy",
    type: "vXResource",
    tt: "lbl.ClassTypes_CLASS_TYPE_RANGER_POLICY"
  },
  CLASS_TYPE_RANGER_SERVICE: {
    value: 1030,
    label: "Ranger Service",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_RANGER_SERVICE",
    modelName: "VXRangerService",
    type: "vXRangerService",
    tt: "lbl.ClassTypes_CLASS_TYPE_RANGER_SERVICE"
  },
  CLASS_TYPE_RANGER_SECURITY_ZONE: {
    value: 1056,
    label: "Ranger Security Zone",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_RANGER_SECURITY_ZONE",
    modelName: "VXRangerService",
    type: "vXRangerService",
    tt: "lbl.ClassTypes_CLASS_TYPE_RANGER_SECURITY_ZONE"
  },
  CLASS_TYPE_RANGER_ROLE: {
    value: 1057,
    label: "Ranger Role",
    rbkey: "xa.enum.ClassTypes.CLASS_TYPE_RANGER_ROLE",
    modelName: "VXRole",
    type: "vXRole",
    tt: "lbl.ClassTypes_CLASS_TYPE_RANGER_ROLE"
  }
};

/* Audit  LoginSession */
export const AuthStatus = {
  AUTH_STATUS_UNKNOWN: {
    value: 0,
    label: "Unknown",
    rbkey: "xa.enum.AuthStatus.AUTH_STATUS_UNKNOWN",
    tt: "lbl.AuthStatus_AUTH_STATUS_UNKNOWN"
  },
  AUTH_STATUS_SUCCESS: {
    value: 1,
    label: "Success",
    rbkey: "xa.enum.AuthStatus.AUTH_STATUS_SUCCESS",
    tt: "lbl.AuthStatus_AUTH_STATUS_SUCCESS"
  },
  AUTH_STATUS_WRONG_PASSWORD: {
    value: 2,
    label: "Wrong Password",
    rbkey: "xa.enum.AuthStatus.AUTH_STATUS_WRONG_PASSWORD",
    tt: "lbl.AuthStatus_AUTH_STATUS_WRONG_PASSWORD"
  },
  AUTH_STATUS_DISABLED: {
    value: 3,
    label: "Account Disabled",
    rbkey: "xa.enum.AuthStatus.AUTH_STATUS_DISABLED",
    tt: "lbl.AuthStatus_AUTH_STATUS_DISABLED"
  },
  AUTH_STATUS_LOCKED: {
    value: 4,
    label: "Locked",
    rbkey: "xa.enum.AuthStatus.AUTH_STATUS_LOCKED",
    tt: "lbl.AuthStatus_AUTH_STATUS_LOCKED"
  },
  AUTH_STATUS_PASSWORD_EXPIRED: {
    value: 5,
    label: "Password Expired",
    rbkey: "xa.enum.AuthStatus.AUTH_STATUS_PASSWORD_EXPIRED",
    tt: "lbl.AuthStatus_AUTH_STATUS_PASSWORD_EXPIRED"
  },
  AUTH_STATUS_USER_NOT_FOUND: {
    value: 6,
    label: "User not found",
    rbkey: "xa.enum.AuthStatus.AUTH_STATUS_USER_NOT_FOUND",
    tt: "lbl.AuthStatus_AUTH_STATUS_USER_NOT_FOUND"
  }
};

export const AuthType = {
  AUTH_TYPE_UNKNOWN: {
    value: 0,
    label: "Unknown",
    rbkey: "xa.enum.AuthType.AUTH_TYPE_UNKNOWN",
    tt: "lbl.AuthType_AUTH_TYPE_UNKNOWN"
  },
  AUTH_TYPE_PASSWORD: {
    value: 1,
    label: "Username/Password",
    rbkey: "xa.enum.AuthType.AUTH_TYPE_PASSWORD",
    tt: "lbl.AuthType_AUTH_TYPE_PASSWORD"
  },
  AUTH_TYPE_KERBEROS: {
    value: 2,
    label: "Kerberos",
    rbkey: "xa.enum.AuthType.AUTH_TYPE_KERBEROS",
    tt: "lbl.AuthType_AUTH_TYPE_KERBEROS"
  },
  AUTH_TYPE_SSO: {
    value: 3,
    label: "SingleSignOn",
    rbkey: "xa.enum.AuthType.AUTH_TYPE_SSO",
    tt: "lbl.AuthType_AUTH_TYPE_SSO"
  },
  AUTH_TYPE_TRUSTED_PROXY: {
    value: 4,
    label: "Trusted Proxy",
    rbkey: "xa.enum.AuthType.AUTH_TYPE_TRUSTED_PROXY",
    tt: "lbl.AuthType_AUTH_TYPE_TRUSTED_PROXY"
  }
};

export const ActivationStatus = {
  ACT_STATUS_DISABLED: {
    value: 0,
    label: "Disabled",
    rbkey: "xa.enum.ActivationStatus.ACT_STATUS_DISABLED",
    tt: "lbl.ActivationStatus_ACT_STATUS_DISABLED"
  },
  ACT_STATUS_ACTIVE: {
    value: 1,
    label: "Active",
    rbkey: "xa.enum.ActivationStatus.ACT_STATUS_ACTIVE",
    tt: "lbl.ActivationStatus_ACT_STATUS_ACTIVE"
  }
};

/*Permission Edit Page */
export const AccessResult = {
  ACCESS_RESULT_DENIED: {
    value: 0,
    label: "Denied",
    rbkey: "xa.enum.AccessResult.ACCESS_RESULT_DENIED",
    tt: "lbl.AccessResult_ACCESS_RESULT_DENIED",
    auditFilterLabel: "DENIED"
  },
  ACCESS_RESULT_ALLOWED: {
    value: 1,
    label: "Allowed",
    rbkey: "xa.enum.AccessResult.ACCESS_RESULT_ALLOWED",
    tt: "lbl.AccessResult_ACCESS_RESULT_ALLOWED",
    auditFilterLabel: "ALLOWED"
  },
  ACCESS_RESULT_NOT_DETERMINED: {
    value: 2,
    label: "Not Determined",
    rbkey: "xa.enum.AccessResult.ACCESS_RESULT_NOT_DETERMINED",
    tt: "lbl.AccessResult_ACCESS_RESULT_NOT_DETERMINED",
    auditFilterLabel: "NOT_DETERMINED"
  }
};

export const RangerPolicyType = {
  RANGER_ACCESS_POLICY_TYPE: {
    value: 0,
    label: "Access",
    rbkey: "xa.enum.AccessResult.ACCESS_RESULT_ALLOWED",
    tt: "lbl.AccessResult_ACCESS_RESULT_ALLOWED"
  },
  RANGER_MASKING_POLICY_TYPE: {
    value: 1,
    label: "Masking",
    rbkey: "xa.enum.AccessResult.ACCESS_RESULT_DENIED",
    tt: "lbl.AccessResult_ACCESS_RESULT_DENIED"
  },
  RANGER_ROW_FILTER_POLICY_TYPE: {
    value: 2,
    label: "Row Level Filter",
    rbkey: "xa.enum.AccessResult.ACCESS_RESULT_DENIED",
    tt: "lbl.AccessResult_ACCESS_RESULT_DENIED"
  }
};

export const getEnumElementByValue = (enumObj, value) => {
  let obj;
  for (const key in enumObj) {
    if (enumObj[key].value === value) {
      obj = enumObj[key];
      break;
    }
  }
  return obj;
};

export const enumValueToLabel = (myEnum, value) => {
  var element;
  for (const key in myEnum) {
    if (myEnum[key].value === value) {
      element = myEnum[key];
    }
  }
  return element;
};

export const RegexValidation = {
  PASSWORD: {
    regexExpression: /^.*(?=.{8,256})(?=.*[0-9])(?=.*[a-z])(?=.*[A-Z]).*$/,
    message:
      "Password should be minimum 8 characters, atleast one uppercase letter, one lowercase letter and one numeric. For FIPS environment password should be minimum 14 characters with atleast one uppercase letter, one special characters, one lowercase letter and one numeric."
  },
  NAME_VALIDATION: {
    regexExpressionForName:
      /^([A-Za-z0-9_]|[\u00C0-\u017F])([a-z0-9,._\-+/@= ]|[\u00C0-\u017F])+$/i,
    regexExpressionForFirstAndLastName:
      /^([A-Za-z0-9_]|[\u00C0-\u017F])([a-zA-Z0-9\s_. -@]|[\u00C0-\u017F])+$/i,
    regexforNameValidation: /^[a-zA-Z0-9_-][a-zA-Z0-9\s_-]{0,254}$/,
    regexExpressionForSecondaryName:
      /^([A-Za-z0-9_]|[\u00C0-\u017F])([a-zA-Z0-9\s_. -@]|[\u00C0-\u017F])+$/i,
    regexforServiceNameValidation: /^[a-zA-Z0-9_-][a-zA-Z0-9_-]{0,254}$/,
    serviceNameValidationMessage:
      "Name should not contain space, it should be less than 256 characters and special characters are not allowed (except _ -).",
    nameValidationMessage: (
      <>
        1. Name should be start with alphabet / numeric / underscore / non-us
        characters.
        <br />
        2. Allowed special character ,._-+/@= and space. <br />
        3. Name length should be greater than one."
      </>
    ),
    regexforNameValidationMessage:
      "Name should not start with space, it should be less than 256 characters and special characters are not allowed(except _ - and space).",
    secondaryNameValidationMessage: (
      <>
        1. Name should be start with alphabet / numeric / underscore / non-us
        characters.
        <br />
        2. Allowed special character ._-@ and space.
        <br />
        3. Name length should be greater than one."
      </>
    )
  },
  EMAIL_VALIDATION: {
    regexExpressionForEmail:
      /^[\w]([\-\.\w])+[\w]+@[\w]+[\w\-]+[\w]*\.([\w]+[\w\-]+[\w]*(\.[a-z][a-z|0-9]*)?)$/,
    message: "Invalid email address"
  }
};

export const PathAssociateWithModule = {
  "Resource Based Policies": [
    "/policymanager/resource",
    "/service/:serviceDefId/create",
    "/service/:serviceDefId/edit/:serviceId",
    "/service/:serviceId/policies/:policyType",
    "/service/:serviceId/policies/create/:policyType",
    "/service/:serviceId/policies/:policyId/edit"
  ],
  "Tag Based Policies": [
    "/policymanager/tag",
    "/service/:serviceDefId/create",
    "/service/:serviceDefId/edit/:serviceId",
    "/service/:serviceId/policies/:policyType",
    "/service/:serviceId/policies/create/:policyType",
    "/service/:serviceId/policies/:policyId/edit"
  ],
  Reports: ["/reports/userAccess"],
  Audit: [
    "/reports/audit/bigData",
    "/reports/audit/admin",
    "/reports/audit/loginSession",
    "/reports/audit/agent",
    "/reports/audit/pluginStatus",
    "/reports/audit/userSync",
    "/reports/audit/eventlog/:eventId"
  ],
  "Security Zone": [
    "/zones/zone/list",
    "/zones/zone/:id",
    "/zones/create",
    "/zones/edit/:id"
  ],
  "Key Manager": [
    "/kms/keys/:kmsManagePage/manage/:kmsServiceName",
    "/kms/keys/:serviceName/create"
  ],
  "Users/Groups": [
    "/users/usertab",
    "/user/create",
    "/user/:userID",
    "/users/grouptab",
    "/group/create",
    "/group/:groupId",
    "/users/roletab",
    "/roles/create",
    "/roles/:roleId"
  ],
  Permission: ["/permissions/models", "/permissions/:permissionId/edit"],
  Profile: ["/userprofile"],
  KnoxSignOut: ["/knoxSSOWarning"],
  DataNotFound: ["/dataNotFound"],
  PageNotFound: ["/pageNotFound"],
  localLogin: ["/locallogin"],
  slashPath: ["/"],
  Forbidden: ["/forbidden"]
};

/* Access */

export const DefStatus = {
  RecursiveStatus: {
    STATUS_RECURSIVE: {
      value: true,
      label: "recursive",
      rbkey: "xa.enum.RecursiveStatus.RECURSIVE",
      tt: "lbl.RecursiveStatus_RECURSIVE"
    },
    STATUS_NONRECURSIVE: {
      value: false,
      label: "nonrecursive",
      rbkey: "xa.enum.RecursiveStatus.NONRECURSIVE",
      tt: "lbl.RecursiveStatus_NONRECURSIVE"
    }
  },
  ExcludeStatus: {
    STATUS_EXCLUDE: {
      value: true,
      label: "exclude",
      rbkey: "xa.enum.ExcludeStatus.EXCLUDE",
      tt: "lbl.ExcludeStatus_EXCLUDE"
    },
    STATUS_INCLUDE: {
      value: false,
      label: "include",
      rbkey: "xa.enum.ExcludeStatus.INCLUDE",
      tt: "lbl.ExcludeStatus_INCLUDE"
    }
  }
};

/* PolicyListing QuerParams Name */

export const QueryParams = {
  PolicyListing: {
    id: { columnName: "id", queryParamName: "policyId" },
    name: { columnName: "name", queryParamName: "policyName" }
  }
};

/* Alert warning Message in Policy Listing */

export const alertMessage = {
  hdfs: {
    label: "HDFS",
    configs: "xasecure.add-hadoop"
  },
  yarn: {
    label: "YARN",
    configs: "ranger.add-yarn"
  }
};

export const ServiceType = {
  Service_UNKNOWN: {
    value: 0,
    label: "Unknown",
    rbkey: "xa.enum.AssetType.ASSET_UNKNOWN",
    tt: "lbl.AssetType_ASSET_UNKNOWN"
  },
  Service_HDFS: {
    value: 1,
    label: "hdfs",
    rbkey: "xa.enum.AssetType.ASSET_HDFS",
    tt: "lbl.AssetType_ASSET_HDFS"
  },
  Service_HIVE: {
    value: 2,
    label: "hive",
    rbkey: "xa.enum.AssetType.ASSET_HIVE",
    tt: "lbl.AssetType_ASSET_HIVE"
  },
  Service_HBASE: {
    value: 3,
    label: "hbase",
    rbkey: "xa.enum.AssetType.ASSET_HBASE",
    tt: "lbl.AssetType_ASSET_HBASE"
  },
  Service_KNOX: {
    value: 4,
    label: "knox",
    rbkey: "xa.enum.AssetType.ASSET_KNOX",
    tt: "lbl.AssetType_ASSET_KNOX"
  },
  Service_STORM: {
    value: 5,
    label: "storm",
    rbkey: "xa.enum.AssetType.ASSET_STORM",
    tt: "lbl.AssetType_ASSET_STORM"
  },
  Service_SOLR: {
    value: 6,
    label: "solr",
    rbkey: "xa.enum.AssetType.ASSET_SOLR",
    tt: "lbl.AssetType_ASSET_SOLR"
  },
  SERVICE_TAG: {
    value: 7,
    label: "tag",
    rbkey: "xa.enum.ServiceType.SERVICE_TAG",
    tt: "lbl.ServiceType_SERVICE_TAG"
  },
  Service_KMS: {
    value: 8,
    label: "kms",
    rbkey: "xa.enum.ServiceType.SERVICE_KMS",
    tt: "lbl.ServiceType_SERVICE_KMS"
  },
  Service_YARN: {
    value: 8,
    label: "yarn",
    rbkey: "xa.enum.ServiceType.SERVICE_YARN",
    tt: "lbl.ServiceType_SERVICE_YARN"
  }
};
export const ServerAttrName = [
  {
    text: "Group Name",
    info: "Name of Group."
  },
  {
    text: "Policy Name",
    info: "Enter name of policy."
  },
  {
    text: "Status",
    info: "Status of Policy Enable/Disable."
  },
  {
    text: "User Name",
    info: "Name of User."
  },
  {
    text: "Role Name",
    info: "Name of Role."
  },
  {
    text: "Policy Label",
    info: "Label of policy"
  }
];

export const ResourcesOverrideInfoMsg = {
  collection: "Solr collection.",
  column: "Column Name",
  "column-family": "Hbase column-family",
  database: "Database",
  entity: "Atlas all-entity.",
  keyname: "Key Name",
  path: "Name of policy resource path.",
  queue: "Yarn queue.",
  service: "Name of service.",
  table: "Table Name",
  tag: "Tag Name.",
  topic: "Kafka topic.",
  topology: "Topology Name",
  type: "Policy for all type.",
  udf: "Hive udf.",
  url: "Hive url.",
  "type-category": "Atlas type category.",
  "entity-type": "Atlas entity type.",
  "entity-classification": "Atlas entity classification.",
  "atlas-service": "Atlas services.",
  connector: "Connectivity By Sqoop.",
  link: "Linker Name.",
  job: "Sqoop Job Name.",
  project: "Kylin Project Level.",
  "nifi-resource": "NiFi Resource Identifier."
};

export const ServiceRequestDataRangerAcl = [
  ServiceType.Service_HIVE.label,
  ServiceType.Service_HBASE.label,
  ServiceType.Service_HDFS.label,
  ServiceType.Service_SOLR.label
];

export const ServiceRequestDataHadoopAcl = [ServiceType.Service_HDFS.label];

export const UsersyncDetailsKeyDisplayMap = {
  unixBackend: "Unix",
  fileName: "File Name",
  syncTime: "Sync time",
  lastModified: "Last modified time",
  minUserId: "Minimum user id",
  minGroupId: "Minimum group id",
  totalUsersSynced: "Total number of users synced",
  totalGroupsSynced: "Total number of groups synced",
  totalUsersDeleted: "Total number of users marked for delete",
  totalGroupsDeleted: "Total number of groups marked for delete",
  ldapUrl: "Ldap url",
  isIncrementalSync: "Incremental sync",
  userSearchEnabled: "User search enabled",
  userSearchFilter: "User search filter",
  groupSearchEnabled: "Group search enabled",
  groupSearchFilter: "Group search filter",
  groupSearchFirstEnabled: "Group search first enabled",
  groupHierarchyLevel: "Group hierarchy level"
};

export const pluginStatusColumnInfoMsg = {
  Policy: {
    title: "Policy (Time details)",
    lastUpdated: "Last update time of policies",
    downloadTime: "Last policies download time (sync-up with Ranger).",
    activeTime:
      "Last time the downloaded policies became active for enforcement.",
    downloadTimeDelayMsg:
      "Latest update in policies are not yet downloaded (sync-up with Ranger).",
    activationTimeDelayMsg:
      "Latest update in policies are not yet active for enforcement."
  },
  Tag: {
    title: "Tag Policy (Time details)",
    lastUpdated: "Last update time of tags.",
    downloadTime: "Last tags download time (sync-up with Ranger).",
    activeTime: "Last time the downloaded tags became active for enforcement.",
    downloadTimeDelayMsg:
      "Latest update in tags are not yet downloaded (sync-up with Ranger).",
    activationTimeDelayMsg:
      "Latest update in tags are not yet active for enforcement."
  }
};

export const additionalServiceConfigs = [
  {
    label: "Policy Download Users",
    name: "policy.download.auth.users",
    type: "user"
  },
  {
    label: "Tag Download Users",
    name: "tag.download.auth.users",
    type: "user"
  },
  {
    label: "Service Admin Users",
    name: "service.admin.users",
    type: "user"
  },
  {
    label: "Service Admin Groups",
    name: "service.admin.groups",
    type: "group"
  },
  {
    label: "Superusers",
    name: "ranger.plugin.super.users",
    type: "user"
  },
  {
    label: "Superuser Groups",
    name: "ranger.plugin.super.groups",
    type: "group"
  },
  {
    label: "Userstore Download Users",
    name: "userstore.download.auth.users",
    type: "user"
  }
];
