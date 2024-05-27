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

import React, { useState } from "react";
import { getUserProfile } from "Utils/appState";
import {
  UserRoles,
  PathAssociateWithModule,
  QueryParams,
  RangerPolicyType,
  ServiceType
} from "Utils/XAEnums";
import {
  filter,
  find,
  flatMap,
  forEach,
  uniq,
  map,
  union,
  includes,
  pick,
  isEmpty,
  isUndefined,
  isNull,
  some,
  has,
  sortBy,
  isArray
} from "lodash";
import { matchRoutes } from "react-router-dom";
import dateFormat from "dateformat";
import moment from "moment-timezone";
import CustomBreadcrumb from "../views/CustomBreadcrumb";
import { CustomTooltip } from "../components/CommonComponents";
import InfiniteScroll from "react-infinite-scroll-component";
import { toast } from "react-toastify";
import { policyInfoMessage } from "./XAMessages";
import { fetchApi } from "Utils/fetchAPI";
import folderIcon from "Images/folder-grey.png";

export const LoginUser = (role) => {
  const userProfile = getUserProfile();
  const currentUserRoles = userProfile?.userRoleList[0];
  if (!currentUserRoles && currentUserRoles == "") {
    return false;
  }

  return currentUserRoles == role;
};

export const isSystemAdmin = () => {
  return LoginUser("ROLE_SYS_ADMIN") ? true : false;
};

export const isKeyAdmin = () => {
  return LoginUser("ROLE_KEY_ADMIN") ? true : false;
};

export const isUser = () => {
  return LoginUser("ROLE_USER") ? true : false;
};

export const isAuditor = () => {
  return LoginUser("ROLE_ADMIN_AUDITOR") ? true : false;
};

export const isKMSAuditor = () => {
  return LoginUser("ROLE_KEY_ADMIN_AUDITOR") ? true : false;
};

export const isRenderMasking = (dataMaskDef) => {
  return dataMaskDef &&
    dataMaskDef.maskTypes &&
    dataMaskDef.maskTypes.length > 0
    ? true
    : false;
};

export const isRenderRowFilter = (rowFilterDef) => {
  return rowFilterDef &&
    rowFilterDef.resources &&
    rowFilterDef.resources.length > 0
    ? true
    : false;
};

export const getUserAccessRoleList = () => {
  var userRoleList = [];
  filter(UserRoles, function (val, key) {
    if (
      isKeyAdmin() &&
      UserRoles.ROLE_SYS_ADMIN.value != val.value &&
      UserRoles.ROLE_ADMIN_AUDITOR.value != val.value
    ) {
      userRoleList.push({ value: key, label: val.label });
    } else if (
      isSystemAdmin() &&
      UserRoles.ROLE_KEY_ADMIN.value != val.value &&
      UserRoles.ROLE_KEY_ADMIN_AUDITOR.value != val.value
    ) {
      userRoleList.push({ value: key, label: val.label });
    } else if (isUser() && UserRoles.ROLE_USER.value == val.value) {
      userRoleList.push({ value: key, label: val.label });
    } else if (
      isAuditor() &&
      UserRoles.ROLE_KEY_ADMIN.value != val.value &&
      UserRoles.ROLE_KEY_ADMIN_AUDITOR.value != val.value
    ) {
      userRoleList.push({ value: key, label: val.label });
    } else if (
      isKMSAuditor() &&
      UserRoles.ROLE_SYS_ADMIN.value != val.value &&
      UserRoles.ROLE_ADMIN_AUDITOR.value != val.value
    ) {
      userRoleList.push({ value: key, label: val.label });
    }
  });
  return userRoleList;
};

export const isObject = (value) => {
  return typeof value === "object" && !Array.isArray(value) && value !== null;
};

export const hasAccessToTab = (tabName) => {
  const userProfile = getUserProfile();
  let userModules = map(userProfile?.userPermList, "moduleName");
  let groupModules = map(userProfile?.groupPermissions, "moduleName");
  let moduleNames = union(userModules, groupModules);
  let returnFlag = includes(moduleNames, tabName);
  return returnFlag;
};

export const hasAccessToPath = (pathName) => {
  let allowPath = [];
  const userProfile = getUserProfile();
  if (pathName == "/") {
    pathName = "/policymanager/resource";
  }
  let userModules = map(userProfile?.userPermList, "moduleName");
  let groupModules = map(userProfile?.groupPermissions, "moduleName");
  let moduleNames = union(userModules, groupModules);
  moduleNames.push("Profile");
  moduleNames.push("KnoxSignOut");
  moduleNames.push("DataNotFound");
  moduleNames.push("PageNotFound");
  moduleNames.push("Forbidden");

  moduleNames.push("localLogin");
  if (isSystemAdmin() || isAuditor()) {
    moduleNames.push("Permission");
  }
  let allRouter = [],
    returnFlag = true;
  for (const key in PathAssociateWithModule) {
    allRouter = [...allRouter, ...PathAssociateWithModule[key]];
  }
  let isValidRouter = matchRoutes(
    allRouter.map((val) => ({ path: val })),
    pathName
  );
  if (isValidRouter != null && isValidRouter.length > 0) {
    forEach(moduleNames, function (key) {
      allowPath.push(PathAssociateWithModule[key]);
    });
    allowPath = uniq(flatMap(allowPath));
    returnFlag =
      matchRoutes(
        allowPath.map((val) => ({ path: val })),
        pathName
      ) === null;
  }

  return !returnFlag;
};

/* Time Stamp */
export const setTimeStamp = (dateTime) => {
  let formatDateTime = dateFormat(parseInt(dateTime), "mm/dd/yyyy hh:MM:ss TT");
  return !isEmpty(dateTime) ? (
    <span title={formatDateTime}>
      {formatDateTime}
      <div className="text-muted">
        <small>
          {moment(formatDateTime, "MM/DD/YYYY h:mm:ss A").fromNow()}
        </small>
      </div>
    </span>
  ) : (
    <center>--</center>
  );
};

/* Policy validity period time zone */
export const getAllTimeZoneList = () => {
  let Timezones = [
    { text: "Africa/Abidjan (GMT)", id: "Africa/Abidjan" },
    { text: "Africa/Accra (GMT)", id: "Africa/Accra" },
    { text: "Africa/Addis_Ababa (EAT)", id: "Africa/Addis_Ababa" },
    { text: "Africa/Algiers (CET)", id: "Africa/Algiers" },
    { text: "Africa/Asmara (EAT)", id: "Africa/Asmara" },
    { text: "Africa/Asmera (EAT)", id: "Africa/Asmera" },
    { text: "Africa/Bamako (GMT)", id: "Africa/Bamako" },
    { text: "Africa/Bangui (WAT)", id: "Africa/Bangui" },
    { text: "Africa/Banjul (GMT)", id: "Africa/Banjul" },
    { text: "Africa/Bissau (GMT)", id: "Africa/Bissau" },
    { text: "Africa/Blantyre (CAT)", id: "Africa/Blantyre" },
    { text: "Africa/Brazzaville (WAT)", id: "Africa/Brazzaville" },
    { text: "Africa/Cairo (EET)", id: "Africa/Cairo" },
    { text: "Africa/Casablanca (WEST)", id: "Africa/Casablanca" },
    { text: "Africa/Ceuta (CEST)", id: "Africa/Ceuta" },
    { text: "Africa/Conakry (GMT)", id: "Africa/Conakry" },
    { text: "Africa/Dakar (GMT)", id: "Africa/Dakar" },
    { text: "Africa/Dar_es_Salaam (EAT)", id: "Africa/Dar_es_Salaam" },
    { text: "Africa/Douala (WAT)", id: "Africa/Douala" },
    { text: "Africa/El_Aaiun (WEST)", id: "Africa/El_Aaiun" },
    { text: "Africa/Johannesburg (SAST)", id: "Africa/Johannesburg" },
    { text: "Africa/Freetown (GMT)", id: "Africa/Freetown" },
    { text: "Africa/Gaborone (CAT)", id: "Africa/Gaborone" },
    { text: "Africa/Harare (CAT)", id: "Africa/Harare" },
    { text: "Africa/Juba (EAT)", id: "Africa/Juba" },
    { text: "Africa/Kampala (EAT)", id: "Africa/Kampala" },
    { text: "Africa/Khartoum (CAT)", id: "Africa/Khartoum" },
    { text: "Africa/Kigali (CAT)", id: "Africa/Kigali" },
    { text: "Africa/Luanda (WAT)", id: "Africa/Luanda" },
    { text: "Africa/Kinshasa (WAT)", id: "Africa/Kinshasa" },
    { text: "Africa/Lagos (WAT)", id: "Africa/Lagos" },
    { text: "Africa/Libreville (WAT)", id: "Africa/Libreville" },
    { text: "Africa/Lome (GMT)", id: "Africa/Lome" },
    { text: "Africa/Lubumbashi (CAT)", id: "Africa/Lubumbashi" },
    { text: "Africa/Lusaka (CAT)", id: "Africa/Lusaka" },
    { text: "Africa/Malabo (WAT)", id: "Africa/Malabo" },
    { text: "Africa/Maputo (CAT)", id: "Africa/Maputo" },
    { text: "Africa/Maseru (SAST)", id: "Africa/Maseru" },
    { text: "Africa/Ndjamena (WAT)", id: "Africa/Ndjamena" },
    { text: "Africa/Mbabane (SAST)", id: "Africa/Mbabane" },
    { text: "Africa/Mogadishu (EAT)", id: "Africa/Mogadishu" },
    { text: "Africa/Monrovia (GMT)", id: "Africa/Monrovia" },
    { text: "Africa/Nairobi (EAT)", id: "Africa/Nairobi" },
    { text: "Africa/Niamey (WAT)", id: "Africa/Niamey" },
    { text: "Africa/Nouakchott (GMT)", id: "Africa/Nouakchott" },
    { text: "Africa/Ouagadougou (GMT)", id: "Africa/Ouagadougou" },
    { text: "Africa/Porto-Novo (WAT)", id: "Africa/Porto-Novo" },
    { text: "Africa/Sao_Tome (GMT)", id: "Africa/Sao_Tome" },
    { text: "Africa/Tripoli (EET)", id: "Africa/Tripoli" },
    { text: "Africa/Tunis (CET)", id: "Africa/Tunis" },
    { text: "Africa/Timbuktu (GMT)", id: "Africa/Timbuktu" },
    { text: "Africa/Windhoek (CAT)", id: "Africa/Windhoek" },
    { text: "America/Adak (HDT)", id: "America/Adak" },
    { text: "America/Antigua (AST)", id: "America/Antigua" },
    { text: "America/Araguaina (BRT)", id: "America/Araguaina" },
    {
      text: "America/Argentina/Buenos_Aires (ART)",
      id: "America/Argentina/Buenos_Aires"
    },
    {
      text: "America/Argentina/Catamarca (ART)",
      id: "America/Argentina/Catamarca"
    },
    {
      text: "America/Argentina/ComodRivadavia (ART)",
      id: "America/Argentina/ComodRivadavia"
    },
    {
      text: "America/Argentina/Cordoba (ART)",
      id: "America/Argentina/Cordoba"
    },
    { text: "America/Argentina/Jujuy (ART)", id: "America/Argentina/Jujuy" },
    {
      text: "America/Argentina/La_Rioja (ART)",
      id: "America/Argentina/La_Rioja"
    },
    {
      text: "America/Argentina/Mendoza (ART)",
      id: "America/Argentina/Mendoza"
    },
    {
      text: "America/Argentina/Rio_Gallegos (ART)",
      id: "America/Argentina/Rio_Gallegos"
    },
    { text: "America/Argentina/Salta (ART)", id: "America/Argentina/Salta" },
    {
      text: "America/Argentina/San_Juan (ART)",
      id: "America/Argentina/San_Juan"
    },
    { text: "America/Anchorage (AKDT)", id: "America/Anchorage" },
    { text: "America/Anguilla (AST)", id: "America/Anguilla" },
    {
      text: "America/Argentina/San_Luis (ART)",
      id: "America/Argentina/San_Luis"
    },
    {
      text: "America/Argentina/Tucuman (ART)",
      id: "America/Argentina/Tucuman"
    },
    {
      text: "America/Argentina/Ushuaia (ART)",
      id: "America/Argentina/Ushuaia"
    },
    { text: "America/Asuncion (PYST)", id: "America/Asuncion" },
    { text: "America/Aruba (AST)", id: "America/Aruba" },
    { text: "America/Atikokan (EST)", id: "America/Atikokan" },
    { text: "America/Bahia (BRT)", id: "America/Bahia" },
    { text: "America/Bahia_Banderas (CDT)", id: "America/Bahia_Banderas" },
    { text: "America/Atka (HDT)", id: "America/Atka" },
    { text: "America/Barbados (AST)", id: "America/Barbados" },
    { text: "America/Belem (BRT)", id: "America/Belem" },
    { text: "America/Belize (CST)", id: "America/Belize" },
    { text: "America/Blanc-Sablon (AST)", id: "America/Blanc-Sablon" },
    { text: "America/Boa_Vista (AMT)", id: "America/Boa_Vista" },
    { text: "America/Bogota (COT)", id: "America/Bogota" },
    { text: "America/Boise (MDT)", id: "America/Boise" },
    { text: "America/Buenos_Aires (ART)", id: "America/Buenos_Aires" },
    { text: "America/Cambridge_Bay (MDT)", id: "America/Cambridge_Bay" },
    { text: "America/Campo_Grande (AMST)", id: "America/Campo_Grande" },
    { text: "America/Cancun (EST)", id: "America/Cancun" },
    { text: "America/Caracas (VET)", id: "America/Caracas" },
    { text: "America/Catamarca (ART)", id: "America/Catamarca" },
    { text: "America/Cayenne (GFT)", id: "America/Cayenne" },
    { text: "America/Cayman (EST)", id: "America/Cayman" },
    { text: "America/Chicago (CDT)", id: "America/Chicago" },
    { text: "America/Chihuahua (MDT)", id: "America/Chihuahua" },
    { text: "America/Coral_Harbour (EST)", id: "America/Coral_Harbour" },
    { text: "America/Cordoba (ART)", id: "America/Cordoba" },
    { text: "America/Costa_Rica (CST)", id: "America/Costa_Rica" },
    { text: "America/Creston (MST)", id: "America/Creston" },
    { text: "America/Cuiaba (AMST)", id: "America/Cuiaba" },
    { text: "America/Curacao (AST)", id: "America/Curacao" },
    { text: "America/Danmarkshavn (GMT)", id: "America/Danmarkshavn" },
    { text: "America/Dawson (PDT)", id: "America/Dawson" },
    { text: "America/Dawson_Creek (MST)", id: "America/Dawson_Creek" },
    { text: "America/Denver (MDT)", id: "America/Denver" },
    { text: "America/Detroit (EDT)", id: "America/Detroit" },
    { text: "America/Dominica (AST)", id: "America/Dominica" },
    { text: "America/Edmonton (MDT)", id: "America/Edmonton" },
    { text: "America/Eirunepe (ACT)", id: "America/Eirunepe" },
    { text: "America/El_Salvador (CST)", id: "America/El_Salvador" },
    { text: "America/Ensenada (PDT)", id: "America/Ensenada" },
    { text: "America/Fort_Nelson (MST)", id: "America/Fort_Nelson" },
    { text: "America/Fort_Wayne (EDT)", id: "America/Fort_Wayne" },
    { text: "America/Fortaleza (BRT)", id: "America/Fortaleza" },
    { text: "America/Glace_Bay (ADT)", id: "America/Glace_Bay" },
    { text: "America/Godthab (WGT)", id: "America/Godthab" },
    { text: "America/Goose_Bay (ADT)", id: "America/Goose_Bay" },
    { text: "America/Grand_Turk (EDT)", id: "America/Grand_Turk" },
    { text: "America/Grenada (AST)", id: "America/Grenada" },
    { text: "America/Guadeloupe (AST)", id: "America/Guadeloupe" },
    { text: "America/Guatemala (CST)", id: "America/Guatemala" },
    { text: "America/Guayaquil (ECT)", id: "America/Guayaquil" },
    { text: "America/Guyana (GYT)", id: "America/Guyana" },
    { text: "America/Halifax (ADT)", id: "America/Halifax" },
    { text: "America/Indiana/Knox (CDT)", id: "America/Indiana/Knox" },
    { text: "America/Indiana/Marengo (EDT)", id: "America/Indiana/Marengo" },
    {
      text: "America/Indiana/Petersburg (EDT)",
      id: "America/Indiana/Petersburg"
    },
    { text: "America/Havana (CDT)", id: "America/Havana" },
    { text: "America/Hermosillo (MST)", id: "America/Hermosillo" },
    {
      text: "America/Indiana/Indianapolis (EDT)",
      id: "America/Indiana/Indianapolis"
    },
    {
      text: "America/Indiana/Tell_City (CDT)",
      id: "America/Indiana/Tell_City"
    },
    { text: "America/Indiana/Vevay (EDT)", id: "America/Indiana/Vevay" },
    {
      text: "America/Indiana/Vincennes (EDT)",
      id: "America/Indiana/Vincennes"
    },
    { text: "America/Indiana/Winamac (EDT)", id: "America/Indiana/Winamac" },
    { text: "America/Indianapolis (EDT)", id: "America/Indianapolis" },
    { text: "America/Inuvik (MDT)", id: "America/Inuvik" },
    { text: "America/Iqaluit (EDT)", id: "America/Iqaluit" },
    { text: "America/Jamaica (EST)", id: "America/Jamaica" },
    { text: "America/Juneau (AKDT)", id: "America/Juneau" },
    {
      text: "America/Kentucky/Louisville (EDT)",
      id: "America/Kentucky/Louisville"
    },
    {
      text: "America/Kentucky/Monticello (EDT)",
      id: "America/Kentucky/Monticello"
    },
    { text: "America/Knox_IN (CDT)", id: "America/Knox_IN" },
    { text: "America/Kralendijk (AST)", id: "America/Kralendijk" },
    { text: "America/Jujuy (ART)", id: "America/Jujuy" },
    { text: "America/La_Paz (BOT)", id: "America/La_Paz" },
    { text: "America/Louisville (EDT)", id: "America/Louisville" },
    { text: "America/Lower_Princes (AST)", id: "America/Lower_Princes" },
    { text: "America/Maceio (BRT)", id: "America/Maceio" },
    { text: "America/Lima (PET)", id: "America/Lima" },
    { text: "America/Los_Angeles (PDT)", id: "America/Los_Angeles" },
    { text: "America/Managua (CST)", id: "America/Managua" },
    { text: "America/Manaus (AMT)", id: "America/Manaus" },
    { text: "America/Marigot (AST)", id: "America/Marigot" },
    { text: "America/Martinique (AST)", id: "America/Martinique" },
    { text: "America/Matamoros (CDT)", id: "America/Matamoros" },
    { text: "America/Mazatlan (MDT)", id: "America/Mazatlan" },
    { text: "America/Menominee (CDT)", id: "America/Menominee" },
    { text: "America/Merida (CDT)", id: "America/Merida" },
    { text: "America/Mendoza (ART)", id: "America/Mendoza" },
    { text: "America/Metlakatla (AKDT)", id: "America/Metlakatla" },
    { text: "America/Mexico_City (CDT)", id: "America/Mexico_City" },
    { text: "America/Miquelon (PMST)", id: "America/Miquelon" },
    { text: "America/Moncton (ADT)", id: "America/Moncton" },
    { text: "America/Monterrey (CDT)", id: "America/Monterrey" },
    { text: "America/Montevideo (UYT)", id: "America/Montevideo" },
    { text: "America/Montreal (EDT)", id: "America/Montreal" },
    { text: "America/Montserrat (AST)", id: "America/Montserrat" },
    { text: "America/Nassau (EDT)", id: "America/Nassau" },
    { text: "America/New_York (EDT)", id: "America/New_York" },
    { text: "America/Nipigon (EDT)", id: "America/Nipigon" },
    { text: "America/Nome (AKDT)", id: "America/Nome" },
    { text: "America/Noronha (FNT)", id: "America/Noronha" },
    {
      text: "America/North_Dakota/Beulah (CDT)",
      id: "America/North_Dakota/Beulah"
    },
    {
      text: "America/North_Dakota/Center (CDT)",
      id: "America/North_Dakota/Center"
    },
    {
      text: "America/North_Dakota/New_Salem (CDT)",
      id: "America/North_Dakota/New_Salem"
    },
    { text: "America/Ojinaga (MDT)", id: "America/Ojinaga" },
    { text: "America/Panama (EST)", id: "America/Panama" },
    { text: "America/Pangnirtung (EDT)", id: "America/Pangnirtung" },
    { text: "America/Paramaribo (SRT)", id: "America/Paramaribo" },
    { text: "America/Phoenix (MST)", id: "America/Phoenix" },
    { text: "America/Port_of_Spain (AST)", id: "America/Port_of_Spain" },
    { text: "America/Port-au-Prince (EDT)", id: "America/Port-au-Prince" },
    { text: "America/Porto_Velho (AMT)", id: "America/Porto_Velho" },
    { text: "America/Porto_Acre (ACT)", id: "America/Porto_Acre" },
    { text: "America/Puerto_Rico (AST)", id: "America/Puerto_Rico" },
    { text: "America/Punta_Arenas (CLST)", id: "America/Punta_Arenas" },
    { text: "America/Rainy_River (CDT)", id: "America/Rainy_River" },
    { text: "America/Rankin_Inlet (CDT)", id: "America/Rankin_Inlet" },
    { text: "America/Recife (BRT)", id: "America/Recife" },
    { text: "America/Regina (CST)", id: "America/Regina" },
    { text: "America/Resolute (CDT)", id: "America/Resolute" },
    { text: "America/Rio_Branco (ACT)", id: "America/Rio_Branco" },
    { text: "America/Rosario (ART)", id: "America/Rosario" },
    { text: "America/Santa_Isabel (PDT)", id: "America/Santa_Isabel" },
    { text: "America/Santarem (BRT)", id: "America/Santarem" },
    { text: "America/Santiago (CLT)", id: "America/Santiago" },
    { text: "America/Santo_Domingo (AST)", id: "America/Santo_Domingo" },
    { text: "America/Sao_Paulo (BRST)", id: "America/Sao_Paulo" },
    { text: "America/Scoresbysund (EGT)", id: "America/Scoresbysund" },
    { text: "America/Shiprock (MDT)", id: "America/Shiprock" },
    { text: "America/Sitka (AKDT)", id: "America/Sitka" },
    { text: "America/St_Barthelemy (AST)", id: "America/St_Barthelemy" },
    { text: "America/St_Johns (NDT)", id: "America/St_Johns" },
    { text: "America/St_Kitts (AST)", id: "America/St_Kitts" },
    { text: "America/St_Thomas (AST)", id: "America/St_Thomas" },
    { text: "America/St_Lucia (AST)", id: "America/St_Lucia" },
    { text: "America/St_Vincent (AST)", id: "America/St_Vincent" },
    { text: "America/Swift_Current (CST)", id: "America/Swift_Current" },
    { text: "America/Tegucigalpa (CST)", id: "America/Tegucigalpa" },
    { text: "America/Thunder_Bay (EDT)", id: "America/Thunder_Bay" },
    { text: "America/Tijuana (PDT)", id: "America/Tijuana" },
    { text: "America/Toronto (EDT)", id: "America/Toronto" },
    { text: "America/Tortola (AST)", id: "America/Tortola" },
    { text: "America/Vancouver (PDT)", id: "America/Vancouver" },
    { text: "America/Virgin (AST)", id: "America/Virgin" },
    { text: "America/Thule (ADT)", id: "America/Thule" },
    { text: "America/Whitehorse (PDT)", id: "America/Whitehorse" },
    { text: "America/Winnipeg (CDT)", id: "America/Winnipeg" },
    { text: "America/Yakutat (AKDT)", id: "America/Yakutat" },
    { text: "America/Yellowknife (MDT)", id: "America/Yellowknife" },
    { text: "Antarctica/Casey (CAST)", id: "Antarctica/Casey" },
    { text: "Antarctica/Davis (DAVT)", id: "Antarctica/Davis" },
    {
      text: "Antarctica/DumontDUrville (DDUT)",
      id: "Antarctica/DumontDUrville"
    },
    { text: "Antarctica/Macquarie (MIST)", id: "Antarctica/Macquarie" },
    { text: "Antarctica/Mawson (MAWT)", id: "Antarctica/Mawson" },
    { text: "Antarctica/McMurdo (NZST)", id: "Antarctica/McMurdo" },
    { text: "Antarctica/Palmer (CLST)", id: "Antarctica/Palmer" },
    { text: "Antarctica/Rothera (ART)", id: "Antarctica/Rothera" },
    { text: "Antarctica/South_Pole (NZST)", id: "Antarctica/South_Pole" },
    { text: "Antarctica/Syowa (SYOT)", id: "Antarctica/Syowa" },
    { text: "Antarctica/Troll (CEST)", id: "Antarctica/Troll" },
    { text: "Antarctica/Vostok (VOST)", id: "Antarctica/Vostok" },
    { text: "Arctic/Longyearbyen (CEST)", id: "Arctic/Longyearbyen" },
    { text: "Asia/Aden (AST)", id: "Asia/Aden" },
    { text: "Asia/Almaty (ALMT)", id: "Asia/Almaty" },
    { text: "Asia/Amman (EEST)", id: "Asia/Amman" },
    { text: "Asia/Anadyr (ANAT)", id: "Asia/Anadyr" },
    { text: "Asia/Aqtau (AQTT)", id: "Asia/Aqtau" },
    { text: "Asia/Aqtobe (AQTT)", id: "Asia/Aqtobe" },
    { text: "Asia/Ashgabat (TMT)", id: "Asia/Ashgabat" },
    { text: "Asia/Ashkhabad (TMT)", id: "Asia/Ashkhabad" },
    { text: "Asia/Atyrau (AQTT)", id: "Asia/Atyrau" },
    { text: "Asia/Baghdad (AST)", id: "Asia/Baghdad" },
    { text: "Asia/Baku (AZT)", id: "Asia/Baku" },
    { text: "Asia/Bahrain (AST)", id: "Asia/Bahrain" },
    { text: "Asia/Barnaul", id: "Asia/Barnaul" },
    { text: "Asia/Beirut (EEST)", id: "Asia/Beirut" },
    { text: "Asia/Bishkek (KGT)", id: "Asia/Bishkek" },
    { text: "Asia/Brunei (BNT)", id: "Asia/Brunei" },
    { text: "Asia/Bangkok (ICT)", id: "Asia/Bangkok" },
    { text: "Asia/Calcutta (IST)", id: "Asia/Calcutta" },
    { text: "Asia/Chita (YAKT)", id: "Asia/Chita" },
    { text: "Asia/Choibalsan (CHOT)", id: "Asia/Choibalsan" },
    { text: "Asia/Chongqing (CST)", id: "Asia/Chongqing" },
    { text: "Asia/Chungking (CST)", id: "Asia/Chungking" },
    { text: "Asia/Colombo (IST)", id: "Asia/Colombo" },
    { text: "Asia/Dacca (BST)", id: "Asia/Dacca" },
    { text: "Asia/Damascus (EEST)", id: "Asia/Damascus" },
    { text: "Asia/Dhaka (BDT)", id: "Asia/Dhaka" },
    { text: "Asia/Dili (TLT)", id: "Asia/Dili" },
    { text: "Asia/Dubai (GST)", id: "Asia/Dubai" },
    { text: "Asia/Dushanbe (TJT)", id: "Asia/Dushanbe" },
    { text: "Asia/Famagusta (EEST)", id: "Asia/Famagusta" },
    { text: "Asia/Gaza (EEST)", id: "Asia/Gaza" },
    { text: "Asia/Harbin (CST)", id: "Asia/Harbin" },
    { text: "Asia/Hebron (EEST)", id: "Asia/Hebron" },
    { text: "Asia/Hong_Kong (HKT)", id: "Asia/Hong_Kong" },
    { text: "Asia/Ho_Chi_Minh (ICT)", id: "Asia/Ho_Chi_Minh" },
    { text: "Asia/Hovd (HOVT)", id: "Asia/Hovd" },
    { text: "Asia/Irkutsk (IRKT)", id: "Asia/Irkutsk" },
    { text: "Asia/Istanbul (TRT)", id: "Asia/Istanbul" },
    { text: "Asia/Jakarta (WIB)", id: "Asia/Jakarta" },
    { text: "Asia/Jayapura (WIT)", id: "Asia/Jayapura" },
    { text: "Asia/Jerusalem (IDT)", id: "Asia/Jerusalem" },
    { text: "Asia/Kabul (AFT)", id: "Asia/Kabul" },
    { text: "Asia/Kamchatka (PETT)", id: "Asia/Kamchatka" },
    { text: "Asia/Karachi (PKT)", id: "Asia/Karachi" },
    { text: "Asia/Kashgar (XJT)", id: "Asia/Kashgar" },
    { text: "Asia/Kathmandu (NPT)", id: "Asia/Kathmandu" },
    { text: "Asia/Katmandu (NPT)", id: "Asia/Katmandu" },
    { text: "Asia/Khandyga (YAKT)", id: "Asia/Khandyga" },
    { text: "Asia/Kolkata (IST)", id: "Asia/Kolkata" },
    { text: "Asia/Krasnoyarsk (KRAT)", id: "Asia/Krasnoyarsk" },
    { text: "Asia/Kuala_Lumpur (MYT)", id: "Asia/Kuala_Lumpur" },
    { text: "Asia/Kuching (MYT)", id: "Asia/Kuching" },
    { text: "Asia/Kuwait (AST)", id: "Asia/Kuwait" },
    { text: "Asia/Macao (CST)", id: "Asia/Macao" },
    { text: "Asia/Macau (CST)", id: "Asia/Macau" },
    { text: "Asia/Magadan (MAGT)", id: "Asia/Magadan" },
    { text: "Asia/Makassar (WITA)", id: "Asia/Makassar" },
    { text: "Asia/Manila (PHT)", id: "Asia/Manila" },
    { text: "Asia/Muscat (GST)", id: "Asia/Muscat" },
    { text: "Africa/Bujumbura (CAT)", id: "Africa/Bujumbura" },
    { text: "Asia/Nicosia (EEST)", id: "Asia/Nicosia" },
    { text: "Asia/Novokuznetsk (KRAT)", id: "Asia/Novokuznetsk" },
    { text: "Asia/Novosibirsk (NOVT)", id: "Asia/Novosibirsk" },
    { text: "Asia/Omsk (OMST)", id: "Asia/Omsk" },
    { text: "Asia/Oral (ORAT)", id: "Asia/Oral" },
    { text: "Asia/Phnom_Penh (ICT)", id: "Asia/Phnom_Penh" },
    { text: "Asia/Pontianak (WIB)", id: "Asia/Pontianak" },
    { text: "Asia/Pyongyang (KST)", id: "Asia/Pyongyang" },
    { text: "Asia/Qatar (AST)", id: "Asia/Qatar" },
    { text: "Asia/Qyzylorda (QYZT)", id: "Asia/Qyzylorda" },
    { text: "Asia/Rangoon (MMT)", id: "Asia/Rangoon" },
    { text: "Asia/Riyadh (AST)", id: "Asia/Riyadh" },
    { text: "Asia/Saigon (ICT)", id: "Asia/Saigon" },
    { text: "Asia/Sakhalin (SAKT)", id: "Asia/Sakhalin" },
    { text: "Asia/Samarkand (UZT)", id: "Asia/Samarkand" },
    { text: "Asia/Seoul (KST)", id: "Asia/Seoul" },
    { text: "Asia/Shanghai (CST)", id: "Asia/Shanghai" },
    { text: "Asia/Singapore (SGT)", id: "Asia/Singapore" },
    { text: "Asia/Srednekolymsk (SRET)", id: "Asia/Srednekolymsk" },
    { text: "Asia/Taipei (CST)", id: "Asia/Taipei" },
    { text: "Asia/Tashkent (UZT)", id: "Asia/Tashkent" },
    { text: "Asia/Tbilisi (GET)", id: "Asia/Tbilisi" },
    { text: "Asia/Tehran (IRST)", id: "Asia/Tehran" },
    { text: "Asia/Tel_Aviv (IDT)", id: "Asia/Tel_Aviv" },
    { text: "Asia/Thimbu (BTT)", id: "Asia/Thimbu" },
    { text: "Asia/Thimphu (BTT)", id: "Asia/Thimphu" },
    { text: "Asia/Tokyo (JST)", id: "Asia/Tokyo" },
    { text: "Africa/Djibouti (EAT)", id: "Africa/Djibouti" },
    { text: "Asia/Tomsk", id: "Asia/Tomsk" },
    { text: "Asia/Ujung_Pandang (WITA)", id: "Asia/Ujung_Pandang" },
    { text: "Asia/Ulaanbaatar (ULAT)", id: "Asia/Ulaanbaatar" },
    { text: "Asia/Ulan_Bator (ULAT)", id: "Asia/Ulan_Bator" },
    { text: "Asia/Urumqi (XJT)", id: "Asia/Urumqi" },
    { text: "Asia/Ust-Nera (VLAT)", id: "Asia/Ust-Nera" },
    { text: "Asia/Vientiane (ICT)", id: "Asia/Vientiane" },
    { text: "Asia/Vladivostok (VLAT)", id: "Asia/Vladivostok" },
    { text: "Asia/Yakutsk (YAKT)", id: "Asia/Yakutsk" },
    { text: "Asia/Yangon (MMT)", id: "Asia/Yangon" },
    { text: "Asia/Yekaterinburg (YEKT)", id: "Asia/Yekaterinburg" },
    { text: "Asia/Yerevan (AMT)", id: "Asia/Yerevan" },
    { text: "Atlantic/Azores (AZOT)", id: "Atlantic/Azores" },
    { text: "Atlantic/Bermuda (ADT)", id: "Atlantic/Bermuda" },
    { text: "Atlantic/Canary (WEST)", id: "Atlantic/Canary" },
    { text: "Atlantic/Cape_Verde (CVT)", id: "Atlantic/Cape_Verde" },
    { text: "Atlantic/Faeroe (WEST)", id: "Atlantic/Faeroe" },
    { text: "Atlantic/Faroe (WEST)", id: "Atlantic/Faroe" },
    { text: "Atlantic/Jan_Mayen (CEST)", id: "Atlantic/Jan_Mayen" },
    { text: "Atlantic/Madeira (WEST)", id: "Atlantic/Madeira" },
    { text: "Atlantic/Reykjavik (GMT)", id: "Atlantic/Reykjavik" },
    { text: "Atlantic/South_Georgia (GST)", id: "Atlantic/South_Georgia" },
    { text: "Atlantic/St_Helena (GMT)", id: "Atlantic/St_Helena" },
    { text: "Atlantic/Stanley (FKST)", id: "Atlantic/Stanley" },
    { text: "Australia/ACT (AEST)", id: "Australia/ACT" },
    { text: "Australia/Adelaide (ACST)", id: "Australia/Adelaide" },
    { text: "Australia/Brisbane (AEST)", id: "Australia/Brisbane" },
    { text: "Australia/Canberra (AEST)", id: "Australia/Canberra" },
    { text: "Australia/Eucla (ACWST)", id: "Australia/Eucla" },
    { text: "Australia/Hobart (AEST)", id: "Australia/Hobart" },
    { text: "Australia/Broken_Hill (ACST)", id: "Australia/Broken_Hill" },
    { text: "Australia/Currie (AEST)", id: "Australia/Currie" },
    { text: "Australia/Darwin (ACST)", id: "Australia/Darwin" },
    { text: "Australia/LHI (LHT)", id: "Australia/LHI" },
    { text: "Australia/Lindeman (AEST)", id: "Australia/Lindeman" },
    { text: "Australia/Lord_Howe (LHDT)", id: "Australia/Lord_Howe" },
    { text: "Australia/Melbourne (AEST)", id: "Australia/Melbourne" },
    { text: "Australia/North (ACST)", id: "Australia/North" },
    { text: "Australia/NSW (AEST)", id: "Australia/NSW" },
    { text: "Australia/Perth (AWST)", id: "Australia/Perth" },
    { text: "Australia/Queensland (AEST)", id: "Australia/Queensland" },
    { text: "Australia/South (ACST)", id: "Australia/South" },
    { text: "Australia/Sydney (AEST)", id: "Australia/Sydney" },
    { text: "Australia/Tasmania (AEST)", id: "Australia/Tasmania" },
    { text: "Australia/Victoria (AEST)", id: "Australia/Victoria" },
    { text: "Australia/West (AWST)", id: "Australia/West" },
    { text: "Australia/Yancowinna (ACST)", id: "Australia/Yancowinna" },
    { text: "Brazil/Acre (ACT)", id: "Brazil/Acre" },
    { text: "Brazil/DeNoronha (FNT)", id: "Brazil/DeNoronha" },
    { text: "Brazil/East (BRT)", id: "Brazil/East" },
    { text: "Brazil/West (AMT)", id: "Brazil/West" },
    { text: "Canada/Atlantic (ADT)", id: "Canada/Atlantic" },
    { text: "Canada/Central (CDT)", id: "Canada/Central" },
    { text: "Canada/Eastern (EDT)", id: "Canada/Eastern" },
    { text: "Canada/Mountain (MDT)", id: "Canada/Mountain" },
    { text: "Canada/Newfoundland (NDT)", id: "Canada/Newfoundland" },
    { text: "Canada/Pacific (PDT)", id: "Canada/Pacific" },
    { text: "Canada/Saskatchewan (CST)", id: "Canada/Saskatchewan" },
    { text: "Canada/Yukon (PDT)", id: "Canada/Yukon" },
    { text: "CET (CEST)", id: "CET" },
    { text: "Chile/Continental (CLT)", id: "Chile/Continental" },
    { text: "Chile/EasterIsland (EAST)", id: "Chile/EasterIsland" },
    { text: "CST6CDT (CDT)", id: "CST6CDT" },
    { text: "Cuba (CDT)", id: "Cuba" },
    { text: "EET (EEST)", id: "EET" },
    { text: "Egypt (EET)", id: "Egypt" },
    { text: "Eire (IST)", id: "Eire" },
    { text: "EST (EST)", id: "EST" },
    { text: "EST5EDT (EDT)", id: "EST5EDT" },
    { text: "Etc/GMT (GMT)", id: "Etc/GMT" },
    { text: "Etc/GMT-0 (GMT)", id: "Etc/GMT-0" },
    { text: "Etc/GMT-1", id: "Etc/GMT-1" },
    { text: "Etc/GMT-10", id: "Etc/GMT-10" },
    { text: "Etc/GMT-11", id: "Etc/GMT-11" },
    { text: "Etc/GMT-12", id: "Etc/GMT-12" },
    { text: "Etc/GMT-13", id: "Etc/GMT-13" },
    { text: "Etc/GMT-14", id: "Etc/GMT-14" },
    { text: "Etc/GMT-2", id: "Etc/GMT-2" },
    { text: "Etc/GMT-3", id: "Etc/GMT-3" },
    { text: "Etc/GMT-4", id: "Etc/GMT-4" },
    { text: "Etc/GMT-5", id: "Etc/GMT-5" },
    { text: "Etc/GMT-6", id: "Etc/GMT-6" },
    { text: "Etc/GMT-7", id: "Etc/GMT-7" },
    { text: "Etc/GMT-8", id: "Etc/GMT-8" },
    { text: "Etc/GMT-9", id: "Etc/GMT-9" },
    { text: "Etc/GMT+0 (GMT)", id: "Etc/GMT+0" },
    { text: "Etc/GMT+1", id: "Etc/GMT+1" },
    { text: "Etc/GMT+10", id: "Etc/GMT+10" },
    { text: "Etc/GMT+11", id: "Etc/GMT+11" },
    { text: "Etc/GMT+12", id: "Etc/GMT+12" },
    { text: "Etc/GMT+2", id: "Etc/GMT+2" },
    { text: "Etc/GMT+3", id: "Etc/GMT+3" },
    { text: "Etc/GMT+4", id: "Etc/GMT+4" },
    { text: "Etc/GMT+5", id: "Etc/GMT+5" },
    { text: "Etc/GMT+6", id: "Etc/GMT+6" },
    { text: "Etc/GMT+7", id: "Etc/GMT+7" },
    { text: "Etc/GMT+8", id: "Etc/GMT+8" },
    { text: "Etc/GMT+9", id: "Etc/GMT+9" },
    { text: "Etc/GMT0 (GMT)", id: "Etc/GMT0" },
    { text: "Etc/Universal (UTC)", id: "Etc/Universal" },
    { text: "Etc/Greenwich (GMT)", id: "Etc/Greenwich" },
    { text: "Etc/UCT (UCT)", id: "Etc/UCT" },
    { text: "Etc/UTC (UTC)", id: "Etc/UTC" },
    { text: "Etc/Zulu (UTC)", id: "Etc/Zulu" },
    { text: "Europe/Amsterdam (CEST)", id: "Europe/Amsterdam" },
    { text: "Europe/Andorra (CEST)", id: "Europe/Andorra" },
    { text: "Europe/Athens (EEST)", id: "Europe/Athens" },
    { text: "Europe/Belfast (BST)", id: "Europe/Belfast" },
    { text: "Europe/Belgrade (CEST)", id: "Europe/Belgrade" },
    { text: "Europe/Berlin (CEST)", id: "Europe/Berlin" },
    { text: "Europe/Bratislava (CEST)", id: "Europe/Bratislava" },
    { text: "Europe/Brussels (CEST)", id: "Europe/Brussels" },
    { text: "Europe/Astrakhan", id: "Europe/Astrakhan" },
    { text: "Europe/Bucharest (EEST)", id: "Europe/Bucharest" },
    { text: "Europe/Busingen (CEST)", id: "Europe/Busingen" },
    { text: "Europe/Budapest (CEST)", id: "Europe/Budapest" },
    { text: "Europe/Chisinau (EEST)", id: "Europe/Chisinau" },
    { text: "Europe/Copenhagen (CEST)", id: "Europe/Copenhagen" },
    { text: "Europe/Dublin (IST)", id: "Europe/Dublin" },
    { text: "Europe/Gibraltar (CEST)", id: "Europe/Gibraltar" },
    { text: "Europe/Guernsey (BST)", id: "Europe/Guernsey" },
    { text: "Europe/Helsinki (EEST)", id: "Europe/Helsinki" },
    { text: "Europe/Isle_of_Man (BST)", id: "Europe/Isle_of_Man" },
    { text: "Europe/Istanbul (EET)", id: "Europe/Istanbul" },
    { text: "Europe/Jersey (BST)", id: "Europe/Jersey" },
    { text: "Europe/Kaliningrad (EET)", id: "Europe/Kaliningrad" },
    { text: "Europe/Kiev (EEST)", id: "Europe/Kiev" },
    { text: "Europe/Kirov (MSK)", id: "Europe/Kirov" },
    { text: "Europe/Lisbon (WEST)", id: "Europe/Lisbon" },
    { text: "Europe/Ljubljana (CEST)", id: "Europe/Ljubljana" },
    { text: "Europe/London (BST)", id: "Europe/London" },
    { text: "Europe/Luxembourg (CEST)", id: "Europe/Luxembourg" },
    { text: "Europe/Madrid (CEST)", id: "Europe/Madrid" },
    { text: "Europe/Malta (CEST)", id: "Europe/Malta" },
    { text: "Europe/Mariehamn (EEST)", id: "Europe/Mariehamn" },
    { text: "Europe/Minsk (MSK)", id: "Europe/Minsk" },
    { text: "Europe/Monaco (CEST)", id: "Europe/Monaco" },
    { text: "Europe/Moscow (MSK)", id: "Europe/Moscow" },
    { text: "Europe/Nicosia (EEST)", id: "Europe/Nicosia" },
    { text: "Europe/Oslo (CEST)", id: "Europe/Oslo" },
    { text: "Europe/Paris (CEST)", id: "Europe/Paris" },
    { text: "Europe/Podgorica (CEST)", id: "Europe/Podgorica" },
    { text: "Europe/Prague (CEST)", id: "Europe/Prague" },
    { text: "Europe/Riga (EEST)", id: "Europe/Riga" },
    { text: "Europe/Rome (CEST)", id: "Europe/Rome" },
    { text: "Europe/Samara (SAMT)", id: "Europe/Samara" },
    { text: "Europe/San_Marino (CEST)", id: "Europe/San_Marino" },
    { text: "Europe/Sarajevo (CEST)", id: "Europe/Sarajevo" },
    { text: "Europe/Saratov", id: "Europe/Saratov" },
    { text: "Europe/Simferopol (MSK)", id: "Europe/Simferopol" },
    { text: "Europe/Skopje (CEST)", id: "Europe/Skopje" },
    { text: "Europe/Sofia (EEST)", id: "Europe/Sofia" },
    { text: "Europe/Stockholm (CEST)", id: "Europe/Stockholm" },
    { text: "Europe/Tallinn (EEST)", id: "Europe/Tallinn" },
    { text: "Europe/Tirane (CEST)", id: "Europe/Tirane" },
    { text: "Europe/Tiraspol (EEST)", id: "Europe/Tiraspol" },
    { text: "Europe/Ulyanovsk", id: "Europe/Ulyanovsk" },
    { text: "Europe/Uzhgorod (EEST)", id: "Europe/Uzhgorod" },
    { text: "Europe/Vaduz (CEST)", id: "Europe/Vaduz" },
    { text: "Europe/Vatican (CEST)", id: "Europe/Vatican" },
    { text: "Europe/Vienna (CEST)", id: "Europe/Vienna" },
    { text: "Europe/Vilnius (EEST)", id: "Europe/Vilnius" },
    { text: "Europe/Volgograd (MSK)", id: "Europe/Volgograd" },
    { text: "Europe/Warsaw (CEST)", id: "Europe/Warsaw" },
    { text: "Europe/Zagreb (CEST)", id: "Europe/Zagreb" },
    { text: "Europe/Zaporozhye (EEST)", id: "Europe/Zaporozhye" },
    { text: "Europe/Zurich (CEST)", id: "Europe/Zurich" },
    { text: "GB (BST)", id: "GB" },
    { text: "GB-Eire (BST)", id: "GB-Eire" },
    { text: "GMT (GMT)", id: "GMT" },
    { text: "GMT0 (GMT)", id: "GMT0" },
    { text: "Greenwich (GMT)", id: "Greenwich" },
    { text: "Hongkong (HKT)", id: "Hongkong" },
    { text: "HST (HST)", id: "HST" },
    { text: "Iceland (GMT)", id: "Iceland" },
    { text: "Indian/Antananarivo (EAT)", id: "Indian/Antananarivo" },
    { text: "Indian/Chagos (IOT)", id: "Indian/Chagos" },
    { text: "Indian/Christmas (CXT)", id: "Indian/Christmas" },
    { text: "Indian/Cocos (CCT)", id: "Indian/Cocos" },
    { text: "Indian/Comoro (EAT)", id: "Indian/Comoro" },
    { text: "Indian/Kerguelen (TFT)", id: "Indian/Kerguelen" },
    { text: "Indian/Mahe (SCT)", id: "Indian/Mahe" },
    { text: "Indian/Maldives (MVT)", id: "Indian/Maldives" },
    { text: "Indian/Mauritius (MUT)", id: "Indian/Mauritius" },
    { text: "Indian/Mayotte (EAT)", id: "Indian/Mayotte" },
    { text: "Indian/Reunion (RET)", id: "Indian/Reunion" },
    { text: "Iran (IRT)", id: "Iran" },
    { text: "Israel (IDT)", id: "Israel" },
    { text: "Jamaica (EST)", id: "Jamaica" },
    { text: "Japan (JST)", id: "Japan" },
    { text: "Kwajalein (MHT)", id: "Kwajalein" },
    { text: "Libya (EET)", id: "Libya" },
    { text: "MET (MEST)", id: "MET" },
    { text: "Mexico/BajaNorte (PDT)", id: "Mexico/BajaNorte" },
    { text: "Mexico/BajaSur (MDT)", id: "Mexico/BajaSur" },
    { text: "Mexico/General (CDT)", id: "Mexico/General" },
    { text: "MST (MST)", id: "MST" },
    { text: "MST7MDT (MDT)", id: "MST7MDT" },
    { text: "Navajo (MDT)", id: "Navajo" },
    { text: "NZ (NZST)", id: "NZ" },
    { text: "NZ-CHAT (CHAT)", id: "NZ-CHAT" },
    { text: "Pacific/Apia (WSDT)", id: "Pacific/Apia" },
    { text: "Pacific/Auckland (NZST)", id: "Pacific/Auckland" },
    { text: "Pacific/Bougainville (BT)", id: "Pacific/Bougainville" },
    { text: "Pacific/Chatham (CHADT)", id: "Pacific/Chatham" },
    { text: "Pacific/Chuuk (CHUT)", id: "Pacific/Chuuk" },
    { text: "Pacific/Easter (EAST)", id: "Pacific/Easter" },
    { text: "Pacific/Efate (VUT)", id: "Pacific/Efate" },
    { text: "Pacific/Enderbury (PHOT)", id: "Pacific/Enderbury" },
    { text: "Pacific/Fakaofo (TKT)", id: "Pacific/Fakaofo" },
    { text: "Pacific/Fiji (FJST)", id: "Pacific/Fiji" },
    { text: "Pacific/Funafuti (TVT)", id: "Pacific/Funafuti" },
    { text: "Pacific/Galapagos (GALT)", id: "Pacific/Galapagos" },
    { text: "Pacific/Gambier (GAMT)", id: "Pacific/Gambier" },
    { text: "Pacific/Guadalcanal (SBT)", id: "Pacific/Guadalcanal" },
    { text: "Pacific/Guam (ChST)", id: "Pacific/Guam" },
    { text: "Pacific/Honolulu (HST)", id: "Pacific/Honolulu" },
    { text: "Pacific/Johnston (HST)", id: "Pacific/Johnston" },
    { text: "Pacific/Kiritimati (LINT)", id: "Pacific/Kiritimati" },
    { text: "Pacific/Kosrae (KOST)", id: "Pacific/Kosrae" },
    { text: "Pacific/Kwajalein (MHT)", id: "Pacific/Kwajalein" },
    { text: "Pacific/Majuro (MHT)", id: "Pacific/Majuro" },
    { text: "Pacific/Marquesas (MART)", id: "Pacific/Marquesas" },
    { text: "Pacific/Midway (SST)", id: "Pacific/Midway" },
    { text: "Pacific/Nauru (NRT)", id: "Pacific/Nauru" },
    { text: "Pacific/Niue (NUT)", id: "Pacific/Niue" },
    { text: "Pacific/Norfolk (NFT)", id: "Pacific/Norfolk" },
    { text: "Pacific/Noumea (NCT)", id: "Pacific/Noumea" },
    { text: "Pacific/Pago_Pago (SST)", id: "Pacific/Pago_Pago" },
    { text: "Pacific/Palau (PWT)", id: "Pacific/Palau" },
    { text: "Pacific/Pitcairn (PST)", id: "Pacific/Pitcairn" },
    { text: "Pacific/Pohnpei (PONT)", id: "Pacific/Pohnpei" },
    { text: "Pacific/Ponape (PONT)", id: "Pacific/Ponape" },
    { text: "Pacific/Port_Moresby (PGT)", id: "Pacific/Port_Moresby" },
    { text: "Pacific/Rarotonga (CKT)", id: "Pacific/Rarotonga" },
    { text: "Pacific/Saipan (ChST)", id: "Pacific/Saipan" },
    { text: "Pacific/Samoa (SST)", id: "Pacific/Samoa" },
    { text: "Pacific/Tahiti (TAHT)", id: "Pacific/Tahiti" },
    { text: "Pacific/Tarawa (GILT)", id: "Pacific/Tarawa" },
    { text: "Pacific/Tongatapu (TOT)", id: "Pacific/Tongatapu" },
    { text: "Pacific/Truk (CHUT)", id: "Pacific/Truk" },
    { text: "Pacific/Wake (WAKT)", id: "Pacific/Wake" },
    { text: "Pacific/Wallis (WFT)", id: "Pacific/Wallis" },
    { text: "Pacific/Yap (CHUT)", id: "Pacific/Yap" },
    { text: "Poland (CEST)", id: "Poland" },
    { text: "Portugal (WEST)", id: "Portugal" },
    { text: "PRC (CST)", id: "PRC" },
    { text: "PST8PDT (PDT)", id: "PST8PDT" },
    { text: "ROK (KST)", id: "ROK" },
    { text: "Singapore (SGT)", id: "Singapore" },
    { text: "Turkey (EET)", id: "Turkey" },
    { text: "UCT (UCT)", id: "UCT" },
    { text: "Universal (UTC)", id: "Universal" },
    { text: "US/Alaska (AKDT)", id: "US/Alaska" },
    { text: "US/Aleutian (HDT)", id: "US/Aleutian" },
    { text: "US/Arizona (MST)", id: "US/Arizona" },
    { text: "US/Central (CDT)", id: "US/Central" },
    { text: "US/East-Indiana (EDT)", id: "US/East-Indiana" },
    { text: "US/Eastern (EDT)", id: "US/Eastern" },
    { text: "US/Hawaii (HST)", id: "US/Hawaii" },
    { text: "US/Indiana-Starke (CDT)", id: "US/Indiana-Starke" },
    { text: "US/Michigan (EDT)", id: "US/Michigan" },
    { text: "US/Mountain (MDT)", id: "US/Mountain" },
    { text: "US/Pacific (PDT)", id: "US/Pacific" },
    { text: "US/Pacific-New (PDT)", id: "US/Pacific-New" },
    { text: "US/Samoa (SST)", id: "US/Samoa" },
    { text: "UTC (UTC)", id: "UTC" },
    { text: "W-SU (MSK)", id: "W-SU" },
    { text: "WET (WEST)", id: "WET" },
    { text: "Zulu (UTC)", id: "Zulu" }
  ];
  return Timezones;
};

export const showGroupsOrUsersOrRolesForPolicy = (
  showType,
  rawData,
  policyType
) => {
  let itemList = [];
  if (policyType == "0") {
    itemList = [
      "policyItems",
      "allowExceptions",
      "denyPolicyItems",
      "denyExceptions"
    ];
  } else if (policyType == "1") {
    itemList = ["dataMaskPolicyItems"];
  } else {
    itemList = ["rowFilterPolicyItems"];
  }
  let allTypes = new Set();
  for (let item of itemList) {
    if (rawData[item] && rawData[item].length > 0)
      for (const obj of rawData[item]) {
        if (!isEmpty(obj?.[showType])) {
          allTypes = new Set([...allTypes, ...obj[showType]]);
        }
      }
  }
  allTypes = [...allTypes];
  return allTypes;
};

var links = {
  ServiceManager: function (zoneName) {
    return {
      href: "/policymanager/resource",
      text: !isUndefined(zoneName && zoneName.selectedZone)
        ? `Service Manager : ${zoneName && zoneName.selectedZone.label} zone`
        : "Service Manager"
    };
  },
  TagBasedServiceManager: function (zoneName) {
    return {
      href: "/policymanager/tag",
      text: !isUndefined(zoneName && zoneName.selectedZone)
        ? `Service Manager : ${zoneName && zoneName.selectedZone.label} zone`
        : "Service Manager"
    };
  },
  ServiceCreate: {
    href: "",
    text: "Create Service"
  },
  ServiceEdit: function (serviceDetails) {
    return {
      href: `/service/${serviceDetails.serviceDefId}/edit/${serviceDetails.serviceId}  `,
      text: "Edit Service"
    };
  },
  ManagePolicies: function (policy) {
    return {
      href: `/service/${policy.serviceId}/policies/${policy.policyType}`,
      text: `${policy.serviceName} Policies`
    };
  },
  PolicyCreate: {
    href: "",
    text: "Create Policy"
  },
  PolicyEdit: {
    href: "",
    text: "Edit Policy"
  },
  Users: {
    href: "/users/usertab",
    text: "Users/Groups/Roles"
  },
  SecurityZone: function (zoneId) {
    return {
      href: isUndefined(zoneId) ? "/zones/zone/list" : `/zones/zone/${zoneId}`,
      text: "Security Zone"
    };
  },
  ZoneCreate: {
    href: "/zones/create",
    text: "Create Zone"
  },
  ZoneEdit: {
    href: "",
    text: "Edit Zone"
  },
  Kms: function (kms) {
    return {
      href: isUndefined(kms) ? "" : `/kms/keys/edit/manage/${kms.serviceName}`,
      text: `KMS`
    };
  },
  KmsKeyForService: function (serviceDetails) {
    return {
      href: `/service/${serviceDetails.serviceDefId}/edit/${serviceDetails.serviceId}  `,
      text: serviceDetails.serviceName
    };
  },
  KmsKeyCreate: function (kms) {
    return {
      href: `/kms/keys/${kms.serviceName}/create`,
      text: `Key Create`
    };
  },
  UserProfile: {
    href: "",
    text: "User Profile"
  },
  UserCreate: {
    href: "",
    text: "User Create"
  },
  UserEdit: function (userId) {
    return {
      href: `/user/${userId}`,
      text: "User Edit"
    };
  },
  Groups: {
    href: "/users/grouptab",
    text: "Users/Groups/Roles"
  },
  GroupCreate: {
    href: "",
    text: "Group Create"
  },
  GroupEdit: function (userId) {
    return {
      href: `/group/${userId}`,
      text: "Group Edit"
    };
  },
  Roles: {
    href: "/users/roletab",
    text: "Users/Groups/Roles"
  },
  RoleCreate: {
    href: "",
    text: "Role Create"
  },
  RoleEdit: function (roleId) {
    return {
      href: `/role/${roleId}`,
      text: "Role Edit"
    };
  },
  ModulePermissions: {
    href: "/permissions/models",
    text: "Permissions"
  },
  ModulePermissionEdit: function (permissionData) {
    return {
      href: `/permissions/${permissionData.id}/edit`,
      text: permissionData.module
    };
  },
  UserAccessReport: {
    href: "",
    text: "User Access Report"
  }
};

export const commonBreadcrumb = (type, options) => {
  let data = [];
  type?.map((obj) => {
    if (typeof links[obj] == "function") {
      let filterdata = {};
      filterdata[obj] = links[obj](options);
      return data.push(filterdata);
    }
    if (typeof links[obj] != "function") {
      return data.push(pick(links, obj));
    }
  });
  return <CustomBreadcrumb data={data} links={links} type={type} />;
};

/* PolicyListing QuerParams Name */
export const QueryParamsName = (id) => {
  if (id == QueryParams.PolicyListing.id.columnName) {
    return QueryParams.PolicyListing.id.queryParamName;
  }
  if (id == QueryParams.PolicyListing.name.columnName) {
    return QueryParams.PolicyListing.name.queryParamName;
  }
};

/* QueryParams for sorting */
export const getTableSortBy = (sortArr = []) => {
  return sortArr.map(({ id }) => id).join(",");
};

export const getTableSortType = (sortArr = []) => {
  return sortArr.map(({ desc }) => (desc ? "desc" : "asc")).join(",");
};

/* Info icon */
export const InfoIcon = (props) => {
  const { css, position, message } = props;
  return (
    <span className={`${css}`}>
      <CustomTooltip
        placement={`${position}`}
        content={message}
        icon="fa-fw fa fa-info-circle"
      />
    </span>
  );
};

/* Edit Permission Module Infinite Scroll */
export const CustomInfinteScroll = (props) => {
  const { data, removeUsrGrp, scrollableDiv } = props;
  const [count, setCount] = useState({
    startIndex: 0,
    maxItems: 200
  });

  const [current, setCurrent] = useState(
    data.slice(count.startIndex, count.maxItems)
  );

  const getMoreData = () => {
    let newData = current.concat(
      data.slice(count.startIndex + 200, count.maxItems + 200)
    );
    setCurrent(newData);
    setCount({
      startIndex: count.startIndex + 200,
      maxItems: count.maxItems + 200
    });
  };

  const removeData = (value) => {
    removeUsrGrp(value);
  };

  return (
    <div id={scrollableDiv} className="permission-infinite-scroll">
      <InfiniteScroll
        dataLength={current.length}
        next={getMoreData}
        hasMore={true}
        scrollableTarget={scrollableDiv}
      >
        {current.map((obj) => (
          <span className="selected-widget" key={obj.value}>
            <i
              role="button"
              id="button"
              className="icon remove fa-fw fa fa-remove"
              onClick={() => removeData(obj)}
              data-id={obj.value}
              data-cy={obj.value}
            />
            {obj.label}
          </span>
        ))}
      </InfiniteScroll>
    </div>
  );
};

export const fetchSearchFilterParams = (
  auditTabName,
  searchParams,
  searchFilterOptions
) => {
  let finalSearchFilterData = {};
  let searchFilterParam = {};
  let searchParam = {};
  let defaultSearchFilterParam = [];

  // Get search filter params from current search params
  for (const [key, value] of searchParams.entries()) {
    let searchFilterObj = find(searchFilterOptions, {
      urlLabel: key
    });

    if (!isUndefined(searchFilterObj)) {
      let category = searchFilterObj.category;
      let categoryValue = value;

      if (searchFilterObj?.addMultiple) {
        let oldValue = searchFilterParam[category];
        let newValue = value;
        if (oldValue) {
          if (isArray(oldValue)) {
            searchFilterParam[category].push(newValue);
            searchParam[key].push(newValue);
          } else {
            searchFilterParam[category] = [oldValue, newValue];
            searchParam[key] = [oldValue, newValue];
          }
        } else {
          searchFilterParam[category] = newValue;
          searchParam[key] = newValue;
        }
      } else {
        if (searchFilterObj.type == "textoptions") {
          let textOptionObj = find(searchFilterObj.options(), {
            label: categoryValue
          });
          categoryValue = !isUndefined(textOptionObj)
            ? textOptionObj.value
            : categoryValue;
        }

        searchFilterParam[category] = categoryValue;
        searchParam[key] = value;
      }
      defaultSearchFilterParam.push({
        category: category,
        value: categoryValue
      });
    } else {
      searchParam[key] = value;
    }
  }

  // Get search filter params from localStorage
  if (isEmpty(searchFilterParam)) {
    if (!isNull(localStorage.getItem(auditTabName))) {
      const localStorageParams =
        !isEmpty(localStorage.getItem(auditTabName)) &&
        JSON.parse(localStorage.getItem(auditTabName));

      for (const localParam in localStorageParams) {
        let searchFilterObj = find(searchFilterOptions, {
          urlLabel: localParam
        });

        if (!isUndefined(searchFilterObj)) {
          let category = searchFilterObj.category;
          let value = localStorageParams[localParam];

          if (searchFilterObj?.addMultiple) {
            if (isArray(value)) {
              for (const val of value) {
                searchFilterParam[category] = value;
                defaultSearchFilterParam.push({
                  category: category,
                  value: val
                });
                searchParam[localParam] = value;
              }
            } else {
              searchFilterParam[category] = value;
              defaultSearchFilterParam.push({
                category: category,
                value: value
              });
              searchParam[localParam] = value;
            }
          } else {
            if (searchFilterObj.type == "textoptions") {
              let textOptionObj = find(searchFilterObj.options(), {
                label: value
              });
              value = !isUndefined(textOptionObj) ? textOptionObj.value : value;
            }

            searchFilterParam[category] = value;
            defaultSearchFilterParam.push({
              category: category,
              value: value
            });
            searchParam[localParam] = localStorageParams[localParam];
          }
        } else {
          searchParam[localParam] = localStorageParams[localParam];
        }
      }
    }
  }

  finalSearchFilterData["searchFilterParam"] = searchFilterParam;
  finalSearchFilterData["defaultSearchFilterParam"] = defaultSearchFilterParam;
  finalSearchFilterData["searchParam"] = searchParam;

  return finalSearchFilterData;
};

export const parseSearchFilter = (filter, searchFilterOptions) => {
  let finalSearchFilter = {};
  let searchFilterParam = {};
  let searchParam = {};

  map(filter, function (obj) {
    let searchFilterObj = find(searchFilterOptions, {
      category: obj.category
    });

    if (searchFilterObj !== undefined) {
      if (searchFilterObj?.addMultiple) {
        let oldValue = searchFilterParam[obj.category];
        let newValue = obj.value;
        if (oldValue) {
          if (isArray(oldValue)) {
            searchFilterParam[obj.category].push(newValue);
          } else {
            searchFilterParam[obj.category] = [oldValue, newValue];
          }
        } else {
          searchFilterParam[obj.category] = newValue;
        }
      } else {
        searchFilterParam[obj.category] = obj.value;
      }

      let urlLabelParam = searchFilterObj.urlLabel;

      if (searchFilterObj.type == "textoptions") {
        let textOptionObj = find(searchFilterObj.options(), {
          value: obj.value
        });
        searchParam[urlLabelParam] =
          textOptionObj !== undefined ? textOptionObj.label : obj.value;
      } else {
        if (searchFilterObj?.addMultiple) {
          searchParam[urlLabelParam] =
            searchFilterParam[searchFilterObj.category];
        } else {
          searchParam[urlLabelParam] = obj.value;
        }
      }
    }
  });

  finalSearchFilter["searchParam"] = searchParam;
  finalSearchFilter["searchFilterParam"] = searchFilterParam;

  return finalSearchFilter;
};

export const serverError = (error) => {
  if (error.response !== undefined && has(error.response, "data.msgDesc")) {
    toast.error(error.response.data.msgDesc);
  } else if (error.response !== undefined && has(error.response, "data")) {
    toast.error(error.response.data);
  }
};

/* PolicyInfo for masking and row filter */

export const policyInfo = (policyType, serviceType) => {
  if (
    serviceType != "tag" &&
    policyType == RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value
  ) {
    return policyInfoMessage.maskingPolicyInfoMsg;
  } else if (
    serviceType == "tag" &&
    policyType == RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value
  ) {
    return policyInfoMessage.maskingPolicyInfoMsgForTagBased;
  } else if (
    policyType == RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value
  ) {
    return policyInfoMessage.rowFilterPolicyInfoMsg;
  }
};

/* Policy Expire */
export const isPolicyExpired = (policy) => {
  return !some(
    policy.validitySchedules &&
      policy.validitySchedules.map((m) => {
        if (!m.endTime) {
          return true;
        } else if (isEmpty(m.timeZone)) {
          return new Date().valueOf() > new Date(m.endTime).valueOf()
            ? false
            : true;
        } else {
          return new Date(
            moment.tz(m.timeZone).format("MM/DD/YYYY HH:mm:ss")
          ).valueOf() > new Date(m.endTime).valueOf()
            ? false
            : true;
        }
      })
  );
};

export const getBaseUrl = () => {
  if (!window.location.origin) {
    return (
      window.location.protocol +
      "//" +
      window.location.hostname +
      (window.location.port ? ":" + window.location.port : "")
    );
  }

  return (
    window.location.origin +
    window.location.pathname.substring(
      window.location.pathname.lastIndexOf("/") + 1,
      0
    )
  );
};

/* Drag and Drop Feature */

export const dragStart = (e, position, dragItem) => {
  e.target.style.opacity = 0.4;
  e.target.style.backgroundColor = "#fdf1a6";
  e.stopPropagation();
  dragItem.current = position;
};

export const dragEnter = (e, position, dragOverItem) => {
  dragOverItem.current = position;
};

export const dragOver = (e) => {
  e.preventDefault();
};

export const drop = (e, fields, dragItem, dragOverItem) => {
  e.target.style.opacity = 1;
  e.target.style.backgroundColor = "white";
  if (dragItem.current == dragOverItem.current) {
    return;
  }

  fields.move(dragItem.current, dragOverItem.current);

  dragItem.current = null;
  dragOverItem.current = null;
};

// TODO : Remove below code once different router path is used to distinguish between tag and resource service/policy
export const updateTagActive = (isTagView) => {
  if (isTagView) {
    document
      .getElementById("resourcesButton")
      ?.classList?.remove("navbar-active");
    document.getElementById("tagButton")?.classList?.add("navbar-active");
  } else if (!isTagView) {
    document.getElementById("tagButton")?.classList?.remove("navbar-active");
    document.getElementById("resourcesButton")?.classList?.add("navbar-active");
  }
};

export const handleLogout = async (checkKnoxSSOVal, navigate) => {
  try {
    await fetchApi({
      url: "logout",
      baseURL: "",
      headers: {
        "cache-control": "no-cache"
      }
    });
    if (checkKnoxSSOVal !== undefined || checkKnoxSSOVal !== null) {
      if (checkKnoxSSOVal?.toString() == "false") {
        window.location.replace("/locallogin");
        window.localStorage.clear();
      } else {
        navigate("/knoxSSOWarning");
      }
    } else {
      window.location.replace("login.jsp");
    }
  } catch (error) {
    toast.error(`Error occurred while logout! ${error}`);
  }
};

export const checkKnoxSSO = async (navigate) => {
  const userProfile = getUserProfile();
  let checkKnoxSSOresp = {};
  try {
    checkKnoxSSOresp = await fetchApi({
      url: "plugins/checksso",
      type: "GET",
      headers: {
        "cache-control": "no-cache"
      }
    });
    if (
      checkKnoxSSOresp?.data?.toString() == "true" &&
      userProfile?.configProperties?.inactivityTimeout > 0
    ) {
      window.location.replace("index.html?action=timeout");
    } else {
      handleLogout(checkKnoxSSOresp?.data, navigate);
    }
  } catch (error) {
    if (checkKnoxSSOresp?.status == "419") {
      window.location.replace("login.jsp");
    }
    console.error(`Error occurred while logout! ${error}`);
  }
};

export const navigateTo = {
  navigate: null
};

export const requestDataTitle = (serviceType) => {
  let title = "";
  if (serviceType == ServiceType.Service_HIVE.label) {
    title = `Hive Query`;
  }
  if (serviceType == ServiceType.Service_HBASE.label) {
    title = `HBase Audit Data`;
  }
  if (serviceType == ServiceType.Service_HDFS.label) {
    title = `HDFS Operation Name`;
  }
  if (serviceType == ServiceType.Service_SOLR.label) {
    title = "Solr Query";
  }
  return title;
};

//Policy condition evaluation

export const policyConditionUpdatedJSON = (policyCond) => {
  let newPolicyConditionJSON = [...policyCond];
  newPolicyConditionJSON.filter(function (key) {
    if (!key?.uiHint || key?.uiHint == "") {
      if (
        key.evaluatorOptions &&
        key.evaluatorOptions?.["ui.isMultiline"] == "true"
      ) {
        key["uiHint"] = '{ "isMultiline":true }';
      } else {
        key["uiHint"] = '{ "isMultiValue":true }';
      }
    }
  });
  return newPolicyConditionJSON;
};

// Get resources with help of policy type

export const getResourcesDefVal = (serviceDef, policyType) => {
  let resources = [];
  if (RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value == policyType) {
    resources = sortBy(serviceDef.dataMaskDef.resources, "itemId");
  } else if (
    RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value == policyType
  ) {
    resources = sortBy(serviceDef.rowFilterDef.resources, "itemId");
  } else {
    resources = sortBy(serviceDef.resources, "itemId");
  }
  return resources;
};

// Get defult landing page

export const getLandingPageURl = () => {
  if (hasAccessToTab("Resource Based Policies")) {
    return "/policymanager/resource";
  } else {
    if (hasAccessToTab("Tag Based Policies")) {
      return "/policymanager/tag";
    } else {
      return "/userprofile";
    }
  }
};

export const getServiceDefIcon = (serviceDefName) => {
  let imagePath = folderIcon;
  let imageStyling;

  try {
    const serviceDefIcon =
      require(`../images/serviceDefIcons/${serviceDefName}/icon.svg`).default;
    imagePath = serviceDefIcon;
    imageStyling = { height: "27px", width: "27px" };
  } catch (error) {
    console.log(
      `Continuing to use default icon for ${serviceDefName.toUpperCase()}`
    );
  }

  return (
    <span className={imageStyling !== undefined ? "serviceDef-icon m-r-5" : ""}>
      <img
        src={imagePath}
        style={imageStyling !== undefined ? imageStyling : {}}
        className={imageStyling !== undefined ? "" : "m-r-5"}
        alt={`${serviceDefName.toUpperCase()} Icon`}
        title={`${serviceDefName.toUpperCase()}`}
      />
    </span>
  );
};

export const capitalizeFirstLetter = (str) => {
  return str.charAt(0).toUpperCase() + str.slice(1);
};

export const currentTimeZone = (timeZoneDate) => {
  return timeZoneDate
    ? `${dateFormat(timeZoneDate, "mm/dd/yyyy hh:MM:ss TT ")} ${new Date(
        timeZoneDate
      )
        .toString()
        .replace(/^.*GMT.*\(/, "")
        .replace(/\)$/, "")}`
    : new Date()
        .toString()
        .replace(/^.*GMT.*\(/, "")
        .replace(/\)$/, "");
};
