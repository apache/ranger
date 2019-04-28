/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.authorization.spark.authorizer

import java.util.{List => JList}

import org.apache.commons.lang.StringUtils
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.security.authorization.plugin._
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.{HivePrivilegeObjectType, HivePrivObjectActionType}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.ranger.authorization.spark.authorizer.SparkAccessType.SparkAccessType
import org.apache.ranger.authorization.spark.authorizer.SparkObjectType.SparkObjectType
import org.apache.ranger.authorization.utils.StringUtil
import org.apache.ranger.plugin.policyengine.RangerAccessRequest
import org.apache.ranger.plugin.util.RangerPerfTracer

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class RangerSparkAuthorizer(
    metastoreClientFactory: HiveMetastoreClientFactory,
    hiveConf: HiveConf,
    hiveAuthenticator: HiveAuthenticationProvider,
    sessionContext: HiveAuthzSessionContext) extends HiveAuthorizer {

  import RangerSparkAuthorizer._
  private val mUgi = if (hiveAuthenticator == null) {
    null
  } else {
    Option(hiveAuthenticator.getUserName).map(UserGroupInformation.createRemoteUser).orNull
  }

  private val sparkPlugin = RangerSparkPlugin.build().getOrCreate()

  override def getVersion: HiveAuthorizer.VERSION = HiveAuthorizer.VERSION.V1

  override def checkPrivileges(
      hiveOpType: HiveOperationType,
      inputsHObjs: JList[SparkPrivilegeObject],
      outputHObjs: JList[SparkPrivilegeObject],
      context: HiveAuthzContext): Unit = {

    if (mUgi == null) {
      throw new HiveAccessControlException("Permission denied: user information not available")
    }
    val user = mUgi.getShortUserName
    val groups = mUgi.getGroupNames.toSet.asJava
    val auditHandler = new RangerSparkAuditHandler
    val perf = if (RangerPerfTracer.isPerfTraceEnabled(PERF_SPARKAUTH_REQUEST_LOG)) {
      RangerPerfTracer.getPerfTracer(PERF_SPARKAUTH_REQUEST_LOG,
        "RangerSparkAuthorizer.checkPrivileges()")
    } else {
      null
    }
    try {
      val requests = new ArrayBuffer[RangerSparkAccessRequest]()
      if (inputsHObjs.isEmpty && hiveOpType == HiveOperationType.SHOWDATABASES) {
        val resource = new RangerSparkResource(SparkObjectType.DATABASE, None)
        requests += new RangerSparkAccessRequest(resource, user, groups, hiveOpType.name,
          SparkAccessType.USE, context, sessionContext, sparkPlugin.getClusterName)
      }

      def addAccessRequest(objs: JList[SparkPrivilegeObject], isInput: Boolean): Unit = {
        objs.asScala.foreach { obj =>
          val resource = getSparkResource(obj, hiveOpType)
          if (resource != null) {
            val objectName = obj.getObjectName
            val objectType = resource.getObjectType
            if (objectType == SparkObjectType.URI && isPathInFSScheme(objectName)) {
              val fsAction = getURIAccessType(hiveOpType)
              if (!isURIAccessAllowed(user, fsAction, objectName, hiveConf)) {
                throw new HiveAccessControlException(s"Permission denied: user [$user] does not" +
                  s" have [${fsAction.name}] privilege on [$objectName]")
              }
            } else {
              val accessType = getAccessType(obj, hiveOpType, objectType, isInput)
              if (accessType != SparkAccessType.NONE && !requests.exists(
                o => o.getSparkAccessType == accessType && o.getResource == resource)) {
                requests += new RangerSparkAccessRequest(resource, user, groups, hiveOpType,
                  accessType, context, sessionContext, sparkPlugin.getClusterName)
              }
            }
          }
        }
      }

      addAccessRequest(inputsHObjs, isInput = true)
      addAccessRequest(outputHObjs, isInput = false)
      requests.foreach { request =>
        val resource = request.getResource.asInstanceOf[RangerSparkResource]
        if (resource.getObjectType == SparkObjectType.COLUMN &&
          StringUtils.contains(resource.getColumn, ",")) {
          resource.setServiceDef(sparkPlugin.getServiceDef)
          val colReqs: JList[RangerAccessRequest] = resource.getColumn.split(",")
            .filter(StringUtils.isNotBlank).map { c =>
            val colRes = new RangerSparkResource(SparkObjectType.COLUMN,
              Option(resource.getDatabase), resource.getTable, c)
            val colReq = request.copy()
            colReq.setResource(colRes)
            colReq.asInstanceOf[RangerAccessRequest]
          }.toList.asJava
          val colResults = sparkPlugin.isAccessAllowed(colReqs, auditHandler)
          if (colResults != null) {
            for (c <- colResults.asScala) {
              if (c != null && !c.getIsAllowed) {
                throw new HiveAccessControlException(s"Permission denied: user [$user] does not" +
                  s" have [${request.getSparkAccessType}] privilege on [${resource.getAsString}]")
              }
            }
          }
        } else {
          val result = sparkPlugin.isAccessAllowed(request, auditHandler)
          if (result != null && !result.getIsAllowed) {
            throw new HiveAccessControlException(s"Permission denied: user [$user] does not" +
              s" have [${request.getSparkAccessType}] privilege on [${resource.getAsString}]")
          }
        }
      }
    } finally {
      // TODO（Kent Yao) add auditHandler.flush()
      RangerPerfTracer.log(perf)
    }
  }

  override def filterListCmdObjects(
      listObjs: JList[SparkPrivilegeObject],
      context: HiveAuthzContext): JList[SparkPrivilegeObject] = {
    if (LOG.isDebugEnabled) LOG.debug(s"==> filterListCmdObjects($listObjs, $context)")

    val perf = if (RangerPerfTracer.isPerfTraceEnabled(PERF_SPARKAUTH_REQUEST_LOG)) {
      RangerPerfTracer.getPerfTracer(PERF_SPARKAUTH_REQUEST_LOG,
        "RangerSparkAuthorizer.filterListCmdObjects()")
    } else {
      null
    }

    val ret =
      if (listObjs == null) {
        LOG.debug("filterListCmdObjects: meta objects list was null!")
        null
      } else if (listObjs.isEmpty) {
        listObjs
      } else if (mUgi == null) {
        LOG.warn("filterListCmdObjects: user information not available")
        listObjs
      } else {
        if (LOG.isDebugEnabled) {
          LOG.debug(s"filterListCmdObjects: number of input objects[${listObjs.size}]")
        }
        val user = mUgi.getShortUserName
        val groups = mUgi.getGroupNames.toSet.asJava
        if (LOG.isDebugEnabled) {
          LOG.debug(s"filterListCmdObjects: user[$user], groups[$groups]")
        }

        listObjs.asScala.filter { obj =>
          if (LOG.isDebugEnabled) {
            LOG.debug(s"filterListCmdObjects: actionType[${obj.getActionType}]," +
              s" objectType[${obj.getType}], objectName[${obj.getObjectName}]," +
              s" dbName[${obj.getDbname}], columns[${obj.getColumns}]," +
              s" partitionKeys[${obj.getPartKeys}];" +
              s" context: commandString[${Option(context).map(_.getCommandString).getOrElse("")}" +
              s"], ipAddress[${Option(context).map(_.getIpAddress).getOrElse("")}]")
          }
          createSparkResource(obj) match {
            case Some(resource) =>
              val request = new RangerSparkAccessRequest(
                resource, user, groups, context, sessionContext, sparkPlugin.getClusterName)
              val result = sparkPlugin.isAccessAllowed(request)
              if (request == null) {
                LOG.error("filterListCmdObjects: Internal error: null RangerAccessResult object" +
                  " received back from isAccessAllowed()")
                false
              } else if (!result.getIsAllowed) {
                if (LOG.isDebugEnabled) {
                  val path = resource.getAsString
                  LOG.debug(s"filterListCmdObjects: Permission denied: user [$user] does not have" +
                    s" [${request.getSparkAccessType}] privilege on [$path]. resource[$resource]," +
                    s" request[$request], result[$result]")
                }
                false
              } else {
                true
              }
            case _ =>
              LOG.error("filterListCmdObjects: RangerSparkResource returned by createHiveResource" +
                " is null")
              false
          }
        }.asJava
      }
    RangerPerfTracer.log(perf)
    ret
  }

  def getSparkResource(
      hiveObj: SparkPrivilegeObject,
      hiveOpType: HiveOperationType): RangerSparkResource = {
    import SparkObjectType._
    val objectType = getObjectType(hiveObj, hiveOpType)
    val resource = objectType match {
      case DATABASE => RangerSparkResource(objectType, Option(hiveObj.getDbname))
      case TABLE | VIEW | PARTITION | FUNCTION =>
        RangerSparkResource(objectType, Option(hiveObj.getDbname), hiveObj.getObjectName)
      case COLUMN =>
        RangerSparkResource(objectType, Option(hiveObj.getDbname), hiveObj.getObjectName,
          StringUtils.join(hiveObj.getColumns, ","))
      case URI => RangerSparkResource(objectType, Option(hiveObj.getObjectName))
      case _ => null
    }
    if (resource != null) resource.setServiceDef(sparkPlugin.getServiceDef)
    resource
  }

  private def isPathInFSScheme(objectName: String): Boolean = {
    objectName.nonEmpty && sparkPlugin.fsScheme.exists(objectName.startsWith)
  }


  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Spark SQL supports no Hive DCLs, remain the functions with a default <empty> implementation //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  override def grantPrivileges(
      hivePrincipals: JList[HivePrincipal],
      hivePrivileges: JList[HivePrivilege],
      hivePrivObject: SparkPrivilegeObject,
      grantorPrincipal: HivePrincipal,
      grantOption: Boolean): Unit = {}

  override def revokePrivileges(hivePrincipals: JList[HivePrincipal],
      hivePrivileges: JList[HivePrivilege],
      hivePrivObject: SparkPrivilegeObject,
      grantorPrincipal: HivePrincipal,
      grantOption: Boolean): Unit = {}

  override def createRole(roleName: String, adminGrantor: HivePrincipal): Unit = {}

  override def dropRole(roleName: String): Unit = {}

  override def getPrincipalGrantInfoForRole(roleName: String): JList[HiveRoleGrant] = {
    Seq.empty.asJava
  }

  override def getRoleGrantInfoForPrincipal(principal: HivePrincipal): JList[HiveRoleGrant] = {
    Seq.empty.asJava
  }

  override def grantRole(
      hivePrincipals: JList[HivePrincipal],
      roles: JList[String],
      grantOption: Boolean,
      grantorPrinc: HivePrincipal): Unit = {}

  override def revokeRole(
      hivePrincipals: JList[HivePrincipal],
      roles: JList[String],
      grantOption: Boolean,
      grantorPrinc: HivePrincipal): Unit = {}

  override def getAllRoles: JList[String] = Seq.empty.asJava

  override def showPrivileges(
      principal: HivePrincipal,
      privObj: SparkPrivilegeObject): JList[HivePrivilegeInfo] = Seq.empty.asJava

  override def setCurrentRole(roleName: String): Unit = {}

  override def getCurrentRoleNames: JList[String] = Seq.empty.asJava

  override def applyAuthorizationConfigPolicy(hiveConf: HiveConf): Unit = {}
}

object RangerSparkAuthorizer {
  import HivePrivilegeObjectType._

  private val LOG = LogFactory.getLog(classOf[RangerSparkAuthorizer].getSimpleName.stripSuffix("$"))

  private val PERF_SPARKAUTH_REQUEST_LOG = RangerPerfTracer.getPerfLogger("sparkauth.request")

  def createSparkResource(privilegeObject: SparkPrivilegeObject): Option[RangerSparkResource] = {
    val objectName = privilegeObject.getObjectName
    val dbName = privilegeObject.getDbname
    val objectType = privilegeObject.getType
    objectType match {
      case DATABASE =>
        Some(RangerSparkResource(SparkObjectType.DATABASE, Option(objectName)))
      case TABLE_OR_VIEW =>
        Some(RangerSparkResource(SparkObjectType.DATABASE, Option(dbName), objectName))
      case _ =>
        LOG.warn(s"RangerSparkAuthorizer.createHiveResource: unexpected objectType: $objectType")
        None
    }
  }

  private def getObjectType(
      hiveObj: SparkPrivilegeObject,
      hiveOpType: HiveOperationType): SparkObjectType = hiveObj.getType match {
    case DATABASE | null => SparkObjectType.DATABASE
    case PARTITION => SparkObjectType.PARTITION
    case TABLE_OR_VIEW if hiveOpType.name.toLowerCase.contains("view") => SparkObjectType.VIEW
    case TABLE_OR_VIEW => SparkObjectType.TABLE
    case FUNCTION => SparkObjectType.FUNCTION
    case DFS_URI | LOCAL_URI => SparkObjectType.URI
    case _ => SparkObjectType.NONE
  }

  private def getURIAccessType(hiveOpType: HiveOperationType): FsAction = {
    import HiveOperationType._

    hiveOpType match {
      case LOAD | IMPORT => FsAction.READ
      case EXPORT => FsAction.WRITE
      case CREATEDATABASE | CREATETABLE | CREATETABLE_AS_SELECT | ALTERDATABASE |
           ALTERDATABASE_OWNER | ALTERTABLE_ADDCOLS | ALTERTABLE_REPLACECOLS |
           ALTERTABLE_RENAMECOL | ALTERTABLE_RENAMEPART | ALTERTABLE_RENAME |
           ALTERTABLE_DROPPARTS | ALTERTABLE_ADDPARTS | ALTERTABLE_TOUCH |
           ALTERTABLE_ARCHIVE | ALTERTABLE_UNARCHIVE | ALTERTABLE_PROPERTIES |
           ALTERTABLE_SERIALIZER | ALTERTABLE_PARTCOLTYPE | ALTERTABLE_SERDEPROPERTIES |
           ALTERTABLE_CLUSTER_SORT | ALTERTABLE_BUCKETNUM | ALTERTABLE_UPDATETABLESTATS |
           ALTERTABLE_UPDATEPARTSTATS | ALTERTABLE_PROTECTMODE | ALTERTABLE_FILEFORMAT |
           ALTERTABLE_LOCATION | ALTERINDEX_PROPS | ALTERTABLE_MERGEFILES | ALTERTABLE_SKEWED |
           ALTERTABLE_COMPACT | ALTERPARTITION_SERIALIZER | ALTERPARTITION_SERIALIZER |
           ALTERPARTITION_SERDEPROPERTIES | ALTERPARTITION_BUCKETNUM | ALTERPARTITION_PROTECTMODE |
           ALTERPARTITION_FILEFORMAT | ALTERPARTITION_LOCATION | ALTERPARTITION_MERGEFILES |
           ALTERTBLPART_SKEWED_LOCATION | QUERY => FsAction.ALL
      case _ => FsAction.NONE
    }
  }

  private def isURIAccessAllowed(
      userName: String, action: FsAction, uri: String, conf: HiveConf): Boolean = action match {
    case FsAction.NONE => true
    case _ =>
      try {
        val filePath = new Path(uri)
        val fs = FileSystem.get(filePath.toUri, conf)
        val fileStat = fs.globStatus(filePath)
        if (fileStat != null && fileStat.nonEmpty) fileStat.forall { file =>
          FileUtils.isOwnerOfFileHierarchy(fs, file, userName) ||
            FileUtils.isActionPermittedForFileHierarchy(fs, file, userName, action)
        } else {
          val file = FileUtils.getPathOrParentThatExists(fs, filePath)
          FileUtils.checkFileAccessWithImpersonation(fs, file, action, userName)
          true
        }
      } catch {
        case e: Exception =>
          LOG.error("Error getting permissions for " + uri, e)
          false
      }
  }

  private def getAccessType(
      hiveObj: SparkPrivilegeObject,
      hiveOpType: HiveOperationType,
      sparkObjectType: SparkObjectType,
      isInput: Boolean): SparkAccessType = {
    sparkObjectType match {
      case SparkObjectType.URI if isInput => SparkAccessType.READ
      case SparkObjectType.URI => SparkAccessType.WRITE
      case _ => hiveObj.getActionType match {
        case HivePrivObjectActionType.INSERT | HivePrivObjectActionType.INSERT_OVERWRITE |
             HivePrivObjectActionType.UPDATE | HivePrivObjectActionType.DELETE =>
          SparkAccessType.UPDATE
        case HivePrivObjectActionType.OTHER =>
          import HiveOperationType._
          hiveOpType match {
            case CREATEDATABASE if hiveObj.getType == HivePrivilegeObjectType.DATABASE =>
              SparkAccessType.CREATE
            case CREATEFUNCTION if hiveObj.getType == HivePrivilegeObjectType.FUNCTION =>
              SparkAccessType.CREATE
            case CREATETABLE | CREATEVIEW | CREATETABLE_AS_SELECT
              if hiveObj.getType == HivePrivilegeObjectType.TABLE_OR_VIEW =>
              if (isInput) SparkAccessType.SELECT else SparkAccessType.CREATE
            case ALTERDATABASE | ALTERDATABASE_OWNER | ALTERINDEX_PROPS | ALTERINDEX_REBUILD |
                 ALTERPARTITION_BUCKETNUM | ALTERPARTITION_FILEFORMAT | ALTERPARTITION_LOCATION |
                 ALTERPARTITION_MERGEFILES | ALTERPARTITION_PROTECTMODE |
                 ALTERPARTITION_SERDEPROPERTIES | ALTERPARTITION_SERIALIZER | ALTERTABLE_ADDCOLS |
                 ALTERTABLE_ADDPARTS | ALTERTABLE_ARCHIVE | ALTERTABLE_BUCKETNUM |
                 ALTERTABLE_CLUSTER_SORT | ALTERTABLE_COMPACT | ALTERTABLE_DROPPARTS |
                 ALTERTABLE_FILEFORMAT | ALTERTABLE_LOCATION | ALTERTABLE_MERGEFILES |
                 ALTERTABLE_PARTCOLTYPE | ALTERTABLE_PROPERTIES | ALTERTABLE_PROTECTMODE |
                 ALTERTABLE_RENAME | ALTERTABLE_RENAMECOL | ALTERTABLE_RENAMEPART |
                 ALTERTABLE_REPLACECOLS | ALTERTABLE_SERDEPROPERTIES | ALTERTABLE_SERIALIZER |
                 ALTERTABLE_SKEWED | ALTERTABLE_TOUCH | ALTERTABLE_UNARCHIVE |
                 ALTERTABLE_UPDATEPARTSTATS | ALTERTABLE_UPDATETABLESTATS |
                 ALTERTBLPART_SKEWED_LOCATION | ALTERVIEW_AS | ALTERVIEW_PROPERTIES |
                 ALTERVIEW_RENAME | DROPVIEW_PROPERTIES | MSCK => SparkAccessType.ALTER
            case DROPFUNCTION | DROPINDEX | DROPTABLE | DROPVIEW | DROPDATABASE =>
              SparkAccessType.DROP
            case IMPORT => if (isInput) SparkAccessType.SELECT else SparkAccessType.CREATE
            case EXPORT | LOAD => if (isInput) SparkAccessType.SELECT else SparkAccessType.UPDATE
            case QUERY | SHOW_TABLESTATUS | SHOW_CREATETABLE | SHOWINDEXES | SHOWPARTITIONS |
                 SHOW_TBLPROPERTIES | ANALYZE_TABLE => SparkAccessType.SELECT
            case SHOWCOLUMNS | DESCTABLE =>
              StringUtil.toLower(RangerSparkPlugin.showColumnsOption) match {
                case "show-all" => SparkAccessType.USE
                case _ => SparkAccessType.SELECT
              }
            case SHOWDATABASES | SWITCHDATABASE | DESCDATABASE| SHOWTABLES => SparkAccessType.USE
            case TRUNCATETABLE => SparkAccessType.UPDATE
            case _ => SparkAccessType.NONE
          }
      }
    }
  }
}
