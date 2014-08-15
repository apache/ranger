package com.xasecure.authorization.hive.authorizer;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivObjectActionType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.security.UserGroupInformation;

import com.xasecure.audit.model.EnumRepositoryType;
import com.xasecure.audit.model.HiveAuditEvent;
import com.xasecure.audit.provider.AuditProviderFactory;
import com.xasecure.authorization.hadoop.config.XaSecureConfiguration;
import com.xasecure.authorization.hadoop.constants.XaSecureHadoopConstants;
import com.xasecure.authorization.hive.XaHiveAccessContext;
import com.xasecure.authorization.hive.XaHiveAccessVerifier;
import com.xasecure.authorization.hive.XaHiveAccessVerifierFactory;
import com.xasecure.authorization.hive.XaHiveObjectAccessInfo;
import com.xasecure.authorization.hive.XaHiveObjectAccessInfo.HiveAccessType;
import com.xasecure.authorization.hive.XaHiveObjectAccessInfo.HiveObjectType;
import com.xasecure.authorization.utils.StringUtil;

public class XaSecureHiveAuthorizer extends XaSecureHiveAuthorizerBase {
	private static final Log LOG = LogFactory.getLog(XaSecureHiveAuthorizer.class) ; 

	private static final String XaSecureModuleName =  XaSecureConfiguration.getInstance().get(XaSecureHadoopConstants.AUDITLOG_XASECURE_MODULE_ACL_NAME_PROP , XaSecureHadoopConstants.DEFAULT_XASECURE_MODULE_ACL_NAME) ;
	private static final String repositoryName     = XaSecureConfiguration.getInstance().get(XaSecureHadoopConstants.AUDITLOG_REPOSITORY_NAME_PROP);

	private XaHiveAccessVerifier mHiveAccessVerifier = null ;


	public XaSecureHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
								  HiveConf                   hiveConf,
								  HiveAuthenticationProvider hiveAuthenticator) {
		super(metastoreClientFactory, hiveConf, hiveAuthenticator);

		LOG.debug("XaSecureHiveAuthorizer.XaSecureHiveAuthorizer()");

		mHiveAccessVerifier = XaHiveAccessVerifierFactory.getInstance() ;
	}


	@Override
	public void checkPrivileges(HiveOperationType         hiveOpType,
								List<HivePrivilegeObject> inputHObjs,
							    List<HivePrivilegeObject> outputHObjs,
							    HiveAuthzContext          context)
		      throws HiveAuthzPluginException, HiveAccessControlException {

		if(LOG.isDebugEnabled()) {
			LOG.debug(toString(hiveOpType, inputHObjs, outputHObjs, context));
		}

		UserGroupInformation ugi =  this.getCurrentUserGroupInfo();

		List<XaHiveObjectAccessInfo> objAccessList = getObjectAccessInfo(hiveOpType, inputHObjs, outputHObjs, context);

		for(XaHiveObjectAccessInfo objAccessInfo : objAccessList) {
            boolean ret = false;

            if(objAccessInfo.getObjectType() == HiveObjectType.URI) {
                ret = isURIAccessAllowed(ugi, objAccessInfo.getAccessType(), objAccessInfo.getUri(), getHiveConf());
            } else {
                ret = mHiveAccessVerifier.isAccessAllowed(ugi, objAccessInfo);
            }

			if(! ret) {
				if(mHiveAccessVerifier.isAudited(objAccessInfo)) {
					logAuditEvent(ugi, objAccessInfo, false);
				}
				
				String deniedObjectName = objAccessInfo.getDeinedObjectName();
				
				if(StringUtil.isEmpty(deniedObjectName)) {
					deniedObjectName = objAccessInfo.getObjectName();
				}

				throw new HiveAccessControlException(String.format("Permission denied: user [%s] does not have [%s] privilege on [%s]",
													 ugi.getShortUserName(), objAccessInfo.getAccessType().name(), deniedObjectName));
			}
		}

		// access is allowed; audit all accesses
		for(XaHiveObjectAccessInfo objAccessInfo : objAccessList) {
			if(mHiveAccessVerifier.isAudited(objAccessInfo)) {
				logAuditEvent(ugi, objAccessInfo, true);
			}
		}
	}
	
	private List<XaHiveObjectAccessInfo> getObjectAccessInfo(HiveOperationType         hiveOpType,
														   List<HivePrivilegeObject> inputsHObjs,
														   List<HivePrivilegeObject> outputHObjs,
														   HiveAuthzContext          context) {
		List<XaHiveObjectAccessInfo> ret = new ArrayList<XaHiveObjectAccessInfo>();

		if(inputsHObjs != null) {
			for(HivePrivilegeObject hiveObj : inputsHObjs) {
				XaHiveObjectAccessInfo hiveAccessObj = getObjectAccessInfo(hiveOpType, hiveObj, context, true);
				
				if(hiveAccessObj != null && !ret.contains(hiveAccessObj)) {
					ret.add(hiveAccessObj);
				}
			}
		}

		if(outputHObjs != null) {
			for(HivePrivilegeObject hiveObj : outputHObjs) {
				XaHiveObjectAccessInfo hiveAccessObj = getObjectAccessInfo(hiveOpType, hiveObj, context, false);
				
				if(hiveAccessObj != null && !ret.contains(hiveAccessObj)) {
					ret.add(hiveAccessObj);
				}
			}
		}

		if(ret.size() == 0 && LOG.isDebugEnabled()) {
			LOG.debug("getObjectAccessInfo(): no objects found for access check! " + toString(hiveOpType, inputsHObjs, outputHObjs, context));
		}
		
		return ret;
	}

	private XaHiveObjectAccessInfo getObjectAccessInfo(HiveOperationType hiveOpType, HivePrivilegeObject hiveObj, HiveAuthzContext context, boolean isInput) {
		XaHiveObjectAccessInfo ret = null;

		HiveObjectType objectType = getObjectType(hiveObj, hiveOpType);
		HiveAccessType accessType = getAccessType(hiveObj, hiveOpType, isInput);
		String         operType   = hiveOpType.name();
		
		XaHiveAccessContext hiveContext = new XaHiveAccessContext(context.getIpAddress(), context.getClientType().name(), context.getCommandString(), context.getSessionString());

		switch(objectType) {
			case DATABASE:
				ret = new XaHiveObjectAccessInfo(operType, hiveContext, accessType, hiveObj.getDbname());
			break;
	
			case TABLE:
				ret = new XaHiveObjectAccessInfo(operType, hiveContext, accessType, hiveObj.getDbname(), HiveObjectType.TABLE, hiveObj.getObjectName());
			break;
	
			case VIEW:
				ret = new XaHiveObjectAccessInfo(operType, hiveContext, accessType, hiveObj.getDbname(), HiveObjectType.VIEW, hiveObj.getObjectName());
			break;
	
			case PARTITION:
				ret = new XaHiveObjectAccessInfo(operType, hiveContext, accessType, hiveObj.getDbname(), HiveObjectType.PARTITION, hiveObj.getObjectName());
			break;
	
			case INDEX:
				String indexName = "?"; // TODO:
				ret = new XaHiveObjectAccessInfo(operType, hiveContext, accessType, hiveObj.getDbname(), hiveObj.getObjectName(), HiveObjectType.INDEX, indexName);
			break;
	
			case COLUMN:
				ret = new XaHiveObjectAccessInfo(operType, hiveContext, accessType, hiveObj.getDbname(), hiveObj.getObjectName(), hiveObj.getColumns());
			break;

			case FUNCTION:
				ret = new XaHiveObjectAccessInfo(operType, hiveContext, accessType, hiveObj.getDbname(), HiveObjectType.FUNCTION, hiveObj.getObjectName());
			break;

            case URI:
                ret = new XaHiveObjectAccessInfo(operType, hiveContext, accessType, HiveObjectType.URI, hiveObj.getObjectName());
            break;

			case NONE:
			break;
		}

		return ret;
	}

	private HiveObjectType getObjectType(HivePrivilegeObject hiveObj, HiveOperationType hiveOpType) {
		HiveObjectType objType = HiveObjectType.NONE;

		switch(hiveObj.getType()) {
			case DATABASE:
				objType = HiveObjectType.DATABASE;
			break;

			case PARTITION:
				objType = HiveObjectType.PARTITION;
			break;

			case TABLE_OR_VIEW:
				String hiveOpTypeName = hiveOpType.name().toLowerCase();
				if(hiveOpTypeName.contains("index")) {
					objType = HiveObjectType.INDEX;
				} else if(! StringUtil.isEmpty(hiveObj.getColumns())) {
					objType = HiveObjectType.COLUMN;
				} else if(hiveOpTypeName.contains("view")) {
					objType = HiveObjectType.VIEW;
				} else {
					objType = HiveObjectType.TABLE;
				}
			break;

			case FUNCTION:
				objType = HiveObjectType.FUNCTION;
			break;

			case DFS_URI:
			case LOCAL_URI:
                objType = HiveObjectType.URI;
            break;

			case COMMAND_PARAMS:
			case GLOBAL:
			break;

			case COLUMN:
				// Thejas: this value is unused in Hive; the case should not be hit.
			break;
		}

		return objType;
	}
	
	private HiveAccessType getAccessType(HivePrivilegeObject hiveObj, HiveOperationType hiveOpType, boolean isInput) {
		HiveAccessType           accessType       = HiveAccessType.NONE;
		HivePrivObjectActionType objectActionType = hiveObj.getActionType();
		
		if(objectActionType == HivePrivObjectActionType.INSERT ||
		   objectActionType == HivePrivObjectActionType.INSERT_OVERWRITE) {
			accessType = HiveAccessType.INSERT;
		} else {
			switch(hiveOpType) {
				case CREATEDATABASE:
					if(hiveObj.getType() == HivePrivilegeObjectType.DATABASE) {
						accessType = HiveAccessType.CREATE;
					}
				break;

				case CREATEFUNCTION:
					if(hiveObj.getType() == HivePrivilegeObjectType.FUNCTION) {
						accessType = HiveAccessType.CREATE;
					}
				break;

				case CREATETABLE:
				case CREATEVIEW:
				case CREATETABLE_AS_SELECT:
					if(hiveObj.getType() == HivePrivilegeObjectType.TABLE_OR_VIEW) {
						accessType = isInput ? HiveAccessType.SELECT : HiveAccessType.CREATE;
					}
				break;

				case ALTERDATABASE:
				case ALTERDATABASE_OWNER:
				case ALTERINDEX_PROPS:
				case ALTERINDEX_REBUILD:
				case ALTERPARTITION_BUCKETNUM:
				case ALTERPARTITION_FILEFORMAT:
				case ALTERPARTITION_LOCATION:
				case ALTERPARTITION_MERGEFILES:
				case ALTERPARTITION_PROTECTMODE:
				case ALTERPARTITION_SERDEPROPERTIES:
				case ALTERPARTITION_SERIALIZER:
				case ALTERTABLE_ADDCOLS:
				case ALTERTABLE_ADDPARTS:
				case ALTERTABLE_ARCHIVE:
				case ALTERTABLE_BUCKETNUM:
				case ALTERTABLE_CLUSTER_SORT:
				case ALTERTABLE_COMPACT:
				case ALTERTABLE_DROPPARTS:
				case ALTERTABLE_FILEFORMAT:
				case ALTERTABLE_LOCATION:
				case ALTERTABLE_MERGEFILES:
				case ALTERTABLE_PARTCOLTYPE:
				case ALTERTABLE_PROPERTIES:
				case ALTERTABLE_PROTECTMODE:
				case ALTERTABLE_RENAME:
				case ALTERTABLE_RENAMECOL:
				case ALTERTABLE_RENAMEPART:
				case ALTERTABLE_REPLACECOLS:
				case ALTERTABLE_SERDEPROPERTIES:
				case ALTERTABLE_SERIALIZER:
				case ALTERTABLE_SKEWED:
				case ALTERTABLE_TOUCH:
				case ALTERTABLE_UNARCHIVE:
				case ALTERTBLPART_SKEWED_LOCATION:
				case ALTERVIEW_PROPERTIES:
				case ALTERVIEW_RENAME:
				case DROPVIEW_PROPERTIES:
					accessType = HiveAccessType.ALTER;
				break;

				case DELETE:
				case DROPFUNCTION:
				case DROPINDEX:
				case DROPTABLE:
				case DROPVIEW:
				case DROPDATABASE:
					accessType = HiveAccessType.DROP;
				break;

				case CREATEINDEX:
					accessType = HiveAccessType.INDEX;
				break;

				case IMPORT:
				case EXPORT:
				case LOAD:
					accessType = isInput ? HiveAccessType.SELECT : HiveAccessType.INSERT;
				break;

				case LOCKDB:
				case LOCKTABLE:
				case UNLOCKDB:
				case UNLOCKTABLE:
					accessType = HiveAccessType.LOCK;
				break;

				case QUERY:
					accessType = HiveAccessType.SELECT;
				break;

				case SWITCHDATABASE:
					accessType = HiveAccessType.USE;
				break;

				case TRUNCATETABLE:
					accessType = HiveAccessType.UPDATE;
				break;

				case ADD:
				case ANALYZE_TABLE:
				case COMPILE:
				case CREATEMACRO:
				case CREATEROLE:
				case DESCDATABASE:
				case DESCFUNCTION:
				case DESCTABLE:
				case DFS:
				case DROPMACRO:
				case DROPROLE:
				case EXPLAIN:
				case GRANT_PRIVILEGE:
				case GRANT_ROLE:
				case MSCK:
				case REVOKE_PRIVILEGE:
				case REVOKE_ROLE:
				case RESET:
				case SET:
				case SHOWCOLUMNS:
				case SHOWCONF:
				case SHOWDATABASES:
				case SHOWFUNCTIONS:
				case SHOWINDEXES:
				case SHOWLOCKS:
				case SHOWPARTITIONS:
				case SHOWTABLES:
				case SHOW_COMPACTIONS:
				case SHOW_CREATETABLE:
				case SHOW_GRANT:
				case SHOW_ROLES:
				case SHOW_ROLE_GRANT:
				case SHOW_ROLE_PRINCIPALS:
				case SHOW_TABLESTATUS:
				case SHOW_TBLPROPERTIES:
				case SHOW_TRANSACTIONS:
				break;
			}
		}
		
		return accessType;
	}

    private boolean isURIAccessAllowed(UserGroupInformation ugi, HiveAccessType accessType, String uri, HiveConf conf) {
        boolean ret = false;

        FsAction action = FsAction.NONE;

        switch(accessType) {
            case ALTER:
            case CREATE:
            case UPDATE:
            case DROP:
            case INDEX:
            case INSERT:
            case LOCK:
                action = FsAction.WRITE;
            break;

            case SELECT:
            case USE:
                action = FsAction.READ;
            break;

            case NONE:
            break;
        }

        if(action == FsAction.NONE) {
            ret = true;
        } else {
            try {
                Path       filePath   = new Path(uri);
                FileSystem fs         = FileSystem.get(filePath.toUri(), conf);
                Path       path       = FileUtils.getPathOrParentThatExists(fs, filePath);
                FileStatus fileStatus = fs.getFileStatus(path);
                String     userName   = ugi.getShortUserName();

                if (FileUtils.isOwnerOfFileHierarchy(fs, fileStatus, userName)) {
                    ret = true;
                } else {
                    ret = FileUtils.isActionPermittedForFileHierarchy(fs, fileStatus, userName, action);
                }
            } catch(Exception excp) {
                LOG.error("Error getting permissions for " + uri, excp);
            }
        }

        return ret;
    }
	private void logAuditEvent(UserGroupInformation ugi, XaHiveObjectAccessInfo objAccessInfo, boolean accessGranted) {
		
		HiveAuditEvent auditEvent = new HiveAuditEvent();

		try {
			auditEvent.setAclEnforcer(XaSecureModuleName);
			auditEvent.setSessionId(objAccessInfo.getContext().getSessionString());
			auditEvent.setResourceType("@" + StringUtil.toLower(objAccessInfo.getObjectType().name())); // to be consistent with earlier release
			auditEvent.setAccessType(objAccessInfo.getAccessType().toString());
			auditEvent.setAction(objAccessInfo.getOperType());
			auditEvent.setUser(ugi.getShortUserName());
			auditEvent.setAccessResult((short)(accessGranted ? 1 : 0));
			auditEvent.setClientIP(objAccessInfo.getContext().getClientIpAddress());
			auditEvent.setClientType(objAccessInfo.getContext().getClientType());
			auditEvent.setEventTime(StringUtil.getUTCDate());
			auditEvent.setRepositoryType(EnumRepositoryType.HIVE);
			auditEvent.setRepositoryName(repositoryName) ;
			auditEvent.setRequestData(objAccessInfo.getContext().getCommandString());

			if(! accessGranted && !StringUtil.isEmpty(objAccessInfo.getDeinedObjectName())) {
				auditEvent.setResourcePath(objAccessInfo.getDeinedObjectName());
			} else {
				auditEvent.setResourcePath(objAccessInfo.getObjectName());
			}
		
			if(LOG.isDebugEnabled()) {
				LOG.debug("logAuditEvent [" + auditEvent + "] - START");
			}

			AuditProviderFactory.getAuditProvider().log(auditEvent);

			if(LOG.isDebugEnabled()) {
				LOG.debug("logAuditEvent [" + auditEvent + "] - END");
			}
		}
		catch(Throwable t) {
			LOG.error("ERROR logEvent [" + auditEvent + "]", t);
		}
		
	}
	
	private String toString(HiveOperationType         hiveOpType,
							List<HivePrivilegeObject> inputHObjs,
							List<HivePrivilegeObject> outputHObjs,
							HiveAuthzContext          context) {
		StringBuilder sb = new StringBuilder();
		
		sb.append("'checkPrivileges':{");
		sb.append("'hiveOpType':").append(hiveOpType);

		sb.append(", 'inputHObjs':[");
		toString(inputHObjs, sb);
		sb.append("]");

		sb.append(", 'outputHObjs':[");
		toString(outputHObjs, sb);
		sb.append("]");

		sb.append(", 'context':{");
		sb.append("'clientType':").append(context.getClientType());
		sb.append(", 'commandString':").append(context.getCommandString());
		sb.append(", 'ipAddress':").append(context.getIpAddress());
		sb.append(", 'sessionString':").append(context.getSessionString());
		sb.append("}");

		sb.append(", 'user':").append(this.getCurrentUserGroupInfo().getUserName());
		sb.append(", 'groups':[").append(StringUtil.toString(this.getCurrentUserGroupInfo().getGroupNames())).append("]");

		sb.append("}");

		return sb.toString();
	}

	private StringBuilder toString(List<HivePrivilegeObject> privObjs, StringBuilder sb) {
		if(privObjs != null && privObjs.size() > 0) {
			toString(privObjs.get(0), sb);
			for(int i = 1; i < privObjs.size(); i++) {
				sb.append(",");
				toString(privObjs.get(i), sb);
			}
		}
		
		return sb;
	}

	private StringBuilder toString(HivePrivilegeObject privObj, StringBuilder sb) {
		sb.append("'HivePrivilegeObject':{");
		sb.append("'type':").append(privObj.getType().toString());
		sb.append(", 'dbName':").append(privObj.getDbname());
		sb.append(", 'objectType':").append(privObj.getType());
		sb.append(", 'objectName':").append(privObj.getObjectName());
		sb.append(", 'columns':[").append(StringUtil.toString(privObj.getColumns())).append("]");
		sb.append(", 'partKeys':[").append(StringUtil.toString(privObj.getPartKeys())).append("]");
		sb.append(", 'commandParams':[").append(StringUtil.toString(privObj.getCommandParams())).append("]");
		sb.append(", 'actionType':").append(privObj.getActionType().toString());
		sb.append("}");

		return sb;
	}
}
