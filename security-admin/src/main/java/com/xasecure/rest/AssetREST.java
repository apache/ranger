package com.xasecure.rest;

import java.io.File;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.xasecure.biz.AssetMgr;
import com.xasecure.biz.XABizUtil;
import com.xasecure.common.PropertiesUtil;
import com.xasecure.common.RESTErrorUtil;
import com.xasecure.common.SearchCriteria;
import com.xasecure.common.StringUtil;
import com.xasecure.common.XACommonEnums;
import com.xasecure.common.XASearchUtil;
import com.xasecure.common.annotation.XAAnnotationClassName;
import com.xasecure.common.annotation.XAAnnotationJSMgrName;
import com.xasecure.service.XAccessAuditService;
import com.xasecure.service.XAgentService;
import com.xasecure.service.XAssetService;
import com.xasecure.service.XCredentialStoreService;
import com.xasecure.service.XPolicyExportAuditService;
import com.xasecure.service.XResourceService;
import com.xasecure.service.XTrxLogService;
import com.xasecure.view.VXAccessAuditList;
import com.xasecure.view.VXAsset;
import com.xasecure.view.VXAssetList;
import com.xasecure.view.VXCredentialStore;
import com.xasecure.view.VXCredentialStoreList;
import com.xasecure.view.VXLong;
import com.xasecure.view.VXPolicyExportAuditList;
import com.xasecure.view.VXResource;
import com.xasecure.view.VXResourceList;
import com.xasecure.view.VXResponse;
import com.xasecure.view.VXString;
import com.xasecure.view.VXStringList;
import com.xasecure.view.VXTrxLogList;

@Path("assets")
@Component
@Scope("request")
@XAAnnotationJSMgrName("AssetMgr")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class AssetREST {
	static Logger logger = Logger.getLogger(AssetREST.class);

	@Autowired
	XASearchUtil searchUtil;

	@Autowired
	AssetMgr assetMgr;

	@Autowired
	XAssetService xAssetService;

	@Autowired
	XResourceService xResourceService;

	@Autowired
	XCredentialStoreService xCredentialStoreService;

	@Autowired
	XAgentService xAgentService;

	@Autowired
	RESTErrorUtil restErrorUtil;
	
	@Autowired
	XPolicyExportAuditService xPolicyExportAudits;
	
	@Autowired
	XTrxLogService xTrxLogService;
	
	@Autowired
	XABizUtil msBizUtil;

	@Autowired
	XAccessAuditService xAccessAuditService;
	
	@GET
	@Path("/assets/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXAsset getXAsset(@PathParam("id") Long id) {
		return assetMgr.getXAsset(id);
	}

	@POST
	@Path("/assets")
	@Produces({ "application/xml", "application/json" })
	public VXAsset createXAsset(VXAsset vXAsset) {
		return assetMgr.createXAsset(vXAsset);
	}

	@PUT
	@Path("/assets/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXAsset updateXAsset(VXAsset vXAsset) {
		return assetMgr.updateXAsset(vXAsset);
	}

	@DELETE
	@Path("/assets/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@XAAnnotationClassName(class_name = VXAsset.class)
	public void deleteXAsset(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		boolean force = true;
		assetMgr.deleteXAsset(id, force);
	}

	@POST
	@Path("/assets/testConfig")
	@Produces({ "application/xml", "application/json" })
	public VXResponse testConfig(VXAsset vXAsset) {
		return assetMgr.testConfig(vXAsset);
	}

	@GET
	@Path("/assets")
	@Produces({ "application/xml", "application/json" })
	@SuppressWarnings("rawtypes")
	public VXAssetList searchXAssets(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xAssetService.sortFields);

		searchUtil.extractIntList(request, searchCriteria, "status", "status",
				"status");
		// searchUtil.extractStringList(request, searchCriteria, "status",
		// "status", "status", null, StringUtil.VALIDATION_TEXT);
		Object status = searchCriteria.getParamValue("status");
		if (status == null || ((Collection) status).size() == 0) {
			ArrayList<Integer> valueList = new ArrayList<Integer>();
			valueList.add(XACommonEnums.STATUS_DISABLED);
			valueList.add(XACommonEnums.STATUS_ENABLED);
			searchCriteria.addParam("status", valueList);
		}
		return assetMgr.searchXAssets(searchCriteria);
	}

	@GET
	@Path("/assets/count")
	@Produces({ "application/xml", "application/json" })
	public VXLong countXAssets(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xAssetService.sortFields);
		return assetMgr.getXAssetSearchCount(searchCriteria);
	}

	@GET
	@Path("/resources/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXResource getXResource(@PathParam("id") Long id) {
		return assetMgr.getXResource(id);
	}

	@POST
	@Path("/resources")
	@Produces({ "application/xml", "application/json" })
	public VXResource createXResource(VXResource vXResource) {
		vXResource=assetMgr.createXResource(vXResource);
		return vXResource;
		
	}
	
	@PUT
	@Path("/resources/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXResource updateXResource(VXResource vXResource) {
		return assetMgr.updateXResource(vXResource);
	}

	@DELETE
	@Path("/resources/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@XAAnnotationClassName(class_name = VXResource.class)
	public void deleteXResource(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		boolean force = false;
		assetMgr.deleteXResource(id, force);
	}

	@GET
	@Path("/resources")
	@Produces({ "application/xml", "application/json" })
	public VXResourceList searchXResources(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xResourceService.sortFields);
		// searchUtil.extractStringList(request, searchCriteria, "name", "Name",
		// "name", null, StringUtil.VALIDATION_TEXT);

		searchUtil.extractString(request, searchCriteria, "name",
				"Resource Path", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "policyName",
				"Policy name", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "columns",
				"Column name", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "columnFamilies",
				"Column Family", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "tables", 
				"Tables", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "udfs", 
				"UDFs", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "databases",
				"Databases", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "groupName",
				"Group Name", StringUtil.VALIDATION_TEXT);
		searchUtil.extractInt(request, searchCriteria, "resourceType",
				"Resource Type");
		searchUtil.extractInt(request, searchCriteria, "assetType",
				"Asset Type");
		searchUtil.extractInt(request, searchCriteria, "isEncrypt",
				"Is Encrypt");
		searchUtil.extractInt(request, searchCriteria, "isRecursive",
				"Is Recursive");
		searchUtil.extractLong(request, searchCriteria, "assetId", "Asset Id");
		searchUtil.extractString(request, searchCriteria, "userName",
				"User Name", StringUtil.VALIDATION_TEXT);

		searchUtil.extractLongList(request, searchCriteria, "userId",
				"User Id", "userId");
		// searchUtil.extractLong(request, searchCriteria, "userId",
		// "User Id");
		// searchUtil.extractLong(request, searchCriteria, "groupId",
		// "Group Id");
		searchUtil.extractLongList(request, searchCriteria, "groupId",
				"Group Id", "groupId");
		
		searchUtil.extractString(request, searchCriteria, "topologies",
				"Topology Name", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "services",
				"Service Name", StringUtil.VALIDATION_TEXT);

		// searchUtil.extractIntList(request, searchCriteria, "status",
		// "status", "status");

		// SearchGroup outerGroup = new SearchGroup(SearchGroup.CONDITION.OR);
		// // Get the search fields for objectClassType and objectId
		// SearchField userId = null;
		// SearchField groupId = null;
		// SearchField resourceId = null;
		// List<SearchField> searchFields = xResourceService.searchFields;
		// for (SearchField searchField : searchFields) {
		// if (searchField.getClientFieldName().equals("userId") &&
		// request.getParameterValues("userId")!=null) {
		// userId = searchField;
		// } else if (searchField.getClientFieldName().equals("groupId") &&
		// request.getParameterValues("groupId")!=null) {
		// groupId = searchField;
		// }else if (searchField.getClientFieldName().equals("name") &&
		// request.getParameterValues("name")!=null) {
		// resourceId = searchField;
		// }
		// }
		// if (groupId != null || userId != null || resourceId != null) {
		// SearchGroup innerGroup = new SearchGroup(SearchGroup.CONDITION.AND);
		// SearchValue searchValue=null;
		// if(userId!=null){
		// searchValue = new SearchValue(userId,
		// searchCriteria.getParamValue("userId"));
		//
		// innerGroup.addValue(searchValue);
		// }
		// if(groupId!=null){
		// searchValue = new SearchValue(groupId,
		// searchCriteria.getParamValue("groupId"));
		// innerGroup.addValue(searchValue);
		// }
		// if(resourceId!=null){
		//
		// searchValue = new SearchValue(resourceId,
		// searchCriteria.getParamValue("name"));
		// innerGroup.addValue(searchValue);
		// }
		//
		// outerGroup.addSearchGroup(innerGroup);
		// searchUtil.addSearchGroup(searchCriteria, outerGroup);
		//
		// }
		searchCriteria.setDistinct(true);

		return assetMgr.searchXResources(searchCriteria);
	}

	@GET
	@Path("/hdfs/resources")
	@Produces({ "application/xml", "application/json" })
	public VXStringList pullHdfsResources(@Context HttpServletRequest request) {
		String dataSourceName = request.getParameter("dataSourceName");
		String baseDir = request.getParameter("baseDirectory");
		return assetMgr.getHdfsResources(dataSourceName, baseDir);
	}

	@GET
	@Path("/hive/resources")
	@Produces({ "application/xml", "application/json" })
	public VXStringList pullHiveResources(@Context HttpServletRequest request) {
		String dataSourceName = request.getParameter("dataSourceName");
		String databaseName = request.getParameter("databaseName");
		String tableName = request.getParameter("tableName");
		String columnName = request.getParameter("columnName");
		return assetMgr.getHiveResources(dataSourceName, databaseName,
				tableName, columnName);
	}

	@GET
	@Path("/hbase/resources")
	@Produces({ "application/xml", "application/json" })
	public VXStringList pullHBaseResources(@Context HttpServletRequest request) {
		String dataSourceName = request.getParameter("dataSourceName");
		String tableName = request.getParameter("tableName");
		String columnFamiles = request.getParameter("columnFamilies");
		return assetMgr.getHBaseResources(dataSourceName, tableName,
				columnFamiles);
	}

	@GET
	@Path("/knox/resources")
	@Produces({ "application/xml", "application/json" })
	public VXStringList pullKnoxResources(@Context HttpServletRequest request) {
		String dataSourceName = request.getParameter("dataSourceName");
		String topologyName = request.getParameter("topologyName");
		String serviceName = request.getParameter("serviceName");		
		return assetMgr.getKnoxResources(dataSourceName, topologyName, serviceName);
	}
	
	@GET
	@Path("/resources/count")
	@Produces({ "application/xml", "application/json" })
	public VXLong countXResources(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xResourceService.sortFields);

		return assetMgr.getXResourceSearchCount(searchCriteria);
	}

	@GET
	@Path("/credstores/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXCredentialStore getXCredentialStore(@PathParam("id") Long id) {
		return assetMgr.getXCredentialStore(id);
	}

	@POST
	@Path("/credstores")
	@Produces({ "application/xml", "application/json" })
	public VXCredentialStore createXCredentialStore(
			VXCredentialStore vXCredentialStore) {
		return assetMgr.createXCredentialStore(vXCredentialStore);
	}

	@PUT
	@Path("/credstores")
	@Produces({ "application/xml", "application/json" })
	public VXCredentialStore updateXCredentialStore(
			VXCredentialStore vXCredentialStore) {
		return assetMgr.updateXCredentialStore(vXCredentialStore);
	}

	@DELETE
	@Path("/credstores/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@XAAnnotationClassName(class_name = VXCredentialStore.class)
	public void deleteXCredentialStore(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		boolean force = false;
		assetMgr.deleteXCredentialStore(id, force);
	}

	@GET
	@Path("/credstores")
	@Produces({ "application/xml", "application/json" })
	public VXCredentialStoreList searchXCredentialStores(
			@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xCredentialStoreService.sortFields);
		return assetMgr.searchXCredentialStores(searchCriteria);
	}

	@GET
	@Path("/credstores/count")
	@Produces({ "application/xml", "application/json" })
	public VXLong countXCredentialStores(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xCredentialStoreService.sortFields);
		return assetMgr.getXCredentialStoreSearchCount(searchCriteria);
	}

	@GET
	@Path("/resource/{id}")
	public Response getXResourceFile(@Context HttpServletRequest request,
			@PathParam("id") Long id) {
		String fileType = searchUtil.extractString(request,
				new SearchCriteria(), "fileType", "File type",
				StringUtil.VALIDATION_TEXT);

		File file = assetMgr.getXResourceFile(id, fileType);
		return Response
				.ok(file, MediaType.APPLICATION_OCTET_STREAM)
				.header("Content-Disposition",
						"attachment;filename=" + file.getName()).build();
	}

	@GET
	@Path("/policyList/{repository}")
	@Encoded
	public String getResourceJSON(@Context HttpServletRequest request,
			@PathParam("repository") String repository) {
		
		boolean httpEnabled = PropertiesUtil.getBooleanProperty("http.enabled",true);
		String epoch = request.getParameter("epoch");

		X509Certificate[] certchain = (X509Certificate[]) request.getAttribute(
				"javax.servlet.request.X509Certificate");
		
		String ipAddress = request.getHeader("X-FORWARDED-FOR");  
		if (ipAddress == null) {  
			ipAddress = request.getRemoteAddr();
		}

		boolean isSecure = request.isSecure();
		
		String policyCount = request.getParameter("policyCount");
		String agentId = request.getParameter("agentId");
		
//		File file = assetMgr.getLatestRepoPolicy(repository, 
//				certchain, httpEnabled, epoch, ipAddress, isSecure, policyCount, agentId);
		

//		return Response
//				.ok(file, MediaType.APPLICATION_OCTET_STREAM)
//				.header("Content-Disposition",
//						"attachment;filename=" + file.getName()).build();
		
		String file = assetMgr.getLatestRepoPolicy(repository, 
				certchain, httpEnabled, epoch, ipAddress, isSecure, policyCount, agentId);
		
		return file;
	}

	@GET
	@Path("/exportAudit")
	@Produces({ "application/xml", "application/json" })
	public VXPolicyExportAuditList searchXPolicyExportAudits(
			@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xPolicyExportAudits.sortFields);
		searchUtil.extractString(request, searchCriteria, "agentId", 
				"The XA agent id pulling the policies.", 
				StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "clientIP", 
				"The XA agent ip pulling the policies.",
				StringUtil.VALIDATION_TEXT);		
		searchUtil.extractString(request, searchCriteria, "repositoryName", 
				"Repository name for which export was done.",
				StringUtil.VALIDATION_TEXT);
		searchUtil.extractInt(request, searchCriteria, "httpRetCode", 
				"HTTP response code for exported policy.");
		searchUtil.extractDate(request, searchCriteria, "startDate", 
				"Start date for search", null);
		searchUtil.extractDate(request, searchCriteria, "endDate", 
				"End date for search", null);
		return assetMgr.searchXPolicyExportAudits(searchCriteria);
	}

	@GET
	@Path("/report")
	@Produces({ "application/xml", "application/json" })
	public VXTrxLogList getReportLogs(@Context HttpServletRequest request){
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xTrxLogService.sortFields);
		searchUtil.extractInt(request, searchCriteria, "objectClassType", "Class type for report.");
		searchUtil.extractString(request, searchCriteria, "attributeName", 
				"Attribute Name", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "action", 
				"CRUD Action Type", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "sessionId", 
				"Session Id", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "owner", 
				"Owner", StringUtil.VALIDATION_TEXT);
		searchUtil.extractDate(request, searchCriteria, "startDate", "Trasaction date since", "MM/dd/yyyy");
		searchUtil.extractDate(request, searchCriteria, "endDate", "Trasaction date till", "MM/dd/yyyy");
		return assetMgr.getReportLogs(searchCriteria);
	}
	
	@GET
	@Path("/report/{transactionId}")
	@Produces({ "application/xml", "application/json" })
	public VXTrxLogList getTransactionReport(@Context HttpServletRequest request, 
			@PathParam("transactionId") String transactionId){
		return assetMgr.getTransactionReport(transactionId);
	}
	
	@GET
	@Path("/accessAudit")
	@Produces({ "application/xml", "application/json" })
	public VXAccessAuditList getAccessLogs(@Context HttpServletRequest request){
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xAccessAuditService.sortFields);
		searchUtil.extractString(request, searchCriteria, "accessType",
				"Access Type", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "aclEnforcer",
				"Access Type", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "agentId",
				"Access Type", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "repoName",
				"Access Type", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "sessionId",
				"Access Type", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "requestUser",
				"Access Type", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "requestData",
				"Access Type", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "resourcePath",
				"Access Type", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "clientIP",
				"Client IP", StringUtil.VALIDATION_TEXT);

		searchUtil.extractInt(request, searchCriteria, "auditType", "Audit Type");
		searchUtil.extractInt(request, searchCriteria, "accessResult", "Audit Type");
		searchUtil.extractInt(request, searchCriteria, "assetId", "Audit Type");
		searchUtil.extractLong(request, searchCriteria, "policyId", "Audit Type");
		searchUtil.extractInt(request, searchCriteria, "repoType", "Audit Type");
		
		searchUtil.extractDate(request, searchCriteria, "startDate",
				"startDate", "MM/dd/yyyy");
		searchUtil.extractDate(request, searchCriteria, "endDate", "endDate",
				"MM/dd/yyyy");
		return assetMgr.getAccessLogs(searchCriteria);
	}		
}
