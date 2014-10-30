### Licensed to the Apache Software Foundation (ASF) under one or more
### contributor license agreements.  See the NOTICE file distributed with
### this work for additional information regarding copyright ownership.
### The ASF licenses this file to You under the Apache License, Version 2.0
### (the "License"); you may not use this file except in compliance with
### the License.  You may obtain a copy of the License at
###
###     http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.

###
### Install script that can be used to install Ranger
### To invoke the scipt, run the following command from PowerShell:
###   install.ps1 -username <username> -password <password> or
###   install.ps1 -credentialFilePath <credentialFilePath>
###
### where:
###   <username> and <password> represent account credentials used to run
###   Ranger services as Windows services.
###   <credentialFilePath> encripted credentials file path
###
### By default, Hadoop is installed to "C:\Hadoop". To change this set
### HADOOP_NODE_INSTALL_ROOT environment variable to a location were
### you'd like Hadoop installed.
###
### Script pre-requisites:
###   JAVA_HOME must be set to point to a valid Java location.
###   HADOOP_HOME must be set to point to a valid Hadoop install location.
###
### To uninstall previously installed Single-Node cluster run:
###   uninstall.ps1
###
### NOTE: Notice @version@ strings throughout the file. First compile
### winpkg with "ant winpkg", that will replace the version string.

###

param(
    [String]
    [Parameter( ParameterSetName='UsernamePassword', Position=0, Mandatory=$true )]
    [Parameter( ParameterSetName='UsernamePasswordBase64', Position=0, Mandatory=$true )]
    $username,
    [String]
    [Parameter( ParameterSetName='UsernamePassword', Position=1, Mandatory=$true )]
    $password,
    [String]
    [Parameter( ParameterSetName='UsernamePasswordBase64', Position=1, Mandatory=$true )]
    $passwordBase64,
    [Parameter( ParameterSetName='CredentialFilePath', Mandatory=$true )]
    $credentialFilePath,
    [String]
    $roles
    )

function Main( $scriptDir )
{
    $FinalName = "ranger-@ranger.version@"
    if ( -not (Test-Path ENV:WINPKG_LOG))
    {
        $ENV:WINPKG_LOG = "$FinalName.winpkg.log"
    }

    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"
    $nodeInstallRoot = "$ENV:HADOOP_NODE_INSTALL_ROOT"


    ###
    ### Create the Credential object from the given username and password or the provided credentials file
    ###
    $serviceCredential = Get-HadoopUserCredentials -credentialsHash @{"username" = $username; "password" = $password; `
        "passwordBase64" = $passwordBase64; "credentialFilePath" = $credentialFilePath}
    $username = $serviceCredential.UserName
    Write-Log "Username: $username"
    Write-Log "CredentialFilePath: $credentialFilePath"

    ###
    ### Install and Configure ranger (Looks like this config will come from earlier HDP installation steps )
    ###
    if ( $ENV:RANGER -eq "yes" ) {
      $roles = "ranger" # TODO!!
    }

    Write-Log "Roles are $roles"
    Install "ranger" $nodeInstallRoot $serviceCredential $roles
    Configure "ranger" $nodeInstallRoot $serviceCredential
    Write-Log "Installation of Ranger Admin Tool completed successfully"

    ####################################################################
    ###			Install and Configure ranger-hdfs plugin               ###
    ####################################################################

    $roles = ' '

    Install "ranger-hdfs" $nodeInstallRoot $serviceCredential $roles
    ###
    ### Apply configuration changes to hdfs-site.xml
    ###
	$hdfsChanges = @{
		"dfs.permissions.enabled" = "true"
		"dfs.permissions" = "true"
	}
    ###
    ### Apply configuration changes to xasecure-audit.xml
    ###
	$hdfsAuditChanges = @{
		"xasecure.audit.db.is.enabled"                          = "true"
        "xasecure.audit.jpa.javax.persistence.jdbc.url"			= "jdbc:mysql://${ENV:RANGER_AUDIT_DB_HOST}:${ENV:RANGER_AUDIT_DB_PORT}/${ENV:RANGER_AUDIT_DB_DBNAME}"
		"xasecure.audit.jpa.javax.persistence.jdbc.user"		= "${ENV:RANGER_AUDIT_DB_USERNAME}"
		"xasecure.audit.jpa.javax.persistence.jdbc.password"	= "crypted"
		"xasecure.audit.repository.name"						= "${ENV:RANGER_HDFS_REPO}"
		"xasecure.audit.credential.provider.file"				= "jceks://file/${ENV:RANGER_HDFS_CRED_KEYSTORE_FILE}"
		"xasecure.audit.jpa.javax.persistence.jdbc.driver"		= "com.mysql.jdbc.Driver"
        "xasecure.audit.hdfs.is.enabled"                        = "false"
		"xasecure.audit.hdfs.config.destination.directroy"      = "${ENV:RANGER_HDFS_DESTINATION_DIRECTORY}"
		"xasecure.audit.hdfs.config.destination.file"           = "${ENV:RANGER_HDFS_DESTINTATION_FILE}"
		"xasecure.audit.hdfs.config.destination.flush.interval.seconds"= "{ENV:RANGER_HDFS_DESTINTATION_FLUSH_INTERVAL_SECONDS}"
		"xasecure.audit.hdfs.config.destination.rollover.interval.seconds"= "{ENV:RANGER_HDFS_DESTINTATION_ROLLOVER_INTERVAL_SECONDS}"
		"xasecure.audit.hdfs.config.destination.open.retry.interval.seconds"= "{ENV:RANGER_HDFS_DESTINTATION_OPEN_RETRY_INTERVAL_SECONDS}"
		"xasecure.audit.hdfs.config.local.buffer.directroy"     =  "{ENV:RANGER.HDFS_LOCAL_BUFFER_DIRECTORY}"
		"xasecure.audit.hdfs.config.local.buffer.file"          =  "{ENV:RANGER.HDFS_LOCAL_BUFFER_FILE}"
		"xasecure.audit.hdfs.config.local.buffer.flush.interval.seconds"= "{ENV:RANGER_HDFS_LOCAL_BUFFER_FLUSH_INTERVAL_SECONDS}"
		"xasecure.audit.hdfs.config.local.buffer.rollover.interval.seconds"= "{ENV:RANGER_HDFS_LOCAL_BUFFER_ROLLOVER_INTERVAL_SECONDS}"
		"xasecure.audit.hdfs.config.local.archive.directroy"      = "{ENV:RANGER_HDFS_LOCAL_ARCHIVE_DIRECTORY}"
		"xasecure.audit.hdfs.config.local.archive.max.file.count" =  "{ENV:RANGER_HDFS_LOCAL_ARCHIVE_MAX_FILE_COUNT}"

	}
    ###
    ### Apply configuration changes to xasecure-hdfs-security.xml
    ###
	$hdfsSecurityChanges = @{
		"hdfs.authorization.verifier.classname"					= "com.xasecure.pdp.hdfs.XASecureAuthorizer"
		"xasecure.hdfs.policymgr.url"							= "${ENV:RANGER_EXTERNAL_URL}/service/assets/policyList/${ENV:RANGER_HDFS_REPO}"
		"xasecure.hdfs.policymgr.url.saveAsFile"				= "${ENV:RANGER_HOME}\tmp\hadoop_${ENV:RANGER_HDFS_REPO}"
		"xasecure.hdfs.policymgr.url.laststoredfile"			= "${ENV:RANGER_HOME}\tmp\hadoop_${ENV:RANGER_HDFS_REPO}_json"
		"xasecure.hdfs.policymgr.url.reloadIntervalInMillis"	= "30000"
	}

	### Since we modify different files, this hashtable contains hashtables for
	### each files. So its a hashtable of hashtables!
	$configs = @{}
	$configs.Add("hdfsChanges",$hdfsChanges)
	$configs.Add("hdfsAuditChanges",$hdfsAuditChanges)
	$configs.Add("hdfsSecurityChanges",$hdfsSecurityChanges)

    Configure "ranger-hdfs" $nodeInstallRoot $serviceCredential $configs

    Write-Log "Installation of ranger-hdfs completed successfully"


    ####################################################################
    ###			Install and Configure ranger-hive plugin               ###
    ####################################################################

    $roles = ' '
    Install "ranger-hive" $nodeInstallRoot $serviceCredential $roles

    ####
    #### Apply configuration changes to hive-site.xml
    ####
	$hivechanges =   @{
		"hive.security.authorization.enabled"	= "true"
		"hive.security.authorization.manager"	= "com.xasecure.authorization.hive.authorizer.XaSecureHiveAuthorizerFactory"
		"hive.conf.restricted.list"				= "hive.security.authorization.enabled, hive.security.authorization.manager, hive.security.authenticator.manager"
	}

    ####
    #### Apply configuration changes to hiveserver2-site.xml
    ####
    #$xmlFile = Join-Path $ENV:HIVE_CONF_DIR "hiveserver2-site.xml"
	$hiveServerChanges =  @{
		"hive.security.authorization.enabled"	= "true"
		"hive.security.authorization.manager"	= "com.xasecure.authorization.hive.authorizer.XaSecureHiveAuthorizerFactory"
		"hive.security.authenticator.manager"	= "org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator"
		"hive.conf.restricted.list"				= "hive.security.authorization.enabled, hive.security.authorization.manager, hive.security.authenticator.manager"
	}

    ####
    #### Apply configuration changes to xasecure-audit.xml
    ####
    #$xmlFile = Join-Path $ENV:HIVE_CONF_DIR "xasecure-audit.xml"
    $hiveAuditChanges = @{
        "xasecure.audit.db.is.enabled"                          = "true"
        "xasecure.audit.jpa.javax.persistence.jdbc.url"			= "jdbc:mysql://${ENV:RANGER_AUDIT_DB_HOST}:${ENV:RANGER_AUDIT_DB_PORT}/${ENV:RANGER_AUDIT_DB_DBNAME}"
		"xasecure.audit.jpa.javax.persistence.jdbc.user"		= "${ENV:RANGER_AUDIT_DB_USERNAME}"
		"xasecure.audit.jpa.javax.persistence.jdbc.password"	= "crypted"
		"xasecure.audit.repository.name"						= "${ENV:RANGER_HIVE_REPO}"
		"xasecure.audit.credential.provider.file"				= "jceks://file/${ENV:RANGER_HIVE_CRED_KEYSTORE_FILE}"
	"xasecure.audit.jpa.javax.persistence.jdbc.driver"		= "com.mysql.jdbc.Driver"
        "xasecure.audit.hdfs.is.enabled"                        = "false"
		"xasecure.audit.hdfs.config.destination.directroy"      = "${ENV:RANGER_HDFS_DESTINATION_DIRECTORY}"
		"xasecure.audit.hdfs.config.destination.file"           = "${ENV:RANGER_HDFS_DESTINTATION_FILE}"
		"xasecure.audit.hdfs.config.destination.flush.interval.seconds"= "{ENV:RANGER_HDFS_DESTINTATION_FLUSH_INTERVAL_SECONDS}"
		"xasecure.audit.hdfs.config.destination.rollover.interval.seconds"= "{ENV:RANGER_HDFS_DESTINTATION_ROLLOVER_INTERVAL_SECONDS}"
		"xasecure.audit.hdfs.config.destination.open.retry.interval.seconds"= "{ENV:RANGER_HDFS_DESTINTATION_OPEN_RETRY_INTERVAL_SECONDS}"
		"xasecure.audit.hdfs.config.local.buffer.directroy"     =  "{ENV:RANGER.HDFS_LOCAL_BUFFER_DIRECTORY}"
		"xasecure.audit.hdfs.config.local.buffer.file"          =  "{ENV:RANGER.HDFS_LOCAL_BUFFER_FILE}"
		"xasecure.audit.hdfs.config.local.buffer.flush.interval.seconds"= "{ENV:RANGER_HDFS_LOCAL_BUFFER_FLUSH_INTERVAL_SECONDS}"
		"xasecure.audit.hdfs.config.local.buffer.rollover.interval.seconds"= "{ENV:RANGER_HDFS_LOCAL_BUFFER_ROLLOVER_INTERVAL_SECONDS}"
		"xasecure.audit.hdfs.config.local.archive.directroy"      = "{ENV:RANGER_HDFS_LOCAL_ARCHIVE_DIRECTORY}"
		"xasecure.audit.hdfs.config.local.archive.max.file.count" =  "{ENV:RANGER_HDFS_LOCAL_ARCHIVE_MAX_FILE_COUNT}"

	}

    ####
    #### Apply configuration changes to xasecure-hive-security.xml
    ####
	#
    #$xmlFile = Join-Path $ENV:HIVE_CONF_DIR "xasecure-hive-security.xml"
	#
    $hiveSecurityChanges = @{
		"hive.authorization.verifier.classname"					= "com.xasecure.pdp.hive.XASecureAuthorizer"
		"xasecure.hive.policymgr.url"							= "${ENV:RANGER_EXTERNAL_URL}/service/assets/policyList/${ENV:RANGER_HIVE_REPO}"
		"xasecure.hive.policymgr.url.saveAsFile"				= "${ENV:RANGER_HOME}\tmp\hive_${ENV:RANGER_HIVE_REPO}"
		"xasecure.hive.policymgr.url.laststoredfile"			= "${ENV:RANGER_HOME}\tmp\hive_${ENV:RANGER_HIVE_REPO}_json"
		"xasecure.hive.policymgr.url.reloadIntervalInMillis"	= "30000"
		"xasecure.hive.update.xapolicies.on.grant.revoke"		= "true"
		"xasecure.policymgr.url"								= "${ENV:RANGER_EXTERNAL_URL}"
	}

    $configs = @{}
    #$configs.Add("hiveChanges",$hiveChanges)
    $configs.Add("hiveServerChanges",$hiveServerChanges)
    $configs.Add("hiveAuditChanges",$hiveAuditChanges)
    $configs.Add("hiveSecurityChanges",$hiveSecurityChanges)
    Configure "ranger-hive" $nodeInstallRoot $serviceCredential $configs
    Write-Log "Installation of ranger-hive completed successfully"


    #####################################################################
    ####			Install and Configure ranger-hbase plugin              ###
    #####################################################################
    #

	if ("$ENV:HBASE" -eq "yes") {

	    $roles = ' '
	    Install "ranger-hbase" $nodeInstallRoot $serviceCredential $roles

	    ####
	    #### Apply configuration changes to hbase-site.xml
	    ####
	    #$xmlFile = Join-Path $ENV:HBASE_CONF_DIR "hbase-site.xml"
		$hbaseChanges =   @{
	        "hbase.security.authorization"      = "true"
		"hbase.coprocessor.master.classes"	= "com.xasecure.authorization.hbase.XaSecureAuthorizationCoprocessor"
			"hbase.coprocessor.region.classes"	= "com.xasecure.authorization.hbase.XaSecureAuthorizationCoprocessor"
			"hbase.rpc.protection"				= "PRIVACY"
		    "hbase.rpc.engine"					= "org.apache.hadoop.hbase.ipc.SecureRpcEngine"
		}

	    ####
	    #### Apply configuration changes to xasecure-audit.xml
	    ####
	    #$xmlFile = Join-Path $ENV:HBASE_CONF_DIR "xasecure-audit.xml"
	    $hbaseAuditChanges =   @{
	        "xasecure.audit.db.is.enabled"                          = "false"
	        "xasecure.audit.jpa.javax.persistence.jdbc.url"			= "jdbc:mysql://${ENV:RANGER_AUDIT_DB_HOST}:${ENV:RANGER_AUDIT_DB_PORT}/${ENV:RANGER_AUDIT_DB_DBNAME}"
			"xasecure.audit.jpa.javax.persistence.jdbc.user"		= "${ENV:RANGER_AUDIT_DB_USERNAME}"
			"xasecure.audit.jpa.javax.persistence.jdbc.password"	= "crypted"
			"xasecure.audit.repository.name"						= "${ENV:RANGER_HBASE_REPO}"
			"xasecure.audit.credential.provider.file"				= "jceks://file/${ENV:RANGER_HBASE_CRED_KEYSTORE_FILE}"
		"xasecure.audit.jpa.javax.persistence.jdbc.driver"		= "com.mysql.jdbc.Driver"
			"xasecure.audit.hdfs.is.enabled"                        = "false"
			"xasecure.audit.hdfs.config.destination.directroy"      = "${ENV:RANGER_HDFS_DESTINATION_DIRECTORY}"
			"xasecure.audit.hdfs.config.destination.file"           = "${ENV:RANGER_HDFS_DESTINTATION_FILE}"
			"xasecure.audit.hdfs.config.destination.flush.interval.seconds"= "{ENV:RANGER_HDFS_DESTINTATION_FLUSH_INTERVAL_SECONDS}"
			"xasecure.audit.hdfs.config.destination.rollover.interval.seconds"= "{ENV:RANGER_HDFS_DESTINTATION_ROLLOVER_INTERVAL_SECONDS}"
			"xasecure.audit.hdfs.config.destination.open.retry.interval.seconds"= "{ENV:RANGER_HDFS_DESTINTATION_OPEN_RETRY_INTERVAL_SECONDS}"
			"xasecure.audit.hdfs.config.local.buffer.directroy"     =  "{ENV:RANGER.HDFS_LOCAL_BUFFER_DIRECTORY}"
			"xasecure.audit.hdfs.config.local.buffer.file"          =  "{ENV:RANGER.HDFS_LOCAL_BUFFER_FILE}"
			"xasecure.audit.hdfs.config.local.buffer.flush.interval.seconds"= "{ENV:RANGER_HDFS_LOCAL_BUFFER_FLUSH_INTERVAL_SECONDS}"
			"xasecure.audit.hdfs.config.local.buffer.rollover.interval.seconds"= "{ENV:RANGER_HDFS_LOCAL_BUFFER_ROLLOVER_INTERVAL_SECONDS}"
			"xasecure.audit.hdfs.config.local.archive.directroy"      = "{ENV:RANGER_HDFS_LOCAL_ARCHIVE_DIRECTORY}"
			"xasecure.audit.hdfs.config.local.archive.max.file.count" =  "{ENV:RANGER_HDFS_LOCAL_ARCHIVE_MAX_FILE_COUNT}"


		}

	    ####
	    #### Apply configuration changes to xasecure-hbase-security.xml
	    ####
		#
	    #$xmlFile = Join-Path $ENV:HBASE_CONF_DIR "xasecure-hbase-security.xml"
		#
	    $hbaseSecurityChanges =     @{
			"hbase.authorization.verifier.classname"				= "com.xasecure.pdp.hbase.XASecureAuthorizer"
			"xasecure.hbase.policymgr.url"							= "${ENV:RANGER_EXTERNAL_URL}/service/assets/policyList/${ENV:RANGER_HBASE_REPO}"
			"xasecure.hbase.policymgr.url.saveAsFile"				= "${ENV:RANGER_HOME}\tmp\hbase_${ENV:RANGER_HBASE_REPO}"
			"xasecure.hbase.policymgr.url.laststoredfile"			= "${ENV:RANGER_HOME}\tmp\hbase_${ENV:RANGER_HBASE_REPO}_json"
			"xasecure.hbase.policymgr.url.reloadIntervalInMillis"	= "30000"
			"xasecure.hbase.update.xapolicies.on.grant.revoke"		= "true"
			"xasecure.policymgr.url"								= "${ENV:RANGER_EXTERNAL_URL}"
		}

		$configs = @{}
	    $configs.Add("hbaseChanges",$hbaseChanges)
	    $configs.Add("hbaseAuditChanges",$hbaseAuditChanges)
	    $configs.Add("hbaseSecurityChanges",$hbaseSecurityChanges)
	    Configure "ranger-hbase" $nodeInstallRoot $serviceCredential $configs
	    Write-Log "Installation of ranger-hbase completed successfully"

	    Configure "ranger-hbase" $nodeInstallRoot $serviceCredential $configs
	    Write-Log "Installation of ranger-hbase completed successfully"
	} else {
	    Write-Log "Not installing ranger-hbase, since HBase is not installed"
	}



    #####################################################################
    ####			Install and Configure ranger-knox plugin              ###
    #####################################################################
    #

	if ("$ENV:KNOX" -eq "yes") {

		$roles = ' '
		Install "ranger-knox" $nodeInstallRoot $serviceCredential $roles

	    ####
	    #### Apply configuration changes to xasecure-audit.xml
	    ####
	    #$xmlFile = Join-Path $ENV:KNOX_CONF_DIR "xasecure-audit.xml"
	    $knoxAuditChanges =   @{
	        "xasecure.audit.db.is.enabled"                          = "true"
	        "xasecure.audit.jpa.javax.persistence.jdbc.url"			= "jdbc:mysql://${ENV:RANGER_AUDIT_DB_HOST}:${ENV:RANGER_AUDIT_DB_PORT}/${ENV:RANGER_AUDIT_DB_DBNAME}"
			"xasecure.audit.jpa.javax.persistence.jdbc.user"		= "${ENV:RANGER_AUDIT_DB_USERNAME}"
			"xasecure.audit.jpa.javax.persistence.jdbc.password"	= "crypted"
			"xasecure.audit.repository.name"						= "${ENV:RANGER_KNOX_REPO}"
			"xasecure.audit.credential.provider.file"				= "jceks://file/${ENV:RANGER_KNOX_CRED_KEYSTORE_FILE}"
		"xasecure.audit.jpa.javax.persistence.jdbc.driver"		= "com.mysql.jdbc.Driver"
			"xasecure.audit.hdfs.is.enabled"                        = "false"
			"xasecure.audit.hdfs.config.destination.directroy"      = "${ENV:RANGER_HDFS_DESTINATION_DIRECTORY}"
			"xasecure.audit.hdfs.config.destination.file"           = "${ENV:RANGER_HDFS_DESTINTATION_FILE}"
			"xasecure.audit.hdfs.config.destination.flush.interval.seconds"= "{ENV:RANGER_HDFS_DESTINTATION_FLUSH_INTERVAL_SECONDS}"
			"xasecure.audit.hdfs.config.destination.rollover.interval.seconds"= "{ENV:RANGER_HDFS_DESTINTATION_ROLLOVER_INTERVAL_SECONDS}"
			"xasecure.audit.hdfs.config.destination.open.retry.interval.seconds"= "{ENV:RANGER_HDFS_DESTINTATION_OPEN_RETRY_INTERVAL_SECONDS}"
			"xasecure.audit.hdfs.config.local.buffer.directroy"     =  "{ENV:RANGER.HDFS_LOCAL_BUFFER_DIRECTORY}"
			"xasecure.audit.hdfs.config.local.buffer.file"          =  "{ENV:RANGER.HDFS_LOCAL_BUFFER_FILE}"
			"xasecure.audit.hdfs.config.local.buffer.flush.interval.seconds"= "{ENV:RANGER_HDFS_LOCAL_BUFFER_FLUSH_INTERVAL_SECONDS}"
			"xasecure.audit.hdfs.config.local.buffer.rollover.interval.seconds"= "{ENV:RANGER_HDFS_LOCAL_BUFFER_ROLLOVER_INTERVAL_SECONDS}"
			"xasecure.audit.hdfs.config.local.archive.directroy"      = "{ENV:RANGER_HDFS_LOCAL_ARCHIVE_DIRECTORY}"
			"xasecure.audit.hdfs.config.local.archive.max.file.count" =  "{ENV:RANGER_HDFS_LOCAL_ARCHIVE_MAX_FILE_COUNT}"


		}

	    ####
	    #### Apply configuration changes to xasecure-knox-security.xml
	    ####
		#
	    #$xmlFile = Join-Path $ENV:KNOX_CONF_DIR "xasecure-knox-security.xml"
		#
	    $knoxSecurityChanges =     @{
			"knox.authorization.verifier.classname"				= "com.xasecure.pdp.knox.XASecureAuthorizer"
			"xasecure.knox.policymgr.url"							= "${ENV:RANGER_EXTERNAL_URL}/service/assets/policyList/${ENV:RANGER_KNOX_REPO}"
			"xasecure.knox.policymgr.url.saveAsFile"				= "${ENV:RANGER_HOME}\tmp\knox_${ENV:RANGER_KNOX_REPO}"
			"xasecure.knox.policymgr.url.laststoredfile"			= "${ENV:RANGER_HOME}\tmp\knox_${ENV:RANGER_KNOX_REPO}_json"
			"xasecure.knox.policymgr.url.reloadIntervalInMillis"	= "30000"
			"xasecure.knox.update.xapolicies.on.grant.revoke"		= "true"
			"xasecure.policymgr.url"								= "${ENV:RANGER_EXTERNAL_URL}"
		}

		$configs = @{}
	    $configs.Add("knoxAuditChanges",$knoxAuditChanges)
	    $configs.Add("knoxSecurityChanges",$knoxSecurityChanges)
	    Configure "ranger-knox" $nodeInstallRoot $serviceCredential $configs
	    Write-Log "Installation of ranger-knox completed successfully"

	    Configure "ranger-knox" $nodeInstallRoot $serviceCredential $configs
		Write-Log "Installation of ranger-knox completed successfully"

	} else {
	    Write-Log "Not installing ranger-knox, since Knox is not installed"
	}


    #####################################################################
    ####			Install and Configure ranger-storm plugin              ###
    #####################################################################
    #

	if ("$ENV:STORM" -eq "yes") {
	    $roles = ' '
	    Install "ranger-storm" $nodeInstallRoot $serviceCredential $roles

	    ####
	    #### Apply configuration changes to xasecure-audit.xml
	    ####
	    #$xmlFile = Join-Path $ENV:STORM_CONF_DIR "xasecure-audit.xml"
	    $stormAuditChanges =   @{
	        "xasecure.audit.db.is.enabled"                          = "true"
	        "xasecure.audit.jpa.javax.persistence.jdbc.url"			= "jdbc:mysql://${ENV:RANGER_AUDIT_DB_HOST}:${ENV:RANGER_AUDIT_DB_PORT}/${ENV:RANGER_AUDIT_DB_DBNAME}"
			"xasecure.audit.jpa.javax.persistence.jdbc.user"		= "${ENV:RANGER_AUDIT_DB_USERNAME}"
			"xasecure.audit.jpa.javax.persistence.jdbc.password"	= "crypted"
			"xasecure.audit.repository.name"						= "${ENV:RANGER_STORM_REPO}"
			"xasecure.audit.credential.provider.file"				= "jceks://file/${ENV:RANGER_STORM_CRED_KEYSTORE_FILE}"
		"xasecure.audit.jpa.javax.persistence.jdbc.driver"		= "com.mysql.jdbc.Driver"
			"xasecure.audit.hdfs.is.enabled"                        = "false"
			"xasecure.audit.hdfs.config.destination.directroy"      = "${ENV:RANGER_HDFS_DESTINATION_DIRECTORY}"
			"xasecure.audit.hdfs.config.destination.file"           = "${ENV:RANGER_HDFS_DESTINTATION_FILE}"
			"xasecure.audit.hdfs.config.destination.flush.interval.seconds"= "{ENV:RANGER_HDFS_DESTINTATION_FLUSH_INTERVAL_SECONDS}"
			"xasecure.audit.hdfs.config.destination.rollover.interval.seconds"= "{ENV:RANGER_HDFS_DESTINTATION_ROLLOVER_INTERVAL_SECONDS}"
			"xasecure.audit.hdfs.config.destination.open.retry.interval.seconds"= "{ENV:RANGER_HDFS_DESTINTATION_OPEN_RETRY_INTERVAL_SECONDS}"
			"xasecure.audit.hdfs.config.local.buffer.directroy"     =  "{ENV:RANGER.HDFS_LOCAL_BUFFER_DIRECTORY}"
			"xasecure.audit.hdfs.config.local.buffer.file"          =  "{ENV:RANGER.HDFS_LOCAL_BUFFER_FILE}"
			"xasecure.audit.hdfs.config.local.buffer.flush.interval.seconds"= "{ENV:RANGER_HDFS_LOCAL_BUFFER_FLUSH_INTERVAL_SECONDS}"
			"xasecure.audit.hdfs.config.local.buffer.rollover.interval.seconds"= "{ENV:RANGER_HDFS_LOCAL_BUFFER_ROLLOVER_INTERVAL_SECONDS}"
			"xasecure.audit.hdfs.config.local.archive.directroy"      = "{ENV:RANGER_HDFS_LOCAL_ARCHIVE_DIRECTORY}"
			"xasecure.audit.hdfs.config.local.archive.max.file.count" =  "{ENV:RANGER_HDFS_LOCAL_ARCHIVE_MAX_FILE_COUNT}"


		}

	    ####
	    #### Apply configuration changes to xasecure-storm-security.xml
	    ####
		#
	    #$xmlFile = Join-Path $ENV:STORM_CONF_DIR "xasecure-storm-security.xml"
		#
	    $stormSecurityChanges =     @{
			"storm.authorization.verifier.classname"				= "com.xasecure.pdp.storm.XASecureAuthorizer"
			"xasecure.storm.policymgr.url"							= "${ENV:RANGER_EXTERNAL_URL}/service/assets/policyList/${ENV:RANGER_STORM_REPO}"
			"xasecure.storm.policymgr.url.saveAsFile"				= "${ENV:RANGER_HOME}\tmp\storm_${ENV:RANGER_STORM_REPO}"
			"xasecure.storm.policymgr.url.laststoredfile"			= "${ENV:RANGER_HOME}\tmp\storm_${ENV:RANGER_STORM_REPO}_json"
			"xasecure.storm.policymgr.url.reloadIntervalInMillis"	= "30000"
			"xasecure.storm.update.xapolicies.on.grant.revoke"		= "true"
			"xasecure.policymgr.url"								= "${ENV:RANGER_EXTERNAL_URL}"
		}

		$configs = @{}
	    $configs.Add("stormAuditChanges",$stormAuditChanges)
	    $configs.Add("stormSecurityChanges",$stormSecurityChanges)
	    Configure "ranger-storm" $nodeInstallRoot $serviceCredential $configs
	    Write-Log "Installation of ranger-storm completed successfully"

	    Configure "ranger-storm" $nodeInstallRoot $serviceCredential $configs
		Write-Log "Installation of ranger-storm completed successfully"
	} else {
	    Write-Log "Not installing ranger-storm, since Storm is not installed"
	}


    #####################################################################
    ####         Install and Configure ranger-usersync service              ###
    #####################################################################
    #

    $roles = "ranger-usersync"
    Install "ranger-usersync" $nodeInstallRoot $serviceCredential $roles
    Configure "ranger-usersync" $nodeInstallRoot $serviceCredential
    Write-Log "Installation of ranger-usersync completed successfully"


}

try
{
    $scriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)
    $utilsModule = Import-Module -Name "$scriptDir\..\resources\Winpkg.Utils.psm1" -ArgumentList ("ranger") -PassThru
    $apiModule = Import-Module -Name "$scriptDir\InstallApi.psm1" -PassThru
    Main $scriptDir
}
catch [Exception]
{
    Write-Log $_.Exception.Message $_
    throw $_.Exception.Message
}
finally
{
    if( $apiModule -ne $null )
    {
        Remove-Module $apiModule
    }

    if( $utilsModule -ne $null )
    {

        Remove-Module $utilsModule
    }
}
