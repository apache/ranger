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
### A set of basic PowerShell routines that can be used to install and
### manage Hadoop services on a single node. For use-case see install.ps1.
###

###
### Global variables
###
$ScriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)

$FinalName = "ranger-@ranger.version@"

###############################################################################
###
### Installs ranger.
###
### Arguments:
###     component: Component to be installed, it can be "core, "hdfs" or "mapreduce"
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     role: Space separated list of roles that should be installed.
###           (for example, "jobtracker historyserver" for mapreduce)
###
###############################################################################

function Install(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$false )]
    $serviceCredential,
    [String]
    [Parameter( Position=3, Mandatory=$false )]
    $roles
    )
{

    if ( $component -eq "ranger" )
    {
        InstallRangerCore $nodeInstallRoot $serviceCredential $roles
	}
	elseif ( $component -eq "ranger-hdfs" )
	{
        InstallHdfs $nodeInstallRoot $serviceCredential $roles
	}
	elseif ( $component -eq "ranger-hive" )
	{
        InstallHive $nodeInstallRoot $serviceCredential $roles
	}
	elseif ( $component -eq "ranger-hbase" )
	{
        InstallHBase $nodeInstallRoot $serviceCredential $roles
	}
	elseif ( $component -eq "ranger-knox" )
	{
        InstallKnox $nodeInstallRoot $serviceCredential $roles
	}
	elseif ( $component -eq "ranger-storm" )
	{
        InstallStorm $nodeInstallRoot $serviceCredential $roles
	}
    elseif ( $component -eq "ranger-usersync" )
    {
        InstallUserSync $nodeInstallRoot $serviceCredential $roles
    }
    else
    {
        throw "Install: Unsupported component argument."
    }
}


###############################################################################
###
### Installs Ranger HDFS component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     hdfsRole: Space separated list of  roles that should be installed.
###               (for example, "ranger")
###
###############################################################################
function InstallRangerCore(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$false )]
    $serviceCredential,
    [String]
    [Parameter( Position=2, Mandatory=$false )]
    $roles
    )
{
	$HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"

	### $rangerInstallPath: the name of the folder containing the application, after unzipping
	$rangerInstallPath = Join-Path $nodeInstallRoot $FinalName
	$rangerAdmin = $FinalName + "-admin"
	$rangerAdminInstallPath = Join-Path "$rangerInstallPath" "$rangerAdmin"
	$rangerInstallToBin = Join-Path "$rangerAdminInstallPath" "bin"
	InstallBinaries $nodeInstallRoot $serviceCredential

	if ($roles) {
		###
		### Create Ranger Windows Services and grant user ACLS to start/stop
		###
		### TODO
		Write-Log "Ranger Role Services: $roles"

		### Verify that roles are in the supported set
		### TODO
		CheckRole $roles @("ranger-admin")



		Write-Log "Role : $roles"
		foreach( $service in empty-null ($roles -Split('\s+')))
		{
			CreateAndConfigureHadoopService $service $HDP_RESOURCES_DIR $rangerInstallToBin $serviceCredential
			if ( $service -eq "ranger-admin" )
			{
				$credStorePath = Join-Path $ENV:RANGER_HOME "jceks"
				$credStorePath = $credStorePath -replace "\\", "/"

				### Create Credential Store  directory
				if( -not (Test-Path "$credStorePath"))
				{
					Write-Log "Creating Credential Store directory: `"$credStorePath`""
					$cmd = "mkdir `"$credStorePath`""
					Invoke-CmdChk $cmd
				}

				CreateJCEKS "policyDB.jdbc.password" "${ENV:RANGER_ADMIN_DB_PASSWORD}" "${ENV:RANGER_ADMIN_HOME}\cred\lib" "$credStorePath/xapolicymgr.jceks"
				CreateJCEKS "auditDb.jdbc.password" "${ENV:RANGER_AUDIT_DB_PASSWORD}" "${ENV:RANGER_ADMIN_HOME}\cred\lib" "$credStorePath/xapolicymgr.jceks"
				[Environment]::SetEnvironmentVariable("RANGER_ADMIN_CRED_KEYSTORE_FILE", "$credStorePath\xapolicymgr.jceks" , [EnvironmentVariableTarget]::Machine)
				$ENV:RANGER_ADMIN_CRED_KEYSTORE_FILE = "$credStorePath/xapolicymgr.jceks"

			}

			###
			### Setup ranger service config
			###
			$ENV:PATH="$ENV:HADOOP_HOME\bin;" + $ENV:PATH
			Write-Log "Creating service config ${rangerInstallToBin}\$service.xml"
			# TODO:WINDOWS take python from `which` or `where`
			$cmd = "python $rangerInstallToBin\service_start.py --service > `"$rangerInstallToBin\$service.xml`""
			Invoke-CmdChk $cmd

			Write-Log "Configuring Ranger"
			$cmd = "python $rangerInstallToBin\service_start.py --configure"
			Invoke-CmdChk $cmd

		}
		### end of roles loop
	}
	$username = $serviceCredential.UserName
	GiveFullPermissions $rangerInstallToBin $username $true
	GiveFullPermissions `"$ENV:RANGER_HOME\jceks`" $username $true
	GiveFullPermissions `"$ENV:RANGER_HOME\tmp`" $username $true

	Write-Log "Finished installing Ranger Admin Tool"

}


###############################################################################
###
### Installs Ranger HDFS component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     hdfsRole: Space separated list of  roles that should be installed.
###               (for example, "ranger")
###
###############################################################################
function InstallHdfs(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$false )]
    $serviceCredential,
    [String]
    [Parameter( Position=2, Mandatory=$false )]
    $roles
    )
{

        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"
		# This if will work on the assumption that $component ="ranger" is installed
		# so we have the RANGER_HDFS_HOME properly set
		$credStorePath = Join-Path $ENV:RANGER_HOME "jceks"
		$credStorePath = $credStorePath -replace "\\", "/"

		# setup path variables
        $rangerInstallPath = Join-Path $nodeInstallRoot $FinalName

        Write-Log "Copying ranger-hdfs config files "

		Write-Log "Checking the HADOOP_HOME Installation."
        if( -not (Test-Path $ENV:HADOOP_HOME))
        {
          Write-Log "HADOOP_HOME not set properly; $ENV:HADOOP_HOME does not exist" "Failure"
          throw "Install: HADOOP_HOME not set properly; $ENV:HADOOP_HOME does not exist."
        }



		Write-Log "Checking the HADOOP_CONF_DIR Installation."
        if( -not (Test-Path $ENV:HADOOP_CONF_DIR))
        {
          Write-Log "HADOOP_CONF_DIR not set properly; $ENV:HADOOP_CONF_DIR does not exist" "Failure"
          throw "Install: HADOOP_CONF_DIR not set properly; $ENV:HADOOP_CONF_DIR does not exist."
        }


        $xcopy_cmd = "xcopy /EIYF `"$ENV:RANGER_HDFS_HOME\install\conf.templates\enable\*.xml`" `"$ENV:HADOOP_CONF_DIR`""
        Invoke-CmdChk $xcopy_cmd

		$xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template\xasecure-hadoop-env.cmd`" `"$ENV:HADOOP_CONF_DIR\`""
        Invoke-CmdChk $xcopy_cmd

        $xcopy_cmd = "xcopy /EIYF `"$ENV:RANGER_HDFS_HOME\lib\*.jar`" `"$ENV:HADOOP_HOME\share\hadoop\common\lib\`""
        Invoke-CmdChk $xcopy_cmd


		CreateJCEKS "auditDBCred" "${ENV:RANGER_AUDIT_DB_PASSWORD}" "${ENV:RANGER_HDFS_HOME}\install\lib" "$credStorePath/Repo_${ENV:RANGER_HDFS_REPO}.jceks"

		$username = $serviceCredential.UserName
		GiveFullPermissions `"$ENV:RANGER_HOME\jceks`" $username $true

        [Environment]::SetEnvironmentVariable("RANGER_HDFS_CRED_KEYSTORE_FILE", "$credStorePath\Repo_${ENV:RANGER_HDFS_REPO}.jceks" , [EnvironmentVariableTarget]::Machine)
        $ENV:RANGER_HDFS_CRED_KEYSTORE_FILE = "$credStorePath/Repo_${ENV:RANGER_HDFS_REPO}.jceks"

}

###############################################################################
###
### Installs Ranger Hive component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     hdfsRole: Space separated list of  roles that should be installed.
###               (for example, "ranger")
###
###############################################################################
function InstallHive(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$false )]
    $serviceCredential,
    [String]
    [Parameter( Position=2, Mandatory=$false )]
    $roles
    )
{
        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"
		# This if will work on the assumption that $component ="ranger" is installed
		# so we have the RANGER_HIVE_HOME properly set
		$credStorePath = Join-Path $ENV:RANGER_HOME "jceks"
		$credStorePath = $credStorePath -replace "\\", "/"
        Write-Log "Copying ranger-hive config files "

		Write-Log "Checking the HIVE_HOME Installation."
        if( -not (Test-Path $ENV:HIVE_HOME))
        {
          Write-Log "HIVE_HOME not set properly; $ENV:HIVE_HOME does not exist" "Failure"
          throw "Install: HIVE_HOME not set properly; $ENV:HIVE_HOME does not exist."
        }

        $xcopy_cmd = "xcopy /EIYF `"$ENV:RANGER_HIVE_HOME\install\conf.templates\enable\*.xml`" `"$ENV:HIVE_HOME\conf`""
        Invoke-CmdChk $xcopy_cmd

        $xcopy_cmd = "xcopy /EIYF `"$ENV:RANGER_HIVE_HOME\lib\*.jar`" `"$ENV:HIVE_HOME\lib\`""
        Invoke-CmdChk $xcopy_cmd


        if( -not (Test-Path `"$ENV:HIVE_HOME\conf\hiveserver2-site.xml`"))
		{
			$copy_cmd = "copy `"$ENV:RANGER_HIVE_HOME\install\conf.templates\default\configuration.xml`" `"$ENV:HIVE_HOME\conf\hiveserver2-site.xml`""
			Invoke-CmdChk $copy_cmd
		}

        if( Test-Path `"$ENV:HIVE_HOME\bin\ext\hiveserver2.cmd`")
		{
			$copy_cmd = "copy `"$ENV:HIVE_HOME\bin\ext\hiveserver2.cmd`" `"$ENV:HIVE_HOME\bin\ext\hiveserver2.cmd.orig`""
			Invoke-CmdChk $copy_cmd
		}

		$copy_cmd = "copy `"$HDP_INSTALL_PATH\..\template\hiveserver2-ranger.cmd`" `"$ENV:HIVE_HOME\bin\ext\hiveserver2.cmd`""
		Invoke-CmdChk $copy_cmd

		CreateJCEKS "auditDBCred" "${ENV:RANGER_AUDIT_DB_PASSWORD}" "${ENV:RANGER_HIVE_HOME}\install\lib" "$credStorePath/Repo_${ENV:RANGER_HIVE_REPO}.jceks"

		$username = $serviceCredential.UserName
		GiveFullPermissions `"$ENV:RANGER_HOME\jceks`" $username $true

        [Environment]::SetEnvironmentVariable("RANGER_HIVE_CRED_KEYSTORE_FILE", "$credStorePath\Repo_${ENV:RANGER_HIVE_REPO}.jceks" , [EnvironmentVariableTarget]::Machine)
        $ENV:RANGER_HIVE_CRED_KEYSTORE_FILE = "$credStorePath/Repo_${ENV:RANGER_HIVE_REPO}.jceks"


        #$xcopy_cmd = "xcopy /EIYF `"$ENV:RANGER_HIVE_HOME\template\configuration.xml`" `"$ENV:HADOOP_CONF_DIR`""
        #Invoke-CmdChk $xcopy_cmd


}

###############################################################################
###
### Installs Ranger HBase component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     hdfsRole: Space separated list of  roles that should be installed.
###               (for example, "ranger")
###
###############################################################################
function InstallHBase(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$false )]
    $serviceCredential,
    [String]
    [Parameter( Position=2, Mandatory=$false )]
    $roles
    )
{
		# This if will work on the assumption that $component ="ranger" is installed
		# so we have the RANGER_HIVE_HOME properly set
		$credStorePath = Join-Path $ENV:RANGER_HOME "jceks"
		$credStorePath = $credStorePath -replace "\\", "/"

        Write-Log "Copying ranger-hbase config files "

		Write-Log "Checking the HBASE_HOME Installation."
        if( -not (Test-Path $ENV:HBASE_HOME))
        {
          Write-Log "HBASE_HOME not set properly; $ENV:HBASE_HOME does not exist" "Failure"
          throw "Install: HBASE_HOME not set properly; $ENV:HBASE_HOME does not exist."
        }

		Write-Log "Checking the HBASE_CONF_DIR Installation."
        if( -not (Test-Path $ENV:HBASE_CONF_DIR))
        {
          Write-Log "HBASE_CONF_DIR not set properly; $ENV:HBASE_CONF_DIR does not exist" "Failure"
          throw "Install: HBASE_CONF_DIR not set properly; $ENV:HBASE_CONF_DIR does not exist."
        }

        $xcopy_cmd = "xcopy /EIYF `"$ENV:RANGER_HBASE_HOME\install\conf.templates\enable\*.xml`" `"$ENV:HBASE_CONF_DIR`""
        Invoke-CmdChk $xcopy_cmd

        $xcopy_cmd = "xcopy /EIYF `"$ENV:RANGER_HBASE_HOME\lib\*.jar`" `"$ENV:HBASE_HOME\lib\`""
        Invoke-CmdChk $xcopy_cmd

		CreateJCEKS "auditDBCred" "${ENV:RANGER_AUDIT_DB_PASSWORD}" "${ENV:RANGER_HBASE_HOME}\install\lib" "$credStorePath/Repo_${ENV:RANGER_HBASE_REPO}.jceks"

		$username = $serviceCredential.UserName
		GiveFullPermissions `"$ENV:RANGER_HOME\jceks`" $username $true

        [Environment]::SetEnvironmentVariable("RANGER_HBASE_CRED_KEYSTORE_FILE", "$credStorePath\Repo_${ENV:RANGER_HBASE_REPO}.jceks" , [EnvironmentVariableTarget]::Machine)
        $ENV:RANGER_HBASE_CRED_KEYSTORE_FILE = "$credStorePath/Repo_${ENV:RANGER_HBASE_REPO}.jceks"


        #$xcopy_cmd = "xcopy /EIYF `"$ENV:RANGER_HBASE_HOME\template\configuration.xml`" `"$ENV:HADOOP_CONF_DIR`""
        #Invoke-CmdChk $xcopy_cmd


}


###############################################################################
###
### Installs Ranger Knox component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     hdfsRole: Space separated list of  roles that should be installed.
###               (for example, "ranger")
###
###############################################################################
function InstallKnox(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$false )]
    $serviceCredential,
    [String]
    [Parameter( Position=2, Mandatory=$false )]
    $roles
    )
{
		# This if will work on the assumption that $component ="ranger" is installed
		# so we have the RANGER_HIVE_HOME properly set
		$credStorePath = Join-Path $ENV:RANGER_HOME "jceks"
		$credStorePath = $credStorePath -replace "\\", "/"

        Write-Log "Copying ranger-knox config files "

		Write-Log "Checking the KNOX CONF DIR Installation."
        if( -not (Test-Path $ENV:KNOX_HOME\conf))
        {
          Write-Log "${ENV:KNOX_HOME}\conf does not exist" "Failure"
          throw "Install: ${ENV:KNOX_HOME}\conf dir does not exist."
        }

        $xcopy_cmd = "xcopy /EIYF `"$ENV:RANGER_KNOX_HOME\install\conf.templates\enable\*.xml`" `"${ENV:KNOX_HOME}\conf`""
        Invoke-CmdChk $xcopy_cmd

        $xcopy_cmd = "xcopy /EIYF `"$ENV:RANGER_KNOX_HOME\lib\*.jar`" `"$ENV:KNOX_HOME\lib`""
        Invoke-CmdChk $xcopy_cmd

		CreateJCEKS "auditDBCred" "${ENV:RANGER_AUDIT_DB_PASSWORD}" "${ENV:RANGER_KNOX_HOME}\install\lib" "$credStorePath/Repo_${ENV:RANGER_KNOX_REPO}.jceks"

		$username = $serviceCredential.UserName
		GiveFullPermissions `"$ENV:RANGER_HOME\jceks`" $username $true

        [Environment]::SetEnvironmentVariable("RANGER_KNOX_CRED_KEYSTORE_FILE", "$credStorePath\Repo_${ENV:RANGER_KNOX_REPO}.jceks" , [EnvironmentVariableTarget]::Machine)
        $ENV:RANGER_KNOX_CRED_KEYSTORE_FILE = "$credStorePath/Repo_${ENV:RANGER_KNOX_REPO}.jceks"


        #$xcopy_cmd = "xcopy /EIYF `"$ENV:RANGER_KNOX_HOME\template\configuration.xml`" `"$ENV:HADOOP_CONF_DIR`""
        #Invoke-CmdChk $xcopy_cmd


}


###############################################################################
###
### Installs Ranger Storm component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     hdfsRole: Space separated list of  roles that should be installed.
###               (for example, "ranger")
###
###############################################################################
function InstallStorm(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$false )]
    $serviceCredential,
    [String]
    [Parameter( Position=2, Mandatory=$false )]
    $roles
    )
{
		# This if will work on the assumption that $component ="ranger" is installed
		# so we have the RANGER_HIVE_HOME properly set
		$credStorePath = Join-Path $ENV:RANGER_HOME "jceks"
		$credStorePath = $credStorePath -replace "\\", "/"

        Write-Log "Copying ranger-storm config files "

		Write-Log "Checking the $ENV:STORM_HOME\conf Installation."
        if( -not (Test-Path $ENV:STORM_HOME\conf))
        {
          Write-Log "$ENV:STORM_HOME\conf not set properly; $ENV:STORM_HOME\conf does not exist" "Failure"
          throw "Install: $ENV:STORM_HOME\conf not set properly; $ENV:STORM_HOME\conf does not exist."
        }

        $xcopy_cmd = "xcopy /EIYF `"$ENV:RANGER_STORM_HOME\install\conf.templates\enable\*.xml`" `"$ENV:STORM_HOME\conf`""
        Invoke-CmdChk $xcopy_cmd

        $xcopy_cmd = "xcopy /EIYF `"$ENV:RANGER_STORM_HOME\lib\*.jar`" `"$ENV:STORM_HOME\lib`""
        Invoke-CmdChk $xcopy_cmd

		CreateJCEKS "auditDBCred" "${ENV:RANGER_AUDIT_DB_PASSWORD}" "${ENV:RANGER_STORM_HOME}\install\lib" "$credStorePath/Repo_${ENV:RANGER_STORM_REPO}.jceks"

		$username = $serviceCredential.UserName
		GiveFullPermissions `"$ENV:RANGER_HOME\jceks`" $username $true

        [Environment]::SetEnvironmentVariable("RANGER_STORM_CRED_KEYSTORE_FILE", "$credStorePath\Repo_${ENV:RANGER_STORM_REPO}.jceks" , [EnvironmentVariableTarget]::Machine)
        $ENV:RANGER_STORM_CRED_KEYSTORE_FILE = "$credStorePath/Repo_${ENV:RANGER_STORM_REPO}.jceks"


        #$xcopy_cmd = "xcopy /EIYF `"$ENV:RANGER_STORM_HOME\template\configuration.xml`" `"$ENV:HADOOP_CONF_DIR`""
        #Invoke-CmdChk $xcopy_cmd


}



###############################################################################
###
### Installs Ranger user-sync component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     hdfsRole: Space separated list of  roles that should be installed.
###               (for example, "ranger")
###
###############################################################################
function InstallUserSync(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$false )]
    $serviceCredential,
    [String]
    [Parameter( Position=2, Mandatory=$false )]
    $roles
    )
{
		# This if will work on the assumption that $component ="ranger" is installed
		# so we have the RANGER_USERSYNC_HOME properly set
		$HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"
        ### $rangerInstallPath: the name of the folder containing the application, after unzipping
        $rangerInstallPath = Join-Path $nodeInstallRoot $FinalName
        $rangerAdmin = $FinalName + "-admin"
        $rangerAdminInstallPath = Join-Path "$rangerInstallPath" "$rangerAdmin"
        $rangerInstallToBin = Join-Path "$rangerAdminInstallPath" "bin"

		if ($roles) {
			###
			### Create Ranger-UserSync Windows Services and grant user ACLS to start/stop
			###
			### TODO
			Write-Log "ranger-usersync Role Services: $roles"

			### Verify that roles are in the supported set
			### TODO
			CheckRole $roles @("ranger-usersync")

			Write-Log "Role : $roles"
			foreach( $service in empty-null ($roles -Split('\s+')))
			{
				CreateAndConfigureHadoopService $service $HDP_RESOURCES_DIR $rangerInstallToBin $serviceCredential
				if ( $service -eq "ranger-usersync" )
				{
					$credStorePath = Join-Path $ENV:RANGER_HOME "jceks"
					$credStorePath = $credStorePath -replace "\\", "/"

				    ### Create Credential Store  directory
					if( -not (Test-Path "$credStorePath"))
					{
						Write-Log "Creating Credential Store directory: `"$credStorePath`""
						$cmd = "mkdir `"$credStorePath`""
						Invoke-CmdChk $cmd
					}

					CreateJCEKS "ldap.bind.password" "${ENV:RANGER_SYNC_LDAP_BIND_PASSWORD}" "${ENV:RANGER_ADMIN_HOME}\cred\lib" "$credStorePath/usersync.jceks"

					$username = $serviceCredential.UserName
					GiveFullPermissions `"$ENV:RANGER_HOME\jceks`" $username $true

					[Environment]::SetEnvironmentVariable("RANGER_USERSYNC_CRED_KEYSTORE_FILE", "$credStorePath\usersync.jceks" , [EnvironmentVariableTarget]::Machine)
					$ENV:RANGER_USERSYNC_CRED_KEYSTORE_FILE = "$credStorePath/usersync.jceks"

				}

				###
				### Setup ranger usersync service config
				###
				$ENV:PATH="$ENV:HADOOP_HOME\bin;" + $ENV:PATH
				Write-Log "Creating service config ${rangerInstallToBin}\$service.xml"
				# TODO:WINDOWS take python from `which` or `where`
				$cmd = "python $rangerInstallToBin\ranger_usersync.py --service > `"$rangerInstallToBin\$service.xml`""
				Invoke-CmdChk $cmd
			}
	        ### end of roles loop
        }
		###	Install Ranger UserSync ends

}
###############################################################################
###
### Installs ranger binaries.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential:
###
###############################################################################
function InstallBinaries(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$true )]
    $serviceCredential
    )
{
        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"

        # setup path variables
        $rangerInstallPath = Join-Path $nodeInstallRoot $FinalName
        $rangerInstallToBin = Join-Path "$rangerInstallPath" "bin"

        $rangerAdminFile = $FinalName + "-admin"
        $rangerAdminPath = Join-Path $rangerInstallPath $rangerAdminFile

        $rangerHdfsPluginFile = $FinalName + "-hdfs-plugin"
        $rangerHdfsPluginPath = Join-Path $rangerInstallPath $rangerHdfsPluginFile

        $rangerHBasePluginFile = $FinalName + "-hbase-plugin"
        $rangerHBasePluginPath = Join-Path $rangerInstallPath $rangerHBasePluginFile

        $rangerHivePluginFile = $FinalName + "-hive-plugin"
        $rangerHivePluginPath = Join-Path $rangerInstallPath $rangerHivePluginFile

        $rangerKnoxPluginFile = $FinalName + "-knox-plugin"
        $rangerKnoxPluginPath = Join-Path $rangerInstallPath $rangerKnoxPluginFile

        $rangerStormPluginFile = $FinalName +"-storm-plugin"
        $rangerStormPluginPath = Join-Path $rangerInstallPath $rangerStormPluginFile

        $rangerUserSyncFile = $FinalName + "-usersync"
        $rangerUserSyncPath = Join-Path $rangerInstallPath $rangerUserSyncFile

        Write-Log "Installing $FinalName to $rangerInstallPath"
        #ranger: Installing ranger-0.1.0.2.1.1.0-1111 to D:\HDP\\ranger-0.1.0.2.1.1.0-1111

        Write-Log "Checking the JAVA Installation."
        if( -not (Test-Path $ENV:JAVA_HOME\bin\java.exe))
        {
            Write-Log "JAVA_HOME not set properly; $ENV:JAVA_HOME\bin\java.exe does not exist" "Failure"
            throw "Install: JAVA_HOME not set properly; $ENV:JAVA_HOME\bin\java.exe does not exist."
        }

        Write-Log "Checking the Hadoop Installation."
        if( -not (Test-Path $ENV:HADOOP_HOME\bin\winutils.exe))
        {
          Write-Log "HADOOP_HOME not set properly; $ENV:HADOOP_HOME\bin\winutils.exe does not exist" "Failure"
          throw "Install: HADOOP_HOME not set properly; $ENV:HADOOP_HOME\bin\winutils.exe does not exist."
        }

        ### Create Install Root directory
        if( -not (Test-Path "$rangerInstallPath"))
        {
            Write-Log "Creating Install Root directory: `"$rangerInstallPath`""
            $cmd = "mkdir `"$rangerInstallPath`""
            Invoke-CmdChk $cmd
        }

		### Create Ranger tmp directory
        if( -not (Test-Path "$rangerInstallPath\tmp"))
        {
            Write-Log "Creating Install Root directory: `"$rangerInstallPath`"\tmp"
            $cmd = "mkdir `"$rangerInstallPath`"\tmp"
            Invoke-CmdChk $cmd
        }

        $rangerLogsDir = Join-Path $ENV:HDP_LOG_DIR "ranger"
        ###
        ### ACL Ranger logs directory such that machine users can write to it
        ###
        if( -not (Test-Path "$rangerLogsDir"))
        {
            Write-Log "Creating Ranger logs folder"
            New-Item -Path "$rangerLogsDir" -type directory | Out-Null
        }
        GiveFullPermissions "$rangerLogsDir" "Users"
        Write-Log "Setting the RANGER_LOG_DIR environment variable at machine scope to `"$rangerLogDir`""
        [Environment]::SetEnvironmentVariable("RANGER_LOG_DIR", $rangerLogsDir, [EnvironmentVariableTarget]::Machine)
        $ENV:RANGER_LOG_DIR = "$rangerLogsDir"

        $rangerInstallPathParent = (Get-Item $rangerInstallPath).parent.FullName

        ###
        ###  Unzip Ranger secure from compressed archive
        ###

        Write-Log "Extracting $rangerAdminFile.zip to $rangerInstallPath"

        if ( Test-Path ENV:UNZIP_CMD )
        {
            ### Use external unzip command if given
            $unzipExpr = $ENV:UNZIP_CMD.Replace("@SRC", "`"$HDP_RESOURCES_DIR\$rangerAdminFile.zip`"")
            $unzipExpr = $unzipExpr.Replace("@DEST", "`"$rangerInstallPath`"")
            ### We ignore the error code of the unzip command for now to be
            ### consistent with prior behavior.
            Invoke-Ps $unzipExpr
        }
        else
        {
            $shellApplication = new-object -com shell.application
            $zipPackage = $shellApplication.NameSpace("$HDP_RESOURCES_DIR\$rangerAdminFile.zip")
            $destinationFolder = $shellApplication.NameSpace($rangerInstallPath)
            $destinationFolder.CopyHere($zipPackage.Items(), 20)
        }

        ###
        ### Set RANGER_HOME environment variable
        ###
        Write-Log "Setting the RANGER_HOME environment variable at machine scope to `"$rangerInstallPath`""
        [Environment]::SetEnvironmentVariable("RANGER_HOME", $rangerInstallPath, [EnvironmentVariableTarget]::Machine)
        $ENV:RANGER_HOME = "$rangerInstallPath"


        ###
        ### Set RANGER_ADMIN_HOME environment variable
        ###
        Write-Log "Setting the RANGER_ADMIN_HOME environment variable at machine scope to `"$rangerAdminPath`""
        [Environment]::SetEnvironmentVariable("RANGER_ADMIN_HOME", $rangerAdminPath, [EnvironmentVariableTarget]::Machine)
        $ENV:RANGER_ADMIN_HOME = "$rangerAdminPath"



        ###
        ###  Unzip Ranger HDFS Plugin from compressed archive
        ###

        Write-Log "Extracting $rangerHdfsPluginFile.zip to $rangerInstallPath"

        if ( Test-Path ENV:UNZIP_CMD )
        {
            ### Use external unzip command if given
            $unzipExpr = $ENV:UNZIP_CMD.Replace("@SRC", "`"$HDP_RESOURCES_DIR\$rangerHdfsPluginFile.zip`"")
            $unzipExpr = $unzipExpr.Replace("@DEST", "`"$rangerInstallPath`"")
            ### We ignore the error code of the unzip command for now to be
            ### consistent with prior behavior.
            Invoke-Ps $unzipExpr
        }
        else
        {
            $shellApplication = new-object -com shell.application
            $zipPackage = $shellApplication.NameSpace("$HDP_RESOURCES_DIR\$rangerHdfsPluginFile.zip")
            $destinationFolder = $shellApplication.NameSpace($rangerInstallPath)
            $destinationFolder.CopyHere($zipPackage.Items(), 20)
        }

        ###
        ### Set RANGER_HDFS_HOME environment variable
        ###
        Write-Log "Setting the RANGER_HDFS_HOME environment variable at machine scope to `"$rangerHdfsPluginPath`""
        [Environment]::SetEnvironmentVariable("RANGER_HDFS_HOME", $rangerHdfsPluginPath, [EnvironmentVariableTarget]::Machine)
        $ENV:RANGER_HDFS_HOME = "$rangerHdfsPluginPath"



        ###
        ###  Unzip Ranger HIVE Plugin from compressed archive
        ###

        Write-Log "Extracting $rangerHivePluginFile.zip to $rangerInstallPath"

        if ( Test-Path ENV:UNZIP_CMD )
        {
            ### Use external unzip command if given
            $unzipExpr = $ENV:UNZIP_CMD.Replace("@SRC", "`"$HDP_RESOURCES_DIR\$rangerHivePluginFile.zip`"")
            $unzipExpr = $unzipExpr.Replace("@DEST", "`"$rangerInstallPath`"")
            ### We ignore the error code of the unzip command for now to be
            ### consistent with prior behavior.
            Invoke-Ps $unzipExpr
        }
        else
        {
            $shellApplication = new-object -com shell.application
            $zipPackage = $shellApplication.NameSpace("$HDP_RESOURCES_DIR\$rangerHivePluginFile.zip")
            $destinationFolder = $shellApplication.NameSpace($rangerInstallPath)
            $destinationFolder.CopyHere($zipPackage.Items(), 20)
        }

        ###
        ### Set RANGER_HIVE_HOME environment variable
        ###
        Write-Log "Setting the RANGER_HIVE_HOME environment variable at machine scope to `"$rangerHivePluginPath`""
        [Environment]::SetEnvironmentVariable("RANGER_HIVE_HOME", $rangerHivePluginPath, [EnvironmentVariableTarget]::Machine)
        $ENV:RANGER_HIVE_HOME = "$rangerHivePluginPath"



        ###
        ###  Unzip Ranger HBASE Plugin from compressed archive
        ###

        Write-Log "Extracting $rangerHBasePluginFile.zip to $rangerInstallPath"

        if ( Test-Path ENV:UNZIP_CMD )
        {
            ### Use external unzip command if given
            $unzipExpr = $ENV:UNZIP_CMD.Replace("@SRC", "`"$HDP_RESOURCES_DIR\$rangerHBasePluginFile.zip`"")
            $unzipExpr = $unzipExpr.Replace("@DEST", "`"$rangerInstallPath`"")
            ### We ignore the error code of the unzip command for now to be
            ### consistent with prior behavior.
            Invoke-Ps $unzipExpr
        }
        else
        {
            $shellApplication = new-object -com shell.application
            $zipPackage = $shellApplication.NameSpace("$HDP_RESOURCES_DIR\$rangerHBasePluginFile.zip")
            $destinationFolder = $shellApplication.NameSpace($rangerInstallPath)
            $destinationFolder.CopyHere($zipPackage.Items(), 20)
        }

        ###
        ### Set RANGER_HBASE_HOME environment variable
        ###
        Write-Log "Setting the RANGER_HBASE_HOME environment variable at machine scope to `"$rangerHBasePluginPath`""
        [Environment]::SetEnvironmentVariable("RANGER_HBASE_HOME", $rangerHBasePluginPath, [EnvironmentVariableTarget]::Machine)
        $ENV:RANGER_HBASE_HOME = "$rangerHBasePluginPath"

        ###
        ###  Unzip Ranger Knox Plugin from compressed archive
        ###

        Write-Log "Extracting $rangerKnoxPluginFile.zip to $rangerInstallPath"

        if ( Test-Path ENV:UNZIP_CMD )
        {
            ### Use external unzip command if given
            $unzipExpr = $ENV:UNZIP_CMD.Replace("@SRC", "`"$HDP_RESOURCES_DIR\$rangerKnoxPluginFile.zip`"")
            $unzipExpr = $unzipExpr.Replace("@DEST", "`"$rangerInstallPath`"")
            ### We ignore the error code of the unzip command for now to be
            ### consistent with prior behavior.
            Invoke-Ps $unzipExpr
        }
        else
        {
            $shellApplication = new-object -com shell.application
            $zipPackage = $shellApplication.NameSpace("$HDP_RESOURCES_DIR\$rangerKnoxPluginFile.zip")
            $destinationFolder = $shellApplication.NameSpace($rangerInstallPath)
            $destinationFolder.CopyHere($zipPackage.Items(), 20)
        }

        ###
        ### Set RANGER_KNOX_HOME environment variable
        ###
        Write-Log "Setting the RANGER_KNOX_HOME environment variable at machine scope to `"$rangerKnoxPluginPath`""
        [Environment]::SetEnvironmentVariable("RANGER_KNOX_HOME", $rangerKnoxPluginPath, [EnvironmentVariableTarget]::Machine)
        $ENV:RANGER_KNOX_HOME = "$rangerKnoxPluginPath"

        ###
        ###  Unzip Ranger Storm Plugin from compressed archive
        ###

        Write-Log "Extracting $rangerStormPluginFile.zip to $rangerInstallPath"

        if ( Test-Path ENV:UNZIP_CMD )
        {
            ### Use external unzip command if given
            $unzipExpr = $ENV:UNZIP_CMD.Replace("@SRC", "`"$HDP_RESOURCES_DIR\$rangerStormPluginFile.zip`"")
            $unzipExpr = $unzipExpr.Replace("@DEST", "`"$rangerInstallPath`"")
            ### We ignore the error code of the unzip command for now to be
            ### consistent with prior behavior.
            Invoke-Ps $unzipExpr
        }
        else
        {
            $shellApplication = new-object -com shell.application
            $zipPackage = $shellApplication.NameSpace("$HDP_RESOURCES_DIR\$rangerStormPluginFile.zip")
            $destinationFolder = $shellApplication.NameSpace($rangerInstallPath)
            $destinationFolder.CopyHere($zipPackage.Items(), 20)
        }

        ###
        ### Set RANGER_STORM_HOME environment variable
        ###
        Write-Log "Setting the RANGER_STORM_HOME environment variable at machine scope to `"$rangerStormPluginPath`""
        [Environment]::SetEnvironmentVariable("RANGER_STORM_HOME", $rangerStormPluginPath, [EnvironmentVariableTarget]::Machine)
        $ENV:RANGER_STORM_HOME = "$rangerStormPluginPath"

        ###
        ###  Unzip Ranger UserSync from compressed archive
        ###

        Write-Log "Extracting $rangerUserSyncFile.zip to $rangerInstallPath"

        if ( Test-Path ENV:UNZIP_CMD )
        {
            ### Use external unzip command if given
            $unzipExpr = $ENV:UNZIP_CMD.Replace("@SRC", "`"$HDP_RESOURCES_DIR\$rangerUserSyncFile.zip`"")
            $unzipExpr = $unzipExpr.Replace("@DEST", "`"$rangerInstallPath`"")
            ### We ignore the error code of the unzip command for now to be
            ### consistent with prior behavior.
            Invoke-Ps $unzipExpr
        }
        else
        {
            $shellApplication = new-object -com shell.application
            $zipPackage = $shellApplication.NameSpace("$HDP_RESOURCES_DIR\$rangerUserSyncFile.zip")
            $destinationFolder = $shellApplication.NameSpace($rangerInstallPath)
            $destinationFolder.CopyHere($zipPackage.Items(), 20)
        }

        ###
        ### Set RANGER_USERSYNC_HOME environment variable
        ###
        Write-Log "Setting the RANGER_USERSYNC_HOME environment variable at machine scope to `"$rangerUserSyncPath`""
        [Environment]::SetEnvironmentVariable("RANGER_USERSYNC_HOME", $rangerUserSyncPath, [EnvironmentVariableTarget]::Machine)
        $ENV:RANGER_USERSYNC_HOME = "$rangerUserSyncPath"

}



###############################################################################
###
### Uninstalls Hadoop component.
###
### Arguments:
###     component: Component to be uninstalled, it can be "core, "hdfs" or "mapreduce"
###     nodeInstallRoot: Install folder (for example "C:\Hadoop")
###
###############################################################################

function Uninstall(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot
    )
{
    if ( $component -eq "ranger" )
    {
        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"

	    Write-Log "Uninstalling ranger $FinalName"
	    $rangerInstallPath = Join-Path $nodeInstallRoot $FinalName

        ### If Ranger Core root does not exist exit early
        if ( -not (Test-Path $rangerInstallPath) )
        {
            return
        }

		### Stop and delete services
        ###
        foreach( $service in @("ranger-admin", "ranger-usersync"))
        {
            StopAndDeleteHadoopService $service
        }

	    ###
	    ### Delete install dir
	    ###
	    $cmd = "rd /s /q `"$rangerInstallPath`""
	    Invoke-Cmd $cmd

        ### Removing RANGER_HOME environment variable
        Write-Log "Removing the RANGER_HOME environment variable"
        [Environment]::SetEnvironmentVariable( "RANGER_HOME", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Removing the RANGER_ADMIN_HOME environment variable"
        [Environment]::SetEnvironmentVariable( "RANGER_ADMIN_HOME", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Removing the RANGER_HDFS_HOME environment variable"
        [Environment]::SetEnvironmentVariable( "RANGER_HDFS_HOME", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Removing the RANGER_HBASE_HOME environment variable"
        [Environment]::SetEnvironmentVariable( "RANGER_HBASE_HOME", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Removing the RANGER_HIVE_HOME environment variable"
        [Environment]::SetEnvironmentVariable( "RANGER_HIVE_HOME", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Removing the RANGER_KNOX_HOME environment variable"
        [Environment]::SetEnvironmentVariable( "RANGER_KNOX_HOME", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Removing the RANGER_STORM_HOME environment variable"
        [Environment]::SetEnvironmentVariable( "RANGER_STORM_HOME", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Removing the RANGER_USERSYNC_HOME environment variable"
        [Environment]::SetEnvironmentVariable( "RANGER_USERSYNC_HOME", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Removing the RANGER_ADMIN_CRED_KEYSTORE_FILE environment variable"
        [Environment]::SetEnvironmentVariable( "RANGER_ADMIN_CRED_KEYSTORE_FILE", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Removing the RANGER_HDFS_CRED_KEYSTORE_FILE environment variable"
        [Environment]::SetEnvironmentVariable( "RANGER_HDFS_CRED_KEYSTORE_FILE", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Removing the RANGER_HIVE_CRED_KEYSTORE_FILE environment variable"
        [Environment]::SetEnvironmentVariable( "RANGER_HIVE_CRED_KEYSTORE_FILE", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Removing the RANGER_HBASE_CRED_KEYSTORE_FILE environment variable"
        [Environment]::SetEnvironmentVariable( "RANGER_HBASE_CRED_KEYSTORE_FILE", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Removing the RANGER_KNOX_CRED_KEYSTORE_FILE environment variable"
        [Environment]::SetEnvironmentVariable( "RANGER_KNOX_CRED_KEYSTORE_FILE", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Removing the RANGER_STORM_CRED_KEYSTORE_FILE environment variable"
        [Environment]::SetEnvironmentVariable( "RANGER_STORM_CRED_KEYSTORE_FILE", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Removing the RANGER_USERSYNC_CRED_KEYSTORE_FILE environment variable"
        [Environment]::SetEnvironmentVariable( "RANGER_USERSYNC_CRED_KEYSTORE_FILE", $null, [EnvironmentVariableTarget]::Machine )


        Write-Log "Successfully uninstalled ranger"

    }
    else
    {
        throw "Uninstall: Unsupported component argument."
    }
}

###############################################################################
###
### Start component services.
###
### Arguments:
###     component: Component name
###     roles: List of space separated service to start
###
###############################################################################
function StartService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $roles
    )
{
    Write-Log "Starting `"$component`" `"$roles`" services"

    if ( $component -eq "ranger" )
    {
        Write-Log "StartService: ranger services"
		CheckRole $roles @("ranger-admin","ranger-usersync")

        foreach ( $role in $roles -Split("\s+") )
        {
            Write-Log "Starting $role service"
            Start-Service $role
        }
    }
    else
    {
        throw "StartService: Unsupported component argument."
    }
}

###############################################################################
###
### Stop component services.
###
### Arguments:
###     component: Component name
###     roles: List of space separated service to stop
###
###############################################################################
function StopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $roles
    )
{
    Write-Log "Stopping `"$component`" `"$roles`" services"

    if ( $component -eq "ranger" )
    {
        ### Verify that roles are in the supported set
        CheckRole $roles @("ranger-admin", "ranger-usersync")
        foreach ( $role in $roles -Split("\s+") )
        {
            try
            {
                Write-Log "Stopping $role "
                if (Get-Service "$role" -ErrorAction SilentlyContinue)
                {
                    Write-Log "Service $role exists, stopping it"
                    Stop-Service $role
                }
                else
                {
                    Write-Log "Service $role does not exist, moving to next"
                }
            }
            catch [Exception]
            {
                Write-Host "Can't stop service $role"
            }

        }
    }
    else
    {
        throw "StartService: Unsupported component argument."
    }
}


###############################################################################
###
### Alters the configuration of the ranger component.
###
### Arguments:
###     component: Component to be configured, it should be "ranger"
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     configs:
###
###############################################################################
function Configure(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$false )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=3 )]
    $configs = @{},
    [bool]
    [parameter( Position=4 )]
    $aclAllFolders = $True
    )
{

    if ( $component -eq "ranger" )
    {
        Write-Log "Configure: ranger does not have any configurations"
		### TODO
    }
	elseif ( $component -eq "ranger-hdfs" )
    {
		Write-Log "Configuring Ranger HDFS Plugin"
        ConfigureRangerHdfs $nodeInstallRoot $serviceCredential $configs $aclAllFolders
    }
	elseif ( $component -eq "ranger-hive" )
    {
		Write-Log "Configuring Ranger Hive Plugin"
        ConfigureRangerHive $nodeInstallRoot $serviceCredential $configs $aclAllFolders
    }
	elseif ( $component -eq "ranger-hbase" )
    {
		Write-Log "Configuring Ranger HBase Plugin"
        ConfigureRangerHbase $nodeInstallRoot $serviceCredential $configs $aclAllFolders
    }
	elseif ( $component -eq "ranger-knox" )
    {
		Write-Log "Configuring Ranger Knox Plugin"
        ConfigureRangerKnox $nodeInstallRoot $serviceCredential $configs $aclAllFolders
    }
	elseif ( $component -eq "ranger-storm" )
    {
		Write-Log "Configuring Ranger Storm Plugin"
        ConfigureRangerStorm $nodeInstallRoot $serviceCredential $configs $aclAllFolders
    }
    elseif ( $component -eq "ranger-usersync" )
    {
		Write-Log "Configuring Ranger User Sync Plugin"
        ConfigureRangerUserSync $nodeInstallRoot $serviceCredential $configs $aclAllFolders
    }
    else
    {
        throw "Configure: Unsupported component argument."
    }
}

###############################################################################
###
### Alters the configuration of the Hadoop HDFS component for Ranger.
###
### Arguments:
###   See Configure
###############################################################################
function ConfigureRangerHdfs(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$false )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=2 )]
    $configs = @{},
    [bool]
    [parameter( Position=3 )]
    $aclAllFolders = $True
    )
{

	Write-Log "Modifying hadoop-env.cmd to invoke xasecure-hadoop-env.cmd"
    $file = Join-Path $ENV:HADOOP_CONF_DIR "hadoop-env.cmd"

    #$line = "`set HADOOP_NAMENODE_OPTS= -javaagent:%HADOOP_HOME%\share\hadoop\common\lib\hdfs-agent-@ranger.version@.jar=authagent  %HADOOP_NAMENODE_OPTS%"
    $line = "`if exist %HADOOP_CONF_DIR%\xasecure-hadoop-env.cmd CALL %HADOOP_CONF_DIR%\xasecure-hadoop-env.cmd"
	#TODO:WINDOWS Should we guard against option already being present?
    Add-Content $file $line

	### Regenerate the namenode.xml file
	$service = "namenode"
	Write-Log "Regenerating service config ${ENV:HADOOP_HOME}\$service.xml"
	$cmd = "$ENV:HADOOP_HOME\bin\hdfs.cmd --service $service > `"$ENV:HADOOP_HOME\bin\$service.xml`""
	Invoke-CmdChk $cmd

    ###
    ### Apply configuration changes to hdfs-site.xml
    ###
	$xmlFile = Join-Path $ENV:HADOOP_CONF_DIR "hdfs-site.xml"
    UpdateXmlConfig $xmlFile $configs["hdfsChanges"]

    ###
    ### Apply configuration changes to xasecure-audit.xml
    ###
    $xmlFile = Join-Path $ENV:HADOOP_CONF_DIR "xasecure-audit.xml"
    UpdateXmlConfig $xmlFile $configs["hdfsAuditChanges"]

    ###
    ### Apply configuration changes to xasecure-hdfs-security.xml
    ###
    $xmlFile = Join-Path $ENV:HADOOP_CONF_DIR "xasecure-hdfs-security.xml"
    UpdateXmlConfig $xmlFile $configs["hdfsSecurityChanges"]



 }

###############################################################################
###
### Alters the configuration of the Hadoop Hive component for Ranger.
###
### Arguments:
###   See Configure
###############################################################################
function ConfigureRangerHive(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$false )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=2 )]
    $configs = @{},
    [bool]
    [parameter( Position=3 )]
    $aclAllFolders = $True
    )
{

	### Regenerate the namenode.xml file
	$service = "hiveserver2"
	Write-Log "Regenerating service config ${ENV:HIVE_HOME}\bin\$service.xml"
	$cmd = "$ENV:HIVE_HOME\bin\hive.cmd --service $service catservicexml > `"$ENV:HIVE_HOME\bin\$service.xml`""
	Invoke-CmdChk $cmd

    ###
    ### Apply configuration changes to hive-site.xml
    ###
	# NOT SUPPORTED post Champlain
	#$xmlFile = Join-Path $ENV:HIVE_CONF_DIR "hive-site.xml"
    #UpdateXmlConfig $xmlFile $configs["hivechanges"]

    ###
    ### Apply configuration changes to hiveserver2-site.xml
    ###
    $xmlFile = Join-Path $ENV:HIVE_HOME "conf\hiveserver2-site.xml"
    UpdateXmlConfig $xmlFile $configs["hiveServerChanges"]

    ###
    ### Apply configuration changes to xasecure-hive-security.xml
    ###
    $xmlFile = Join-Path $ENV:HIVE_HOME "conf\xasecure-hive-security.xml"
    UpdateXmlConfig $xmlFile $configs["hiveSecurityChanges"]

    ###
    ### Apply configuration changes to xasecure-audit.xml
    ###
    $xmlFile = Join-Path $ENV:HIVE_HOME "conf\xasecure-audit.xml"
    UpdateXmlConfig $xmlFile $configs["hiveAuditChanges"]

 }


###############################################################################
###
### Alters the configuration of the Hadoop HBase component for Ranger.
###
### Arguments:
###   See Configure
###############################################################################
function ConfigureRangerHbase(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$false )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=2 )]
    $configs = @{},
    [bool]
    [parameter( Position=3 )]
    $aclAllFolders = $True
    )
{

    ###
    ### Apply configuration changes to hbase-site.xml
    ###
	$xmlFile = Join-Path $ENV:HBASE_CONF_DIR "hbase-site.xml"
    UpdateXmlConfig $xmlFile $configs["hbaseChanges"]

    ###
    ### Apply configuration changes to xasecure-hbase-security.xml
    ###
    $xmlFile = Join-Path $ENV:HBASE_CONF_DIR "xasecure-hbase-security.xml"
    UpdateXmlConfig $xmlFile $configs["hbaseSecurityChanges"]

    ###
    ### Apply configuration changes to xasecure-audit.xml
    ###
    $xmlFile = Join-Path $ENV:HBASE_CONF_DIR "xasecure-audit.xml"
    UpdateXmlConfig $xmlFile $configs["hbaseAuditChanges"]


 }



###############################################################################
###
### Alters the configuration of the Hadoop Knox component for Ranger.
###
### Arguments:
###   See Configure
###############################################################################
function ConfigureRangerKnox(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$false )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=2 )]
    $configs = @{},
    [bool]
    [parameter( Position=3 )]
    $aclAllFolders = $True
    )
{

    ###
    ### Apply configuration changes to xasecure-hbase-security.xml
    ###
    $xmlFile = Join-Path $ENV:KNOX_HOME "conf\xasecure-knox-security.xml"
    UpdateXmlConfig $xmlFile $configs["knoxSecurityChanges"]

    ###
    ### Apply configuration changes to xasecure-audit.xml
    ###
    $xmlFile = Join-Path $ENV:KNOX_HOME "conf\xasecure-audit.xml"
    UpdateXmlConfig $xmlFile $configs["knoxAuditChanges"]

	### TODO: Find a better way
	$path = Join-Path $ENV:KNOX_HOME "conf\topologies"
	Get-ChildItem -recurse -path $path -filter '*.xml' | % {
		ReplaceString $_.FullName 'AclsAuthz' 'XASecurePDPKnox'
	}

 }


###############################################################################
###
### Alters the configuration of the Hadoop Storm component for Ranger.
###
### Arguments:
###   See Configure
###############################################################################
function ConfigureRangerStorm(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$false )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=2 )]
    $configs = @{},
    [bool]
    [parameter( Position=3 )]
    $aclAllFolders = $True
    )
{

    ###
    ### Apply configuration changes to xasecure-hbase-security.xml
    ###
    $xmlFile = Join-Path $ENV:STORM_HOME "conf\xasecure-storm-security.xml"
    UpdateXmlConfig $xmlFile $configs["stormSecurityChanges"]

    ###
    ### Apply configuration changes to xasecure-audit.xml
    ###
    $xmlFile = Join-Path $ENV:STORM_HOME "conf\xasecure-audit.xml"
    UpdateXmlConfig $xmlFile $configs["stormAuditChanges"]


 }



###############################################################################
###
### Alters the configuration of the Hadoop UserSync service for Ranger.
###
### Arguments:
###   See Configure
###############################################################################
function ConfigureRangerUserSync(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$false )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=2 )]
    $configs = @{},
    [bool]
    [parameter( Position=3 )]
    $aclAllFolders = $True
    )
{

    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-$HadoopCoreVersion.winpkg.log" $ENV:WINPKG_BIN

    #TODO:WINDOWS Check if appropriate dirs are present and env set
    #if( -not (Test-Path $hadoopInstallToDir ))
    #{
    #    throw "ConfigureRangerHdfs: Install must be called before ConfigureRangerHdfs"
    #}

    #Write-Log "Modifying hadoop-env.cmd to invoke ranger-usersync-hadoop-env.cmd"
    #$file = Join-Path $ENV:HADOOP_CONF_DIR "hadoop-env.cmd"
    $RANGER_USERSYNC_CONF_DIR = Join-Path $ENV:RANGER_USERSYNC_HOME "conf"
    $file = Join-Path  $RANGER_USERSYNC_CONF_DIR "unixauthservice.properties"

    #TODO:WINDOWS Should we guard against option already being present?

    $prop       = "usergroupSync.policymanager.baseURL"
    $propVal    = $ENV:RANGER_EXTERNAL_URL
    ReplacePropertyVal $file $prop $propVal

    $prop       = "usergroupSync.sleepTimeInMillisBetweenSyncCycle"
    $propVal    = $ENV:RANGER_SYNC_INTERVAL
    ReplacePropertyVal $file $prop $propVal

    ##Not there in ENV vars
    if($ENV:SYNCSOURCE.ToUpper() -eq 'LDAP') {
        $prop       = "usergroupSync.source.impl.class"
        $propVal    = "org.apache.ranger.ldapusersync.process.LdapUserGroupBuilder"
    }elseif($ENV:SYNCSOURCE.ToUpper() -eq 'UNIX') {
        $prop       = "usergroupSync.source.impl.class"
        $propVal    = "org.apache.ranger.unixusersync.process.UnixUserGroupBuilder"
    }else{
        $prop       = "usergroupSync.source.impl.class"
        $propVal    = "org.apache.ranger.unixusersync.process.UnixUserGroupBuilder"
    }
    ReplacePropertyVal $file $prop $propVal

    $prop       = "ldapGroupSync.ldapUrl"
    $propVal    = $ENV:RANGER_SYNC_LDAP_URL
    ReplacePropertyVal $file $prop $propVal

    $prop       = "ldapGroupSync.ldapBindDn"
    $propVal    = $ENV:RANGER_SYNC_LDAP_BIND_DN
    ReplacePropertyVal $file $prop $propVal

    $prop       = "ldapGroupSync.ldapBindPassword"
    $propVal    = "_" #$ENV:RANGER_SYNC_LDAP_BIND_PASSWORD
    ReplacePropertyVal $file $prop $propVal

    ##Not there in ENV vars
    $prop       = "ldapGroupSync.ldapBindKeystore"
    $propVal    = $ENV:RANGER_USERSYNC_CRED_KEYSTORE_FILE
    ReplacePropertyVal $file $prop $propVal

    ##Not there in ENV vars
    $prop       = "ldapGroupSync.ldapBindAlias"
    $propVal    = "ldap.bind.password"
    ReplacePropertyVal $file $prop $propVal

    ##Not there in ENV vars
    $prop       = "ldapGroupSync.userSearchBase"
    $propVal    = $ENV:RANGER_SYNC_LDAP_USER_SEARCH_BASE
    ReplacePropertyVal $file $prop $propVal

    $prop       = "ldapGroupSync.userSearchScope"
    $propVal    = $ENV:RANGER_SYNC_LDAP_USER_SEARCH_SCOPE
    ReplacePropertyVal $file $prop $propVal

    $prop       = "ldapGroupSync.userObjectClass"
    $propVal    = $ENV:RANGER_SYNC_LDAP_USER_OBJECT_CLASS
    ReplacePropertyVal $file $prop $propVal

    $prop       = "ldapGroupSync.userObjectClass"
    $propVal    = $ENV:RANGER_SYNC_LDAP_USER_OBJECT_CLASS
    ReplacePropertyVal $file $prop $propVal

    $prop       = "ldapGroupSync.userNameAttribute"
    $propVal    = $ENV:RANGER_SYNC_LDAP_USER_NAME_ATTRIBUTE
    ReplacePropertyVal $file $prop $propVal

    $prop       = "ldapGroupSync.userGroupNameAttribute"
    $propVal    = $ENV:RANGER_SYNC_LDAP_USER_GROUP_NAME_ATTRIBUTE
    ReplacePropertyVal $file $prop $propVal

    $prop       = "ldapGroupSync.username.caseConversion"
    $propVal    = $ENV:RANGER_SYNC_LDAP_USERNAME_CASE_CONVERSION
    ReplacePropertyVal $file $prop $propVal

    $prop       = "ldapGroupSync.groupname.caseConversion"
    $propVal    = $ENV:RANGER_SYNC_LDAP_GROUPNAME_CASE_CONVERSION
    ReplacePropertyVal $file $prop $propVal

    #$prop       = "ldap.bind.password"
    #$propVal    = $ENV:SYNC_LDAP_BIND_ALIAS
    #ReplacePropertyVal $file $prop $propVal

 }

### Helper routing that converts a $null object to nothing. Otherwise, iterating over
### a $null object with foreach results in a loop with one $null element.
function empty-null($obj)
{
   if ($obj -ne $null) { $obj }
}

### Gives full permissions on the folder to the given user
function GiveFullPermissions(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $folder,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $username,
    [bool]
    [Parameter( Position=2, Mandatory=$false )]
    $recursive = $false)
{
    Write-Log "Giving user/group `"$username`" full permissions to `"$folder`""
	### Give /inheritance:e because jceks files in the jceks foler is not
	### getting it by default and hence decrypting of keystore alias is failing!
    $cmd = "icacls `"$folder`" /inheritance:e /grant ${username}:(OI)(CI)F"
    if ($recursive) {
        $cmd += " /T"
    }
    Invoke-CmdChk $cmd
}

### Checks if the given space separated roles are in the given array of
### supported roles.
function CheckRole(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $roles,
    [array]
    [parameter( Position=1, Mandatory=$true )]
    $supportedRoles
    )
{
    foreach ( $role in $roles.Split(" ") )
    {
        if ( -not ( $supportedRoles -contains $role ) )
        {
            throw "CheckRole: Passed in role `"$role`" is outside of the supported set `"$supportedRoles`""
        }
    }
}

### Creates and configures the service.
function CreateAndConfigureHadoopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $service,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $hdpResourcesDir,
    [String]
    [Parameter( Position=2, Mandatory=$true )]
    $serviceBinDir,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=3, Mandatory=$true )]
    $serviceCredential
)
{
    if ( -not ( Get-Service "$service" -ErrorAction SilentlyContinue ) )
    {
		 Write-Log "Creating service `"$service`" as $serviceBinDir\$service.exe"
        $xcopyServiceHost_cmd = "copy /Y `"$HDP_RESOURCES_DIR\serviceHost.exe`" `"$serviceBinDir\$service.exe`""
        Invoke-CmdChk $xcopyServiceHost_cmd

        #Creating the event log needs to be done from an elevated process, so we do it here
        if( -not ([Diagnostics.EventLog]::SourceExists( "$service" )))
        {
            [Diagnostics.EventLog]::CreateEventSource( "$service", "" )
        }

        Write-Log "Adding service $service"
        $s = New-Service -Name "$service" -BinaryPathName "$serviceBinDir\$service.exe" -Credential $serviceCredential -DisplayName "Apache Hadoop $service"
        if ( $s -eq $null )
        {
            throw "CreateAndConfigureHadoopService: Service `"$service`" creation failed"
        }

        $cmd="$ENV:WINDIR\system32\sc.exe failure $service reset= 30 actions= restart/5000"
        Invoke-CmdChk $cmd

        $cmd="$ENV:WINDIR\system32\sc.exe config $service start= demand"
        Invoke-CmdChk $cmd

        Set-ServiceAcl $service
    }
    else
    {
        Write-Log "Service `"$service`" already exists, Removing `"$service`""
        StopAndDeleteHadoopService $service
        CreateAndConfigureHadoopService $service $hdpResourcesDir $serviceBinDir $serviceCredential
    }
}

### Stops and deletes the Hadoop service.
function StopAndDeleteHadoopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $service
)
{
    Write-Log "Stopping $service"
    $s = Get-Service $service -ErrorAction SilentlyContinue

    if( $s -ne $null )
    {
        Stop-Service $service
        $cmd = "sc.exe delete $service"
        Invoke-Cmd $cmd
    }
}

### Helper routine that converts a $null object to nothing. Otherwise, iterating over
### a $null object with foreach results in a loop with one $null element.
function empty-null($obj)
{
   if ($obj -ne $null) { $obj }
}

### Helper routine that updates the given fileName XML file with the given
### key/value configuration values. The XML file is expected to be in the
### Hadoop format. For example:
### <configuration>
###   <property>
###     <name.../><value.../>
###   </property>
### </configuration>
function UpdateXmlConfig(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $fileName,
    [hashtable]
    [parameter( Position=1 )]
    $config = @{} )
{
    $xml = New-Object System.Xml.XmlDocument
    $xml.PreserveWhitespace = $true
    $xml.Load($fileName)

    foreach( $key in empty-null $config.Keys )
    {
        $value = $config[$key]
        $found = $False
        $xml.SelectNodes('/configuration/property') | ? { $_.name -eq $key } | % { $_.value = $value; $found = $True }
        if ( -not $found )
        {
            $xml["configuration"].AppendChild($xml.CreateWhitespace("`r`n  ")) | Out-Null
            $newItem = $xml.CreateElement("property")
            $newItem.AppendChild($xml.CreateWhitespace("`r`n    ")) | Out-Null
            $newItem.AppendChild($xml.CreateElement("name")) | Out-Null
            $newItem.AppendChild($xml.CreateWhitespace("`r`n    ")) | Out-Null
            $newItem.AppendChild($xml.CreateElement("value")) | Out-Null
            $newItem.AppendChild($xml.CreateWhitespace("`r`n  ")) | Out-Null
            $newItem.name = $key
            $newItem.value = $value
            $xml["configuration"].AppendChild($newItem) | Out-Null
            $xml["configuration"].AppendChild($xml.CreateWhitespace("`r`n")) | Out-Null
        }
    }

    $xml.Save($fileName)
    $xml.ReleasePath
}

### Helper routine that replaces string in file
function ReplaceString($file,$find,$replace)
{
    $content = Get-Content $file
    for ($i=1; $i -le $content.Count; $i++)
    {
        if ($content[$i] -like "*$find*")
        {
            $content[$i] = $content[$i].Replace($find, $replace)
        }
    }
    Set-Content -Value $content -Path $file -Force
}

### Helper routine that replaces a property value in a file
function ReplacePropertyVal($file,$findProp,$replaceVal)
{
    $content = Get-Content $file
    for ($i=1; $i -le $content.Count; $i++)
    {
        if($content[$i])
        {
            $prop = $content[$i].Split('=')[0]
            if ($prop.trim() -eq $findProp )
            {
                $content[$i]= ""
                $updatedContent = "$findProp = $replaceVal"
                $content[$i] = $updatedContent
            }
        }
    }
    Set-Content -Value $content -Path $file -Force
}

### Function to create jceks credential file store using hortonworks credentialapi
function CreateJCEKS (
    [String]
    $alias,
    [String]
    $password,
	[String]
	$libPath,
    [String]
    $jceksFile
	)
{

	Write-Log "Creating alias $alias in jceks file : $jceksFile"
    $cmd = "${ENV:JAVA_HOME}\bin\java -cp `"${libPath}\*`" org.apache.ranger.credentialapi.buildks create `"${alias}`" -value `"${password}`" -provider `"jceks://file/${jceksFile}`" "
	Invoke-Cmd $cmd

}


###
### Public API
###
Export-ModuleMember -Function Install
Export-ModuleMember -Function Uninstall
Export-ModuleMember -Function Configure
Export-ModuleMember -Function StartService
Export-ModuleMember -Function StopService
###
### Private API (exposed for test only)
###
Export-ModuleMember -Function UpdateXmlConfig
