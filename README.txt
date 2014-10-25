Build Process
=============

1. Check out the code from GIT repository

2. On the root folder, please execute the following Maven command:

	$ mvn clean compile package install assembly:assembly
    $ mvn eclipse:eclipse

3. After the above build command execution, you should see the following TAR files in the target folder:

	argus-<version-number>-admin.tar
	argus-<version-number>-usersync.tar
	argus-<version-number>-hdfs-agent.tar
	argus-<version-number>-hive-agent.tar
	argus-<version-number>-hbase-agent.tar
	argus-<version-number>-knox-agent.tar
	argus-<version-number>-storm-agent.tar

Importing Argus Project into Eclipse
====================================

1. Create a Eclipse workspace called 'argus'

2. Import maven project from the root directory where argus source code is downloaded (and build)


Deployment Process
==================

Installation Host Information
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
1.	Argus Admin Tool Component  (argus-<version-number>-admin.tar) should be installed on a host where Policy Admin Tool web application runs on port 6080 (default).
2.  Argus User Synchronization Component (argus-<version-number>-usersync.tar) should be installed on a host to synchronize the external user/group information into Argus database via Argus Admin Tool.
3.  Argus Component Agents (plugin) should be installed on the component boxes:
	(a)  HDFS Agent needs to be installed on Name Node hosts
	(b)  Hive Agent needs to be installed on HiveServer2 hosts
	(c)  HBase Agent needs to be installed on both Master and Regional Server nodes.
	(d)  Knox Agent needs to be installed on Knox hosts.
	(e)  Storm Agent needs to be installed on Storm hosts.

Installation Process
~~~~~~~~~~~~~~~~~~~~

1. Download the tar file into a temporary folder in the box where it needs to be installed.

2. Expand the tar file into /usr/lib/argus/ folder

3. Go to the component name under the expanded folder (e.g. /usr/lib/argus/argus-<version-number>-admin/) 

4. Modify the install.properties file with appropriate variables

5. If the module has install.sh, 
	Execute ./install.sh

   If the install.sh file does not exists, 
	Execute ./enable-<component>-agent.sh

