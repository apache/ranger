Build Process
=============

1. Check out the code from GIT repository

2. On the root folder, please execute the following Maven command:

	$ mvn clean compile package install assembly:assembly
    $ mvn eclise:eclipse

3. After the above build command execution, you should see the following TAR files in the target folder:


   ranger-<version>-admin.tar.gz
   ranger-<version>-hbase-plugin.tar.gz
   ranger-<version>-hdfs-plugin.tar.gz
   ranger-<version>-hive-plugin.tar.gz
   ranger-<version>-kafka-plugin.tar.gz
   ranger-<version>-kms.tar.gz
   ranger-<version>-knox-plugin.tar.gz
   ranger-<version>-migration-util.tar.gz
   ranger-<version>-solr-plugin.tar.gz
   ranger-<version>-src.tar.gz
   ranger-<version>-storm-plugin.tar.gz
   ranger-<version>-usersync.tar.gz
   ranger-<version>-yarn-plugin.tar.gz

Importing Apache Ranger Project into Eclipse
============================================

1. Create a Eclipse workspace called 'ranger'

2. Import maven project from the root directory where ranger source code is downloaded (and build)


Deployment Process
==================

Installation Host Information
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
1.	Ranger Admin Tool Component  (ranger-<version-number>-admin.tar.gz) should be installed on a host where Policy Admin Tool web application runs on port 6080 (default).
2.  Ranger User Synchronization Component (ranger-<version-number>-usersync.tar.gz) should be installed on a host to synchronize the external user/group information into Ranger database via Ranger Admin Tool.
3.  Ranger Component plugin should be installed on the component boxes:
	(a)  HDFS Plugin needs to be installed on Name Node hosts
	(b)  Hive Plugin needs to be installed on HiveServer2 hosts
	(c)  HBase Plugin needs to be installed on both Master and Regional Server nodes.
	(d)  Knox Plugin needs to be installed on Knox hosts.
	(e)  Storm Plugin needs to be installed on Storm hosts.
    (f)  Kafka/Solr Plugin needs to be installed on their respective component hosts.

Installation Process
~~~~~~~~~~~~~~~~~~~~

1. Download the tar.gz file into a temporary folder in the box where it needs to be installed.

2. Expand the tar.gz file into /usr/lib/ranger/ folder

3. Go to the component name under the expanded folder (e.g. /usr/lib/ranger/ranger-<version-number>-admin/)

4. Modify the install.properties file with appropriate variables

5. If the module has setup.sh, 
	Execute ./setup.sh

   If the install.sh file does not exists, 
	Execute ./enable-<component>-plugin.sh

