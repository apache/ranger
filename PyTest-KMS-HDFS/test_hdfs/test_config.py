HDFS_USER = "hdfs"
HIVE_USER = "hive"
KEY_ADMIN="keyadmin"
HEADERS={"Content-Type": "application/json","Accept":"application/json"}
PARAMS={"user.name":"keyadmin"}
BASE_URL="http://localhost:9292/kms/v1"
HADOOP_CONTAINER = "ranger-hadoop"
HDFS_USER = "hdfs"
KMS_CONTAINER = "ranger-kms"

#KMS configs that needs to be added in XML file------------add more if needed
KMS_PROPERTY = """<property><name>hadoop.security.key.provider.path</name><value>kms://http@host.docker.internal:9292/kms</value></property>"""

CORE_SITE_XML_PATH = "/opt/hadoop/etc/hadoop/core-site.xml"

 # Ensure PATH is set for /opt/hadoop/bin
SET_PATH_CMD="echo 'export PATH=/opt/hadoop/bin:$PATH' >> /etc/profile && export PATH=/opt/hadoop/bin:$PATH"

HADOOP_NAMENODE_LOG_PATH="/opt/hadoop/logs/hadoop-hdfs-namenode-ranger-hadoop.example.com.log"
KMS_LOG_PATH="/var/log/ranger/kms/ranger-kms-ranger-kms.example.com-root.log"


# HDFS Commands----------------------------------------------------
CREATE_KEY_COMMAND = "hadoop key create my_key -size 128 -provider kms://http@host.docker.internal:9292/kms"

VALIDATE_KEY_COMMAND = "hadoop key list -provider kms://http@host.docker.internal:9292/kms"

CREATE_EZ_COMMANDS = [
    "hdfs dfs -mkdir /secure_zone2",
    "hdfs crypto -createZone -keyName my_key -path /secure_zone2",
    "hdfs crypto -listZones"
]

GRANT_PERMISSIONS_COMMANDS = [
    "hdfs dfs -chmod 700 /secure_zone2",
    "hdfs dfs -chown hive:hive /secure_zone2"
]

HIVE_CREATE_FILE_COMMAND = 'bash -c \'echo "Hello, this is a third file!" > /home/hive/testfile2.txt && ls -l /home/hive/testfile2.txt\''

HIVE_ACTIONS_COMMANDS = [
    "hdfs dfs -put /home/hive/testfile2.txt /secure_zone2/",
    "hdfs dfs -ls /secure_zone2/",
    "hdfs dfs -cat /secure_zone2/testfile2.txt"
]

UNAUTHORIZED_WRITE_COMMAND = 'hdfs dfs -put /home/hbase/hack.txt /secure_zone2/'

UNAUTHORIZED_READ_COMMAND = "hdfs dfs -cat /secure_zone2/testfile2.txt"

CLEANUP_COMMANDS = [
    "hdfs dfs -rm /secure_zone2/testfile2.txt",
    "hdfs dfs -rm -R /secure_zone2"
]
KEY_DELETION_CMD = "bash -c \"echo 'Y' | hadoop key delete my_key -provider kms://http@host.docker.internal:9292/kms\""


