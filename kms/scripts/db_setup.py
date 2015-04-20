#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#

import os
import re
import sys
import errno
import shlex
import platform
import logging
import subprocess
import fileinput
from os.path import basename
from subprocess import Popen,PIPE
from datetime import date
import datetime
from time import gmtime, strftime
globalDict = {}

os_name = platform.system()
os_name = os_name.upper()

if os_name == "LINUX":
	RANGER_KMS_HOME = os.getcwd()
elif os_name == "WINDOWS":
	RANGER_KMS_HOME = os.getenv("RANGER_KMS_HOME")

def check_output(query):
	if os_name == "LINUX":
		p = subprocess.Popen(shlex.split(query), stdout=subprocess.PIPE)
	elif os_name == "WINDOWS":
		p = subprocess.Popen(query, stdout=subprocess.PIPE, shell=True)
	output = p.communicate ()[0]
	return output

def log(msg,type):
	if type == 'info':
		logging.info(" %s",msg)
	if type == 'debug':
		logging.debug(" %s",msg)
	if type == 'warning':
		logging.warning(" %s",msg)
	if type == 'exception':
		logging.exception(" %s",msg)
	if type == 'error':
		logging.error(" %s",msg)

def populate_global_dict():
	global globalDict
	if os_name == "LINUX":
		read_config_file = open(os.path.join(RANGER_KMS_HOME,'install.properties'))
	elif os_name == "WINDOWS":
		read_config_file = open(os.path.join(RANGER_KMS_HOME,'bin','install_config.properties'))
	library_path = os.path.join(RANGER_KMS_HOME,"cred","lib","*")

	for each_line in read_config_file.read().split('\n') :
		if len(each_line) == 0 : continue
		if re.search('=', each_line):
			key , value = each_line.strip().split("=",1)
			key = key.strip()
			if 'PASSWORD' in key:
				jceks_file_path = os.path.join(RANGER_KMS_HOME, 'jceks','ranger_db.jceks')
				statuscode,value = call_keystore(library_path,key,'',jceks_file_path,'get')
				if statuscode == 1:
					value = ''
			value = value.strip()
			globalDict[key] = value

def call_keystore(libpath,aliasKey,aliasValue , filepath,getorcreate):
    finalLibPath = libpath.replace('\\','/').replace('//','/')
    finalFilePath = 'jceks://file/'+filepath.replace('\\','/').replace('//','/')
    if getorcreate == 'create':
        commandtorun = ['java', '-cp', finalLibPath, 'org.apache.ranger.credentialapi.buildks' ,'create', aliasKey, '-value', aliasValue, '-provider',finalFilePath]
        p = Popen(commandtorun,stdin=PIPE, stdout=PIPE, stderr=PIPE)
        output, error = p.communicate()
        statuscode = p.returncode
        return statuscode
    elif getorcreate == 'get':
        commandtorun = ['java', '-cp', finalLibPath, 'org.apache.ranger.credentialapi.buildks' ,'get', aliasKey, '-provider',finalFilePath]
        p = Popen(commandtorun,stdin=PIPE, stdout=PIPE, stderr=PIPE)
        output, error = p.communicate()
        statuscode = p.returncode
        return statuscode, output
    else:
        print 'proper command not received for input need get or create'

class BaseDB(object):

	def check_connection(self, db_name, db_user, db_password):
		log("[I] ---------- Verifying DB connection ----------", "info")

	def check_table(self, db_name, db_user, db_password, TABLE_NAME):
		log("[I] ---------- Verifying table ----------", "info")

	def import_db_file(self, db_name, db_user, db_password, file_name):
		log("[I] ---------- Importing db schema ----------", "info")


class MysqlConf(BaseDB):
	# Constructor
	def __init__(self, host,SQL_CONNECTOR_JAR,JAVA_BIN):
		self.host = host
		self.SQL_CONNECTOR_JAR = SQL_CONNECTOR_JAR
		self.JAVA_BIN = JAVA_BIN

	def get_jisql_cmd(self, user, password ,db_name):
		#path = os.getcwd()
		path = RANGER_KMS_HOME
		self.JAVA_BIN = self.JAVA_BIN.strip("'")
		if os_name == "LINUX":
			jisql_cmd = "%s -cp %s:jisql/lib/* org.apache.util.sql.Jisql -driver mysqlconj -cstring jdbc:mysql://%s/%s -u %s -p %s -noheader -trim -c \;" %(self.JAVA_BIN,self.SQL_CONNECTOR_JAR,self.host,db_name,user,password)
		elif os_name == "WINDOWS":
			jisql_cmd = "%s -cp %s;%s\jisql\\lib\\* org.apache.util.sql.Jisql -driver mysqlconj -cstring jdbc:mysql://%s/%s -u %s -p %s -noheader -trim" %(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, path, self.host, db_name, user, password)
		return jisql_cmd

	def check_connection(self, db_name, db_user, db_password):
		log("[I] Checking connection..", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if os_name == "LINUX":
			query = get_cmd + " -query \"SELECT version();\""
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"SELECT version();\" -c ;"
		output = check_output(query)
		if output.strip('Production  |'):
			log("[I] Checking connection passed.", "info")
			return True
		else:
			log("[E] Can't establish connection!! Exiting.." ,"error")
			log("[I] Please run DB setup first or contact Administrator.." ,"info")
			sys.exit(1)


	def import_db_file(self, db_name, db_user, db_password, file_name):
		name = basename(file_name)
		if os.path.isfile(file_name):
			log("[I] Importing db schema to database " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if os_name == "LINUX":
				query = get_cmd + " -input %s" %file_name
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -input %s -c ;" %file_name
				ret = subprocess.call(query)
			if ret == 0:
				log("[I] "+name + " DB schema imported successfully","info")
			else:
				log("[E] "+name + " DB schema import failed!","error")
				sys.exit(1)
		else:
			log("[E] DB schema file " + name+ " not found","error")
			sys.exit(1)


	def check_table(self, db_name, db_user, db_password, TABLE_NAME):
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if os_name == "LINUX":
			query = get_cmd + " -query \"show tables like '%s';\"" %(TABLE_NAME)
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"show tables like '%s';\" -c ;" %(TABLE_NAME)
		output = check_output(query)
		if output.strip(TABLE_NAME + " |"):
			log("[I] Table " + TABLE_NAME +" already exists in database '" + db_name + "'","info")
			return True
		else:
			log("[I] Table " + TABLE_NAME +" does not exist in database " + db_name + "","info")
			return False


class OracleConf(BaseDB):
	# Constructor
	def __init__(self, host, SQL_CONNECTOR_JAR, JAVA_BIN):
		self.host = host 
		self.SQL_CONNECTOR_JAR = SQL_CONNECTOR_JAR
		self.JAVA_BIN = JAVA_BIN

	def get_jisql_cmd(self, user, password):
		#path = os.getcwd()
		path = RANGER_KMS_HOME
		self.JAVA_BIN = self.JAVA_BIN.strip("'")
		if os_name == "LINUX":
			jisql_cmd = "%s -cp %s:jisql/lib/* org.apache.util.sql.Jisql -driver oraclethin -cstring jdbc:oracle:thin:@%s -u '%s' -p '%s' -noheader -trim" %(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, self.host, user, password)
		elif os_name == "WINDOWS":
			jisql_cmd = "%s -cp %s;%s\jisql\\lib\\* org.apache.util.sql.Jisql -driver oraclethin -cstring jdbc:oracle:thin:@%s -u %s -p %s -noheader -trim" %(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, path, self.host, user, password)
		return jisql_cmd


	def check_connection(self, db_name, db_user, db_password):
		log("[I] Checking connection", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password)
		if os_name == "LINUX":
			query = get_cmd + " -c \; -query \"select * from v$version;\""
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"select * from v$version;\" -c ;"
		output = check_output(query)
		if output.strip('Production  |'):
			log("[I] Connection success", "info")
			return True
		else:
			log("[E] Can't establish connection!", "error")
			sys.exit(1)


	def import_db_file(self, db_name, db_user, db_password, file_name):
		name = basename(file_name)
		if os.path.isfile(file_name):
			log("[I] Importing script " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password)
			if os_name == "LINUX":
				query = get_cmd + " -input %s -c \;" %file_name
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -input %s -c ;" %file_name
				ret = subprocess.call(query)
			if ret == 0:
				log("[I] "+name + " imported successfully","info")
			else:
				log("[E] "+name + " import failed!","error")
				sys.exit(1)
		else:
			log("[E] Import " +name + " sql file not found","error")
			sys.exit(1)


	def check_table(self, db_name, db_user, db_password, TABLE_NAME):
		get_cmd = self.get_jisql_cmd(db_user ,db_password)
		if os_name == "LINUX":
			query = get_cmd + " -c \; -query 'select default_tablespace from user_users;'"
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"select default_tablespace from user_users;\" -c ;"
		output = check_output(query).strip()
		output = output.strip(' |')
		db_name = db_name.upper()
		if output == db_name:
			log("[I] User name " + db_user + " and tablespace " + db_name + " already exists.","info")
			log("[I] Verifying table " + TABLE_NAME +" in tablespace " + db_name, "info")
			get_cmd = self.get_jisql_cmd(db_user, db_password)
			if os_name == "LINUX":
				query = get_cmd + " -c \; -query \"select UPPER(table_name) from all_tables where UPPER(tablespace_name)=UPPER('%s') and UPPER(table_name)=UPPER('%s');\"" %(db_name ,TABLE_NAME)
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"select UPPER(table_name) from all_tables where UPPER(tablespace_name)=UPPER('%s') and UPPER(table_name)=UPPER('%s');\" -c ;" %(db_name ,TABLE_NAME)
			output = check_output(query)
			if output.strip(TABLE_NAME.upper() + ' |'):
				log("[I] Table " + TABLE_NAME +" already exists in tablespace " + db_name + "","info")
				return True
			else:
				log("[I] Table " + TABLE_NAME +" does not exist in tablespace " + db_name + "","info")
				return False
		else:
			log("[E] "+db_user + " user already assigned to some other tablespace , provide different DB name.","error")
			sys.exit(1)



class PostgresConf(BaseDB):
	# Constructor
	def __init__(self, host, SQL_CONNECTOR_JAR, JAVA_BIN):
		self.host = host
		self.SQL_CONNECTOR_JAR = SQL_CONNECTOR_JAR
		self.JAVA_BIN = JAVA_BIN

	def get_jisql_cmd(self, user, password, db_name):
		#TODO: User array for forming command
		#path = os.getcwd()
		path = RANGER_KMS_HOME
		self.JAVA_BIN = self.JAVA_BIN.strip("'")
		if os_name == "LINUX":
			jisql_cmd = "%s -cp %s:jisql/lib/* org.apache.util.sql.Jisql -driver postgresql -cstring jdbc:postgresql://%s:5432/%s -u %s -p %s -noheader -trim -c \;" %(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, self.host, db_name, user, password)
		elif os_name == "WINDOWS":
			jisql_cmd = "%s -cp %s;%s\jisql\\lib\\* org.apache.util.sql.Jisql -driver postgresql -cstring jdbc:postgresql://%s:5432/%s -u %s -p %s -noheader -trim" %(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, path, self.host, db_name, user, password)
		return jisql_cmd

	def check_connection(self, db_name, db_user, db_password):
		log("[I] Checking connection", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if os_name == "LINUX":
			query = get_cmd + " -query \"SELECT 1;\""
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"SELECT 1;\" -c ;"
		output = check_output(query)
		if output.strip('1 |'):
			log("[I] connection success", "info")
			return True
		else:
			log("[E] Can't establish connection", "error")
			sys.exit(1)

	def import_db_file(self, db_name, db_user, db_password, file_name):
		name = basename(file_name)
		if os.path.isfile(file_name):
			log("[I] Importing db schema to database " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if os_name == "LINUX":
				query = get_cmd + " -input %s" %file_name
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -input %s -c ;" %file_name
				ret = subprocess.call(query)
			if ret == 0:
				log("[I] "+name + " DB schema imported successfully","info")
			else:
				log("[E] "+name + " DB schema import failed!","error")
				sys.exit(1)
		else:
			log("[E] DB schema file " + name+ " not found","error")
			sys.exit(1)


	def check_table(self, db_name, db_user, db_password, TABLE_NAME):
		log("[I] Verifying table " + TABLE_NAME +" in database " + db_name, "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if os_name == "LINUX":
			query = get_cmd + " -query \"select * from (select table_name from information_schema.tables where table_catalog='%s' and table_name = '%s') as temp;\"" %(db_name , TABLE_NAME)
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"select * from (select table_name from information_schema.tables where table_catalog='%s' and table_name = '%s') as temp;\" -c ;" %(db_name , TABLE_NAME)
		output = check_output(query)
		if output.strip(TABLE_NAME +" |"):
			log("[I] Table " + TABLE_NAME +" already exists in database " + db_name, "info")
			return True
		else:
			log("[I] Table " + TABLE_NAME +" does not exist in database " + db_name, "info")
			return False


class SqlServerConf(BaseDB):
	# Constructor
	def __init__(self, host, SQL_CONNECTOR_JAR, JAVA_BIN):
		self.host = host
		self.SQL_CONNECTOR_JAR = SQL_CONNECTOR_JAR
		self.JAVA_BIN = JAVA_BIN

	def get_jisql_cmd(self, user, password, db_name):
		#TODO: User array for forming command
		#path = os.getcwd()
		path = RANGER_KMS_HOME
		self.JAVA_BIN = self.JAVA_BIN.strip("'")
		if os_name == "LINUX":
			jisql_cmd = "%s -cp %s:jisql/lib/* org.apache.util.sql.Jisql -user %s -password %s -driver mssql -cstring jdbc:sqlserver://%s:1433\\;databaseName=%s -noheader -trim"%(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, user, password, self.host,db_name)
		elif os_name == "WINDOWS":
			jisql_cmd = "%s -cp %s;%s\\jisql\\lib\\* org.apache.util.sql.Jisql -user %s -password %s -driver mssql -cstring jdbc:sqlserver://%s:1433;databaseName=%s -noheader -trim"%(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, path, user, password, self.host,db_name)
		return jisql_cmd

	def check_connection(self, db_name, db_user, db_password):
		log("[I] Checking connection", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if os_name == "LINUX":
			query = get_cmd + " -c \; -query \"SELECT 1;\""
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"SELECT 1;\" -c ;"
		output = check_output(query)
		if output.strip('1 |'):
			log("[I] Connection success", "info")
			return True
		else:
			log("[E] Can't establish connection", "error")
			sys.exit(1)

	def import_db_file(self, db_name, db_user, db_password, file_name):
		name = basename(file_name)
		if os.path.isfile(file_name):
			log("[I] Importing db schema to database " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if os_name == "LINUX":
				query = get_cmd + " -input %s" %file_name
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -input %s" %file_name
				ret = subprocess.call(query)
			if ret == 0:
				log("[I] "+name + " DB schema imported successfully","info")
			else:
				log("[E] "+name + " DB Schema import failed!","error")
				sys.exit(1)
		else:
			log("[I] DB Schema file " + name+ " not found","error")
			sys.exit(1)

	def check_table(self, db_name, db_user, db_password, TABLE_NAME):
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if os_name == "LINUX":
			query = get_cmd + " -c \; -query \"SELECT TABLE_NAME FROM information_schema.tables where table_name = '%s';\"" %(TABLE_NAME)
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"SELECT TABLE_NAME FROM information_schema.tables where table_name = '%s';\" -c ;" %(TABLE_NAME)
		output = check_output(query)
		if output.strip(TABLE_NAME + " |"):
			log("[I] Table '" + TABLE_NAME + "' already exists in  database '" + db_name + "'","info")
			return True
		else:
			log("[I] Table '" + TABLE_NAME + "' does not exist in database '" + db_name + "'","info")
			return False


def main(argv):
	populate_global_dict()

	FORMAT = '%(asctime)-15s %(message)s'
	logging.basicConfig(format=FORMAT, level=logging.DEBUG)

	if os.environ['JAVA_HOME'] == "":
		log("[E] ---------- JAVA_HOME environment property not defined, aborting installation. ----------", "error")
		sys.exit(1)
	else:
		JAVA_BIN=os.path.join(os.environ['JAVA_HOME'],'bin','java')
	if os_name == "WINDOWS" :
		JAVA_BIN = JAVA_BIN+'.exe'
	if os.path.isfile(JAVA_BIN):
		pass
	else:
		JAVA_BIN=globalDict['JAVA_BIN']
		if os.path.isfile(JAVA_BIN):
			pass
		else:
			log("[E] ---------- JAVA Not Found, aborting installation. ----------", "error")
			sys.exit(1)
	XA_DB_FLAVOR = globalDict['DB_FLAVOR']
	XA_DB_FLAVOR = XA_DB_FLAVOR.upper()

	log("[I] DB FLAVOR :" + XA_DB_FLAVOR ,"info")
	xa_db_host = globalDict['db_host']

	mysql_core_file = globalDict['mysql_core_file']
	mysql_patches = os.path.join('db','mysql','patches')

	oracle_core_file = globalDict['oracle_core_file'] 
	oracle_patches = os.path.join('db','oracle','patches')

	postgres_core_file = globalDict['postgres_core_file']
	postgres_patches = os.path.join('db','postgres','patches')

	sqlserver_core_file = globalDict['sqlserver_core_file']
	sqlserver_patches = os.path.join('db','sqlserver','patches')

	db_name = globalDict['db_name']
	db_user = globalDict['db_user']
	db_password = globalDict['db_password']

	x_db_version = 'x_db_version_h'
	x_user = 'ranger_masterkey'


	if XA_DB_FLAVOR == "MYSQL":
		MYSQL_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
		xa_sqlObj = MysqlConf(xa_db_host, MYSQL_CONNECTOR_JAR, JAVA_BIN)
		xa_db_core_file = os.path.join(RANGER_KMS_HOME , mysql_core_file)
		
	elif XA_DB_FLAVOR == "ORACLE":
		ORACLE_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
		xa_sqlObj = OracleConf(xa_db_host, ORACLE_CONNECTOR_JAR, JAVA_BIN)
		xa_db_core_file = os.path.join(RANGER_KMS_HOME ,oracle_core_file)

	elif XA_DB_FLAVOR == "POSTGRES":
		POSTGRES_CONNECTOR_JAR = globalDict['SQL_CONNECTOR_JAR']
		xa_sqlObj = PostgresConf(xa_db_host, POSTGRES_CONNECTOR_JAR, JAVA_BIN)
		xa_db_core_file = os.path.join(RANGER_KMS_HOME , postgres_core_file)

	elif XA_DB_FLAVOR == "SQLSERVER":
		SQLSERVER_CONNECTOR_JAR = globalDict['SQL_CONNECTOR_JAR']
		xa_sqlObj = SqlServerConf(xa_db_host, SQLSERVER_CONNECTOR_JAR, JAVA_BIN)
		xa_db_core_file = os.path.join(RANGER_KMS_HOME , sqlserver_core_file)
	else:
		log("[E] --------- NO SUCH SUPPORTED DB FLAVOUR!! ---------", "error")
		sys.exit(1)

#	'''

	log("[I] --------- Verifying Ranger DB connection ---------","info")
	xa_sqlObj.check_connection(db_name, db_user, db_password)

	if len(argv)==1:

		log("[I] --------- Verifying Ranger DB tables ---------","info")
		if xa_sqlObj.check_table(db_name, db_user, db_password, x_user):
			pass
		else:
			log("[I] --------- Importing Ranger Core DB Schema ---------","info")
			xa_sqlObj.import_db_file(db_name, db_user, db_password, xa_db_core_file)


main(sys.argv)
