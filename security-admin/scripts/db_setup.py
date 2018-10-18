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
import shlex
import platform
import logging
import subprocess
from os.path import basename
import time
import socket
globalDict = {}

os_name = platform.system()
os_name = os_name.upper()
ranger_version=''
jisql_debug=True
retryPatchAfterSeconds=120
java_patch_regex="^Patch.*?J\d{5}.class$"
is_unix = os_name == "LINUX" or os_name == "DARWIN"
max_memory='1g'

RANGER_ADMIN_HOME = os.getenv("RANGER_ADMIN_HOME")
if RANGER_ADMIN_HOME is None:
	RANGER_ADMIN_HOME = os.getcwd()

if socket.getfqdn().find('.')>=0:
	client_host=socket.getfqdn()
else:
	client_host=socket.gethostbyaddr(socket.gethostname())[0]

RANGER_ADMIN_CONF = os.getenv("RANGER_ADMIN_CONF")
if RANGER_ADMIN_CONF is None:
	if is_unix:
		RANGER_ADMIN_CONF = RANGER_ADMIN_HOME
	elif os_name == "WINDOWS":
		RANGER_ADMIN_CONF = os.path.join(RANGER_ADMIN_HOME,'bin')

def check_output(query):
	if is_unix:
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
	if is_unix:
		read_config_file = open(os.path.join(RANGER_ADMIN_CONF,'install.properties'))
	elif os_name == "WINDOWS":
		read_config_file = open(os.path.join(RANGER_ADMIN_CONF,'install_config.properties'))
		#library_path = os.path.join(RANGER_ADMIN_HOME,"cred","lib","*")
	for each_line in read_config_file.read().split('\n') :
		each_line = each_line.strip();
		if len(each_line) == 0:
			continue
		elif each_line[0] == "#":
			continue
		if re.search('=', each_line):
			key , value = each_line.split("=",1)
			key = key.strip()
			if 'PASSWORD' in key:
				#jceks_file_path = os.path.join(RANGER_ADMIN_HOME, 'jceks','ranger_db.jceks')
				#statuscode,value = call_keystore(library_path,key,'',jceks_file_path,'get')
				#if statuscode == 1:
				value = ''
			value = value.strip()
			globalDict[key] = value

def jisql_log(query, db_password):
	if jisql_debug == True:
		if os_name == "WINDOWS":
			query = query.replace(' -p "'+db_password+'"' , ' -p "********"')
			log("[JISQL] "+query, "info")
		else:
			query = query.replace(" -p '"+db_password+"'" , " -p '********'")
			log("[JISQL] "+query, "info")

def password_validation(password):
	if password:
		if re.search("[\\\`'\"]",password):
			log("[E] password contains one of the unsupported special characters like \" ' \ `","error")
			sys.exit(1)

def subprocessCallWithRetry(query):
	retryCount=1
	returnCode = subprocess.call(query)
	while returnCode!=0:
		retryCount=retryCount+1
		time.sleep(1)
		log("[I] SQL statement execution Failed!! retrying attempt "+str(retryCount)+" of total 3" ,"info")
		returnCode = subprocess.call(query)
		if(returnCode!=0 and retryCount>=3):
			break
	return returnCode
def dbversionBasedOnUserName(userName):
        version = ""
        if userName == "admin" :
                version = 'DEFAULT_ADMIN_UPDATE'
        if userName == "rangerusersync" :
                version = 'DEFAULT_RANGER_USERSYNC_UPDATE'
        if userName == "rangertagsync" :
                version = 'DEFAULT_RANGER_TAGSYNC_UPDATE'
        if userName == "keyadmin" :
                version = 'DEFAULT_KEYADMIN_UPDATE'
        return version
class BaseDB(object):

	def check_connection(self, db_name, db_user, db_password):
		log("[I] ---------- Verifying DB connection ----------", "info")

	def check_table(self, db_name, db_user, db_password, TABLE_NAME):
		log("[I] ---------- Verifying table ----------", "info")

	def import_db_file(self, db_name, db_user, db_password, file_name):
		log("[I] Importing DB file :"+file_name, "info")

	def create_version_history_table(self, db_name, db_user, db_password, DBVERSION_CATALOG_CREATION,TABLE_NAME):
		log("[I] Creating version and patch history info table", "info")

	def apply_patches(self, db_name, db_user, db_password, PATCHES_PATH):
		#first get all patches and then apply each patch
		if not os.path.exists(PATCHES_PATH):
			log("[I] No patches to apply!","info")
		else:
			# files: coming from os.listdir() sorted alphabetically, thus not numerically
			files = os.listdir(PATCHES_PATH)
			if files:
				sorted_files = sorted(files, key=lambda x: str(x.split('.')[0]))
				for filename in sorted_files:
					currentPatch = os.path.join(PATCHES_PATH, filename)
					self.import_db_patches(db_name, db_user, db_password, currentPatch)
				self.update_applied_patches_status(db_name, db_user, db_password, "DB_PATCHES")
			else:
				log("[I] No patches to apply!","info")

	def auditdb_operation(self, xa_db_host, audit_db_host, db_name, audit_db_name, db_user, audit_db_user, db_password, audit_db_password, file_name, TABLE_NAME):
		log("[I] ----------------- Audit DB operations ------------", "info")

	def apply_auditdb_patches(self, xa_sqlObj,xa_db_host, audit_db_host, db_name, audit_db_name, db_user, audit_db_user, db_password, audit_db_password, PATCHES_PATH, TABLE_NAME):
		#first get all patches and then apply each patch
		if not os.path.exists(PATCHES_PATH):
			log("[I] No patches to apply!","info")
		else:
			# files: coming from os.listdir() sorted alphabetically, thus not numerically
			files = os.listdir(PATCHES_PATH)
			if files:
				sorted_files = sorted(files, key=lambda x: str(x.split('.')[0]))
				for filename in sorted_files:
					currentPatch = os.path.join(PATCHES_PATH, filename)
					self.import_auditdb_patches(xa_sqlObj, xa_db_host, audit_db_host, db_name, audit_db_name, db_user, audit_db_user, db_password, audit_db_password, currentPatch, TABLE_NAME)
			else:
				log("[I] No patches to apply!","info")

	def execute_java_patches(xa_db_host, db_user, db_password, db_name):
		log("[I] ----------------- Executing java patches ------------", "info")

	def create_synonym(db_name, db_user, db_password,audit_db_user):
		log("[I] ----------------- Creating Synonym ------------", "info")

	def change_admin_default_password(xa_db_host, db_user, db_password, db_name,userName,oldPassword,newPassword):
                log("[I] ----------------- Changing Ranger "+ userName +" default password  ------------", "info")

	def import_core_db_schema(self, db_name, db_user, db_password, file_name,first_table,last_table):
		log("[I] ---------- Importing Core DB Schema ----------", "info")

        def is_new_install(xa_db_host, db_user, db_password, db_name):
                log("[I] ----------------- Checking Ranger Version ------------", "info")

class MysqlConf(BaseDB):
	# Constructor
	def __init__(self, host,SQL_CONNECTOR_JAR,JAVA_BIN,db_ssl_enabled,db_ssl_required,db_ssl_verifyServerCertificate,javax_net_ssl_keyStore,javax_net_ssl_keyStorePassword,javax_net_ssl_trustStore,javax_net_ssl_trustStorePassword,db_ssl_auth_type):
		self.host = host
		self.SQL_CONNECTOR_JAR = SQL_CONNECTOR_JAR
		self.JAVA_BIN = JAVA_BIN
		self.db_ssl_enabled=db_ssl_enabled.lower()
		self.db_ssl_required=db_ssl_required.lower()
		self.db_ssl_verifyServerCertificate=db_ssl_verifyServerCertificate.lower()
		self.db_ssl_auth_type=db_ssl_auth_type.lower()
		self.javax_net_ssl_keyStore=javax_net_ssl_keyStore
		self.javax_net_ssl_keyStorePassword=javax_net_ssl_keyStorePassword
		self.javax_net_ssl_trustStore=javax_net_ssl_trustStore
		self.javax_net_ssl_trustStorePassword=javax_net_ssl_trustStorePassword

	def get_jisql_cmd(self, user, password ,db_name):
		path = RANGER_ADMIN_HOME
		db_ssl_param=''
		db_ssl_cert_param=''
		if self.db_ssl_enabled == 'true':
			db_ssl_param="?useSSL=%s&requireSSL=%s&verifyServerCertificate=%s" %(self.db_ssl_enabled,self.db_ssl_required,self.db_ssl_verifyServerCertificate)
			if self.db_ssl_verifyServerCertificate == 'true':
				if self.db_ssl_auth_type == '1-way':
					db_ssl_cert_param=" -Djavax.net.ssl.trustStore=%s -Djavax.net.ssl.trustStorePassword=%s " %(self.javax_net_ssl_trustStore,self.javax_net_ssl_trustStorePassword)
				else:
					db_ssl_cert_param=" -Djavax.net.ssl.keyStore=%s -Djavax.net.ssl.keyStorePassword=%s -Djavax.net.ssl.trustStore=%s -Djavax.net.ssl.trustStorePassword=%s " %(self.javax_net_ssl_keyStore,self.javax_net_ssl_keyStorePassword,self.javax_net_ssl_trustStore,self.javax_net_ssl_trustStorePassword)
		self.JAVA_BIN = self.JAVA_BIN.strip("'")
		if is_unix:
			jisql_cmd = "%s %s -cp %s:%s/jisql/lib/* org.apache.util.sql.Jisql -driver mysqlconj -cstring jdbc:mysql://%s/%s%s -u '%s' -p '%s' -noheader -trim -c \;" %(self.JAVA_BIN,db_ssl_cert_param,self.SQL_CONNECTOR_JAR,path,self.host,db_name,db_ssl_param,user,password)
		elif os_name == "WINDOWS":
			jisql_cmd = "%s %s -cp %s;%s\jisql\\lib\\* org.apache.util.sql.Jisql -driver mysqlconj -cstring jdbc:mysql://%s/%s%s -u \"%s\" -p \"%s\" -noheader -trim" %(self.JAVA_BIN,db_ssl_cert_param,self.SQL_CONNECTOR_JAR, path, self.host, db_name, db_ssl_param,user, password)
		return jisql_cmd

	def check_connection(self, db_name, db_user, db_password):
		log("[I] Checking connection..", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if is_unix:
			query = get_cmd + " -query \"SELECT version();\""
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"SELECT version();\" -c ;"
		jisql_log(query, db_password)
		output = check_output(query)
		if output.strip('Production  |'):
			log("[I] Checking connection passed.", "info")
			return True
		else:
			log("[E] Can't establish connection!! Exiting.." ,"error")
			log("[I] Please run DB setup first or contact Administrator.." ,"info")
			sys.exit(1)

	def grant_audit_db_user(self, db_user, audit_db_name, audit_db_user, audit_db_password, db_password,TABLE_NAME):
		hosts_arr =["%", "localhost"]
		hosts_arr.append(self.host)
		for host in hosts_arr:
			log("[I] ---------------Granting privileges TO '"+ audit_db_user + "' on '" + audit_db_name+"'-------------" , "info")
			get_cmd = self.get_jisql_cmd(db_user, db_password, audit_db_name)
			if is_unix:
				query = get_cmd + " -query \"GRANT INSERT ON %s.%s TO '%s'@'%s';\"" %(audit_db_name,TABLE_NAME,audit_db_user,host)
				jisql_log(query, db_password)
				ret = subprocessCallWithRetry(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"GRANT INSERT ON %s.%s TO '%s'@'%s';\" -c ;" %(audit_db_name,TABLE_NAME,audit_db_user,host)
				jisql_log(query, db_password)
				ret = subprocessCallWithRetry(query)
			if ret == 0:
				log("[I] Granting privileges to '" + audit_db_user+"' done on '"+ audit_db_name+"'", "info")
			else:
				log("[E] Granting privileges to '" +audit_db_user+"' failed on '" + audit_db_name+"'", "error")
				sys.exit(1)

	def import_db_file(self, db_name, db_user, db_password, file_name):
		isImported=False
		name = basename(file_name)
		if os.path.isfile(file_name):
			log("[I] Importing db schema to database " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if is_unix:
				query = get_cmd + " -input %s" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -input %s -c ;" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(query)
			if ret == 0:
				log("[I] "+name + " file imported successfully","info")
				isImported=True
			else:
				log("[E] "+name + " file import failed!","error")
		else:
			log("[E] DB schema file " + name+ " not found","error")
			sys.exit(1)
		return isImported

	def import_db_patches(self, db_name, db_user, db_password, file_name):
		name = basename(file_name)
		if os.path.isfile(file_name):
			version = name.split('-')[0]
			log("[I] Executing patch on  " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if is_unix:
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
			jisql_log(query, db_password)
			output = check_output(query)
			if output.strip(version + " |"):
				log("[I] Patch "+ name  +" is already applied" ,"info")
			else:
				if is_unix:
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					while(output.strip(version + " |")):
						log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
						time.sleep(retryPatchAfterSeconds)
						jisql_log(query, db_password)
						output = check_output(query)
				else:
					if is_unix:
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', now(), '%s', now(), '%s','N') ;\"" %(version,ranger_version,client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', now(), '%s', now(), '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret == 0:
						log ("[I] Patch "+ name +" is being applied..","info")
					else:
						log("[E] Patch "+ name +" failed", "error")
						sys.exit(1)
					if is_unix:
						query = get_cmd + " -input %s" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -input %s -c ;" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret!=0:
						if is_unix:
							query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
						jisql_log(query, db_password)
						output = check_output(query)
						if output.strip(version + " |"):
							ret=0
							log("[I] Patch "+ name  +" has been applied by some other process!" ,"info")
					if ret == 0:
						log("[I] "+name + " patch applied","info")
						if is_unix:
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] Patch version updated", "info")
						else:
							if is_unix:
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							log("[E] Updating patch version failed", "error")
							sys.exit(1)
					else:
						if is_unix:
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						log("[E] "+name + " import failed!","error")
						sys.exit(1)

	def import_auditdb_patches(self, xa_sqlObj,xa_db_host, audit_db_host, db_name, audit_db_name, db_user, audit_db_user, db_password, audit_db_password, file_name, TABLE_NAME):
		log("[I] --------- Checking XA_ACCESS_AUDIT table to apply audit db patches --------- ","info")
		output = self.check_table(audit_db_name, db_user, db_password, TABLE_NAME)
		if output == True:
			name = basename(file_name)
			if os.path.isfile(file_name):
				version = name.split('-')[0]
				log("[I] Executing patch on  " + audit_db_name + " from file: " + name,"info")
				get_cmd1 = xa_sqlObj.get_jisql_cmd(db_user, db_password, db_name)
				if is_unix:
					query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					log("[I] Patch "+ name  +" is already applied" ,"info")
				else:
					if is_unix:
						query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
						while(output.strip(version + " |")):
							log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
							time.sleep(retryPatchAfterSeconds)
							jisql_log(query, db_password)
							output = check_output(query)
					else:
						if is_unix:
							query = get_cmd1 + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', now(), '%s', now(), '%s','N') ;\"" %(version,ranger_version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd1 + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', now(), '%s', now(), '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log ("[I] Patch "+ name +" is being applied..","info")
						else:
							log("[E] Patch "+ name +" failed", "error")
							sys.exit(1)
						get_cmd2 = self.get_jisql_cmd(db_user, db_password, audit_db_name)
						if is_unix:
							query = get_cmd2 + " -input %s" %file_name
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd2 + " -input %s -c ;" %file_name
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] "+name + " patch applied","info")
							if is_unix:
								query = get_cmd1 + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd1 + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0:
								log("[I] Patch version updated", "info")
							else:
								if is_unix:
									query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								log("[E] Updating patch version failed", "error")
								sys.exit(1)
						else:
							if is_unix:
								query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							log("[E] "+name + " import failed!","error")
							sys.exit(1)
		else:
			log("[I] Table XA_ACCESS_AUDIT does not exists in " +audit_db_name,"error")
			sys.exit(1)

	def check_table(self, db_name, db_user, db_password, TABLE_NAME):
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if is_unix:
			query = get_cmd + " -query \"show tables like '%s';\"" %(TABLE_NAME)
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"show tables like '%s';\" -c ;" %(TABLE_NAME)
		jisql_log(query, db_password)
		output = check_output(query)
		if output.strip(TABLE_NAME + " |"):
			log("[I] Table " + TABLE_NAME +" already exists in database '" + db_name + "'","info")
			return True
		else:
			log("[I] Table " + TABLE_NAME +" does not exist in database " + db_name + "","info")
			return False

	def auditdb_operation(self, xa_db_host, audit_db_host, db_name, audit_db_name, db_user, audit_db_user, db_password, audit_db_password, file_name, TABLE_NAME):
		log("[I] --------- Check ranger user connection ---------","info")
		self.check_connection(audit_db_name, db_user, db_password)
		log("[I] --------- Check audit table exists --------- ","info")
		output = self.check_table(audit_db_name, db_user, db_password, TABLE_NAME)
		if output == False:
			self.import_db_file(audit_db_name ,db_user, db_password, file_name)
		self.grant_audit_db_user(db_user, audit_db_name, audit_db_user, audit_db_password, db_password,TABLE_NAME)

	def execute_java_patches(self, xa_db_host, db_user, db_password, db_name):
		my_dict = {}
		version = ""
		className = ""
		app_home = os.path.join(RANGER_ADMIN_HOME,"ews","webapp")
		ranger_log = os.path.join(RANGER_ADMIN_HOME,"ews","logs")
		javaFiles = os.path.join(app_home,"WEB-INF","classes","org","apache","ranger","patch")

		if not os.path.exists(javaFiles):
			log("[I] No java patches to apply!","info")
		else:
			files = os.listdir(javaFiles)
			if files:
				for filename in files:
					f = re.match(java_patch_regex,filename)
					if f:
						version = re.match("Patch.*?_(.*).class",filename)
						version = version.group(1)
						key3 = int(version.strip("J"))
						my_dict[key3] = filename

			keylist = my_dict.keys()
			keylist.sort()
			for key in keylist:
				#print "%s: %s" % (key, my_dict[key])
				version = str(key)
				className = my_dict[key]
				className = className.strip(".class")
				if version != "":
					get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
					if is_unix:
						query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\"" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
						log("[I] Java patch "+ className  +" is already applied" ,"info")
					else:
						if is_unix:
							query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\"" %(version)
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\" -c ;" %(version)
						jisql_log(query, db_password)
						output = check_output(query)
						if output.strip(version + " |"):
							while(output.strip(version + " |")):
								log("[I] Java patch "+ className  +" is being applied by some other process" ,"info")
								time.sleep(retryPatchAfterSeconds)
								jisql_log(query, db_password)
								output = check_output(query)
						else:
							if is_unix:
								query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('J%s', now(), '%s', now(), '%s','N') ;\"" %(version,ranger_version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('J%s', now(), '%s', now(), '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0:
								log ("[I] java patch "+ className +" is being applied..","info")
							else:
								log("[E] java patch "+ className +" failed", "error")
								sys.exit(1)
							if is_unix:
								path = os.path.join("%s","WEB-INF","classes","conf:%s","WEB-INF","classes","lib","*:%s","WEB-INF",":%s","META-INF",":%s","WEB-INF","lib","*:%s","WEB-INF","classes",":%s","WEB-INF","classes","META-INF:%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							elif os_name == "WINDOWS":
								path = os.path.join("%s","WEB-INF","classes","conf;%s","WEB-INF","classes","lib","*;%s","WEB-INF",";%s","META-INF",";%s","WEB-INF","lib","*;%s","WEB-INF","classes",";%s","WEB-INF","classes","META-INF;%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							get_java_cmd = "%s -XX:MetaspaceSize=100m -XX:MaxMetaspaceSize=200m -Xmx%s -Xms1g -Dlogdir=%s -Dlog4j.configuration=db_patch.log4j.xml -cp %s org.apache.ranger.patch.%s"%(self.JAVA_BIN,max_memory,ranger_log,path,className)
							if is_unix:
								ret = subprocess.call(shlex.split(get_java_cmd))
							elif os_name == "WINDOWS":
								ret = subprocess.call(get_java_cmd)
							if ret == 0:
								if is_unix:
									query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='J%s' and active='N' and updated_by='%s';\"" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='J%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								if ret == 0:
									log ("[I] java patch "+ className +" is applied..","info")
								else:
									if is_unix:
										query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\"" %(version,client_host)
										jisql_log(query, db_password)
										ret = subprocess.call(shlex.split(query))
									elif os_name == "WINDOWS":
										query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
										jisql_log(query, db_password)
										ret = subprocess.call(query)
									log("[E] java patch "+ className +" failed", "error")
									sys.exit(1)
							else:
								if is_unix:
									query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\"" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								log("[E] applying java patch "+ className +" failed", "error")
								sys.exit(1)

	def change_admin_default_password(self, xa_db_host, db_user, db_password, db_name,userName,oldPassword,newPassword):
		version = ""
		className = "ChangePasswordUtil"
                version = dbversionBasedOnUserName(userName)
		app_home = os.path.join(RANGER_ADMIN_HOME,"ews","webapp")
		ranger_log = os.path.join(RANGER_ADMIN_HOME,"ews","logs")
		filePath = os.path.join(app_home,"WEB-INF","classes","org","apache","ranger","patch","cliutil","ChangePasswordUtil.class")
		if os.path.exists(filePath):
			if version != "":
				get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
				if is_unix:
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
                                        log("[I] Ranger "+ userName +" default password has already been changed!!","info")
				else:
					if is_unix:
						query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
                                                countTries = 0
						while(output.strip(version + " |")):
                                                        if countTries < 3:
                                                                log("[I] Ranger Password change utility is being executed by some other process" ,"info")
                                                                time.sleep(retryPatchAfterSeconds)
                                                                jisql_log(query, db_password)
                                                                output = check_output(query)
                                                                countTries += 1
                                                        else:
                                                                log("[E] Tried updating the password "+ str(countTries) + " times","error")
                                                                log("[E] If Ranger "+  userName +" user password is not being changed by some other process then manually delete the entry from ranger database table x_db_version_h table where version is " + version ,"error")
                                                                sys.exit(1)
					else:
						if is_unix:
							query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', now(), '%s', now(), '%s','N') ;\"" %(version,ranger_version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', now(), '%s', now(), '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
                                                        log ("[I] Ranger "+ userName +" default password change request is in process..","info")
						else:
                                                        log("[E] Ranger "+ userName +" default password change request failed", "error")
                                                        sys.exit(1)
						if is_unix:
							path = os.path.join("%s","WEB-INF","classes","conf:%s","WEB-INF","classes","lib","*:%s","WEB-INF",":%s","META-INF",":%s","WEB-INF","lib","*:%s","WEB-INF","classes",":%s","WEB-INF","classes","META-INF:%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
						elif os_name == "WINDOWS":
							path = os.path.join("%s","WEB-INF","classes","conf;%s","WEB-INF","classes","lib","*;%s","WEB-INF",";%s","META-INF",";%s","WEB-INF","lib","*;%s","WEB-INF","classes",";%s","WEB-INF","classes","META-INF;%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
                                                get_java_cmd = "%s -Dlogdir=%s -Dlog4j.configuration=db_patch.log4j.xml -cp %s org.apache.ranger.patch.cliutil.%s %s %s %s -default"%(self.JAVA_BIN,ranger_log,path,className,'"'+userName+'"','"'+oldPassword+'"','"'+newPassword+'"')
						if is_unix:
							status = subprocess.call(shlex.split(get_java_cmd))
						elif os_name == "WINDOWS":
							status = subprocess.call(get_java_cmd)
						if status == 0 or status==2:
							if is_unix:
								query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0 and status == 0:
                                                                log ("[I] Ranger "+ userName +" default password change request processed successfully..","info")
							elif ret == 0 and status == 2:
                                                                log ("[I] Ranger "+ userName +" default password change request process skipped!","info")
							else:
								if is_unix:
									query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
                                                                log("[E] Ranger "+ userName +" default password change request failed", "error")
								sys.exit(1)
						else:
							if is_unix:
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
                                                        log("[E] Ranger "+ userName +" default password change request failed", "error")
							sys.exit(1)

	def create_version_history_table(self, db_name, db_user, db_password, file_name,table_name):
		name = basename(file_name)
		if os.path.isfile(file_name):
			isTableExist=self.check_table(db_name, db_user, db_password, table_name)
			if isTableExist==False:
				log("[I] Importing "+table_name+" table schema to database " + db_name + " from file: " + name,"info")
			while(isTableExist==False):
				get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
				if is_unix:
					query = get_cmd + " -input %s" %file_name
					jisql_log(query, db_password)
					ret = subprocess.call(shlex.split(query))
				elif os_name == "WINDOWS":
					query = get_cmd + " -input %s -c ;" %file_name
					jisql_log(query, db_password)
					ret = subprocess.call(query)
				if ret == 0:
					log("[I] "+name + " file imported successfully","info")
				else:
					log("[E] "+name + " file import failed!","error")
					time.sleep(30)
				isTableExist=self.check_table(db_name, db_user, db_password, table_name)
		else:
			log("[E] Table schema file " + name+ " not found","error")
			sys.exit(1)

	def import_core_db_schema(self, db_name, db_user, db_password, file_name,first_table,last_table):
		version = 'CORE_DB_SCHEMA'
		if os.path.isfile(file_name):
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if is_unix:
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
			jisql_log(query, db_password)
			output = check_output(query)
			if output.strip(version + " |"):
				log("[I] "+version+" is already imported" ,"info")
			else:
				if is_unix:
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					while(output.strip(version + " |")):
						log("[I] "+ version  +" is being imported by some other process" ,"info")
						time.sleep(retryPatchAfterSeconds)
						jisql_log(query, db_password)
						output = check_output(query)
				else:
					if is_unix:
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', now(), '%s', now(), '%s','N') ;\"" %(version,ranger_version,client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', now(), '%s', now(), '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret != 0:
						log("[E] "+ version +" import failed", "error")
						sys.exit(1)
					isFirstTableExist = self.check_table(db_name, db_user, db_password, first_table)
					isLastTableExist = self.check_table(db_name, db_user, db_password, last_table)
					isSchemaCreated=False
					if isFirstTableExist == True and isLastTableExist == True :
						isSchemaCreated=True
					elif isFirstTableExist == False and isLastTableExist == False :
						isImported=self.import_db_file(db_name, db_user, db_password, file_name)
						if(isImported==False):
							log("[I] "+ version  +" might being imported by some other process" ,"info")
							time.sleep(retryPatchAfterSeconds)
						isLastTableExist=self.check_table(db_name, db_user, db_password, last_table)
						if(isLastTableExist==True):
							isSchemaCreated=True
					elif isFirstTableExist == False or isLastTableExist == False :
						while(isFirstTableExist == False or isLastTableExist==False):
							log("[I] "+ version  +" is being imported by some other process" ,"info")
							time.sleep(retryPatchAfterSeconds)
							isFirstTableExist=self.check_table(db_name, db_user, db_password, first_table)
							isLastTableExist=self.check_table(db_name, db_user, db_password, last_table)
							if(isFirstTableExist==True and isLastTableExist==True):
								isSchemaCreated=True
					if isSchemaCreated == True:
						if is_unix:
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] "+version +" import status has been updated", "info")
						else:
							if is_unix:
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							log("[E] Updating "+version +" import status failed", "error")
							sys.exit(1)
					else:
						if is_unix:
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						log("[E] "+version + " import failed!","error")
						sys.exit(1)

	def hasPendingPatches(self, db_name, db_user, db_password, version):
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if is_unix:
			query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and inst_by = '%s' and active='Y';\"" %(version,ranger_version)
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and inst_by = '%s' and active='Y';\" -c ;" %(version,ranger_version)
		jisql_log(query, db_password)
		output = check_output(query)
		if output.strip(version + " |"):
			return False
		else:
			return True

	def update_applied_patches_status(self,db_name, db_user, db_password,version):
		if self.hasPendingPatches(db_name, db_user, db_password,version) == True:
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if is_unix:
				query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', now(), '%s', now(), '%s','Y') ;\"" %(version,ranger_version,client_host)
				jisql_log(query, db_password)
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', now(), '%s', now(), '%s','Y') ;\" -c ;" %(version,ranger_version,client_host)
				jisql_log(query, db_password)
				ret = subprocess.call(query)
			if ret != 0:
				log("[E] "+ version +" status entry to x_db_version_h table failed", "error")
				sys.exit(1)
			else:
				log("[I] "+ version +" status entry to x_db_version_h table completed", "info")

        def is_new_install(self, xa_db_host, db_user, db_password, db_name):
                get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
                if is_unix:
                        query = get_cmd + " -query \"SELECT version();\""
                elif os_name == "WINDOWS":
                        query = get_cmd + " -query \"SELECT version();\" -c ;"
                output = check_output(query)
                if not output.strip('Production  |'):
                        sys.exit(0)
                get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
                version="J10001"
                if is_unix:
                        query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
                elif os_name == "WINDOWS":
                        query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
                output = check_output(query)
                if not output.strip(version + " |"):
                        sys.exit(0)

class OracleConf(BaseDB):
	# Constructor
	def __init__(self, host, SQL_CONNECTOR_JAR, JAVA_BIN):
		self.host = host 
		self.SQL_CONNECTOR_JAR = SQL_CONNECTOR_JAR
		self.JAVA_BIN = JAVA_BIN

	def get_jisql_cmd(self, user, password):
		path = RANGER_ADMIN_HOME
		self.JAVA_BIN = self.JAVA_BIN.strip("'")
		if not re.search('-Djava.security.egd=file:///dev/urandom', self.JAVA_BIN):
			self.JAVA_BIN = self.JAVA_BIN + " -Djava.security.egd=file:///dev/urandom "

		#if self.host.count(":") == 2:
		if self.host.count(":") == 2 or self.host.count(":") == 0:
			#jdbc:oracle:thin:@[HOST][:PORT]:SID or #jdbc:oracle:thin:@GL
			cstring="jdbc:oracle:thin:@%s" %(self.host)
		else:
			#jdbc:oracle:thin:@//[HOST][:PORT]/SERVICE
			cstring="jdbc:oracle:thin:@//%s" %(self.host)

		if is_unix:
			jisql_cmd = "%s -cp %s:%s/jisql/lib/* org.apache.util.sql.Jisql -driver oraclethin -cstring %s -u '%s' -p '%s' -noheader -trim" %(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, path, cstring, user, password)
		elif os_name == "WINDOWS":
			jisql_cmd = "%s -cp %s;%s\jisql\\lib\\* org.apache.util.sql.Jisql -driver oraclethin -cstring %s -u \"%s\" -p \"%s\" -noheader -trim" %(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, path, cstring, user, password)
		return jisql_cmd


	def check_connection(self, db_name, db_user, db_password):
		log("[I] Checking connection", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password)
		if is_unix:
			query = get_cmd + " -c \; -query \"select * from v$version;\""
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"select * from v$version;\" -c ;"
		jisql_log(query, db_password)
		output = check_output(query)
		if output.strip('Production  |'):
			log("[I] Connection success", "info")
			return True
		else:
			log("[E] Can't establish connection!", "error")
			sys.exit(1)

	def grant_audit_db_user(self, audit_db_name ,db_user,audit_db_user,db_password,audit_db_password):
		get_cmd = self.get_jisql_cmd(db_user, db_password)
		if is_unix:
			query = get_cmd + " -c \; -query 'GRANT SELECT ON %s.XA_ACCESS_AUDIT_SEQ TO %s;'" % (db_user,audit_db_user)
			jisql_log(query, db_password)
			ret = subprocessCallWithRetry(shlex.split(query))
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"GRANT SELECT ON %s.XA_ACCESS_AUDIT_SEQ TO %s;\" -c ;" % (db_user,audit_db_user)
			jisql_log(query, db_password)
			ret = subprocessCallWithRetry(query)
		if ret != 0:
			sys.exit(1)
		if is_unix:
			query = get_cmd + " -c \; -query 'GRANT INSERT ON %s.XA_ACCESS_AUDIT TO %s;'" % (db_user,audit_db_user)
			jisql_log(query, db_password)
			ret = subprocessCallWithRetry(shlex.split(query))
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"GRANT INSERT ON %s.XA_ACCESS_AUDIT TO %s;\" -c ;" % (db_user,audit_db_user)
			jisql_log(query, db_password)
			ret = subprocessCallWithRetry(query)
		if ret != 0:
			sys.exit(1)

	def import_db_file(self, db_name, db_user, db_password, file_name):
		isImported=False
		name = basename(file_name)
		if os.path.isfile(file_name):
			log("[I] Importing script " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password)
			if is_unix:
				query = get_cmd + " -input %s -c \;" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -input %s -c ;" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(query)
			if ret == 0:
				log("[I] "+name + " imported successfully","info")
				isImported=True
			else:
				log("[E] "+name + " import failed!","error")
		else:
			log("[E] DB schema file " + name+ " not found","error")
			sys.exit(1)
		return isImported

	def create_synonym(self,db_name, db_user, db_password,audit_db_user):
		log("[I] ----------------- Creating Synonym ------------", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password)
		if is_unix:
			query = get_cmd + " -c \; -query 'CREATE OR REPLACE SYNONYM %s.XA_ACCESS_AUDIT FOR %s.XA_ACCESS_AUDIT;'" % (audit_db_user,db_user)
			jisql_log(query, db_password)
			ret = subprocessCallWithRetry(shlex.split(query))
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"CREATE OR REPLACE SYNONYM %s.XA_ACCESS_AUDIT FOR %s.XA_ACCESS_AUDIT;\" -c ;" % (audit_db_user,db_user)
			jisql_log(query, db_password)
			ret = subprocessCallWithRetry(query)
		if ret != 0:
			sys.exit(1)
		if is_unix:
			query = get_cmd + " -c \; -query 'CREATE OR REPLACE SYNONYM %s.XA_ACCESS_AUDIT_SEQ FOR %s.XA_ACCESS_AUDIT_SEQ;'" % (audit_db_user,db_user)
			jisql_log(query, db_password)
			ret = subprocessCallWithRetry(shlex.split(query))
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"CREATE OR REPLACE SYNONYM %s.XA_ACCESS_AUDIT_SEQ FOR %s.XA_ACCESS_AUDIT_SEQ;\" -c ;" % (audit_db_user,db_user)
			jisql_log(query, db_password)
			ret = subprocessCallWithRetry(query)
		if ret != 0:
			sys.exit(1)

	def import_db_patches(self, db_name, db_user, db_password, file_name):
		if os.path.isfile(file_name):
			name = basename(file_name)
			version = name.split('-')[0]
			log("[I] Executing patch on " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password)
			if is_unix:
				query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
			jisql_log(query, db_password)
			output = check_output(query)
			if output.strip(version +" |"):
				log("[I] Patch "+ name  +" is already applied" ,"info")
			else:
				if is_unix:
					query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version +" |"):
					while(output.strip(version + " |")):
						log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
						time.sleep(retryPatchAfterSeconds)
						jisql_log(query, db_password)
						output = check_output(query)
				else:
					if is_unix:
						query = get_cmd + " -c \; -query \"insert into x_db_version_h (id,version, inst_at, inst_by, updated_at, updated_by,active) values ( X_DB_VERSION_H_SEQ.nextval,'%s', sysdate, '%s', sysdate, '%s','N');\"" %(version, ranger_version, client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"insert into x_db_version_h (id,version, inst_at, inst_by, updated_at, updated_by,active) values ( X_DB_VERSION_H_SEQ.nextval,'%s', sysdate, '%s', sysdate, '%s','N');\" -c ;" %(version, ranger_version, client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret == 0:
						log ("[I] Patch "+ name +" is being applied..","info")
					else:
						log("[E] Patch "+ name +" failed", "error")
					if is_unix:
						query = get_cmd + " -input %s -c /" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -input %s -c /" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret != 0:
						if is_unix:
							query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
						jisql_log(query, db_password)
						output = check_output(query)
						if output.strip(version +" |"):
							ret=0
							log("[I] Patch "+ name  +" has been applied by some other process!" ,"info")
					if ret == 0:
						log("[I] "+name + " patch applied","info")
						if is_unix:
							query = get_cmd + " -c \; -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] Patch version updated", "info")
						else:
							if is_unix:
								query = get_cmd + " -c \; -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							log("[E] Updating patch version failed", "error")
							sys.exit(1)
					else:
						if is_unix:
							query = get_cmd + " -c \; -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						log("[E] "+name + " Import failed!","error")
						sys.exit(1)

	def import_auditdb_patches(self, xa_sqlObj,xa_db_host, audit_db_host, db_name, audit_db_name, db_user, audit_db_user, db_password, audit_db_password, file_name, TABLE_NAME):
		log("[I] --------- Checking XA_ACCESS_AUDIT table to apply audit db patches --------- ","info")
		output = self.check_table(db_name, db_user, db_password, TABLE_NAME)
		if output == True:
			if os.path.isfile(file_name):
				name = basename(file_name)
				version = name.split('-')[0]
				log("[I] Executing patch on " + audit_db_name + " from file: " + name,"info")
				get_cmd1 = xa_sqlObj.get_jisql_cmd(db_user, db_password)
				if is_unix:
					query = get_cmd1 + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version +" |"):
					log("[I] Patch "+ name  +" is already applied" ,"info")
				else:
					if is_unix:
						query = get_cmd1 + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version +" |"):
						while(output.strip(version + " |")):
							log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
							time.sleep(retryPatchAfterSeconds)
							jisql_log(query, db_password)
							output = check_output(query)
					else:
						if is_unix:
							query = get_cmd1 + " -c \; -query \"insert into x_db_version_h (id,version, inst_at, inst_by, updated_at, updated_by,active) values ( X_DB_VERSION_H_SEQ.nextval,'%s', sysdate, '%s', sysdate, '%s','N');\"" %(version, ranger_version, client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd1 + " -query \"insert into x_db_version_h (id,version, inst_at, inst_by, updated_at, updated_by,active) values ( X_DB_VERSION_H_SEQ.nextval,'%s', sysdate, '%s', sysdate, '%s','N');\" -c ;" %(version, ranger_version, client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log ("[I] Patch "+ name +" is being applied..","info")
						else:
							log("[E] Patch "+ name +" failed", "error")
						get_cmd2 = self.get_jisql_cmd(db_user, db_password)
						if is_unix:
							query = get_cmd2 + " -input %s -c /" %file_name
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd2 + " -input %s -c /" %file_name
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] "+name + " patch applied","info")
							if is_unix:
								query = get_cmd1 + " -c \; -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\"" %(version, client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd1 + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version, client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0:
								log("[I] Patch version updated", "info")
							else:
								if is_unix:
									query = get_cmd1 + " -c \; -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version, client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version, client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								log("[E] Updating patch version failed", "error")
								sys.exit(1)
						else:
							if is_unix:
								query = get_cmd1 + " -c \; -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version, client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version, client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							log("[E] "+name + " Import failed!","error")
							sys.exit(1)
			else:
				log("[I] Patch file not found","error")
				sys.exit(1)

	def check_table(self, db_name, db_user, db_password, TABLE_NAME):
		get_cmd = self.get_jisql_cmd(db_user ,db_password)
		if is_unix:
			query = get_cmd + " -c \; -query 'select default_tablespace from user_users;'"
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"select default_tablespace from user_users;\" -c ;"
		jisql_log(query, db_password)
		output = check_output(query).strip()
		output = output.strip(' |')
		db_name = db_name.upper()
		if output == db_name:
			log("[I] User name " + db_user + " and tablespace " + db_name + " already exists.","info")
			log("[I] Verifying table " + TABLE_NAME +" in tablespace " + db_name, "info")
			get_cmd = self.get_jisql_cmd(db_user, db_password)
			if is_unix:
				query = get_cmd + " -c \; -query \"select UPPER(table_name) from all_tables where UPPER(tablespace_name)=UPPER('%s') and UPPER(table_name)=UPPER('%s');\"" %(db_name ,TABLE_NAME)
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"select UPPER(table_name) from all_tables where UPPER(tablespace_name)=UPPER('%s') and UPPER(table_name)=UPPER('%s');\" -c ;" %(db_name ,TABLE_NAME)
			jisql_log(query, db_password)
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

	def auditdb_operation(self, xa_db_host , audit_db_host , db_name ,audit_db_name, db_user, audit_db_user, db_password, audit_db_password, file_name, TABLE_NAME):
		log("[I] --------- Check admin user connection ---------","info")
		self.check_connection(db_name, db_user, db_password)
		log("[I] --------- Check audit user connection ---------","info")
		self.check_connection(audit_db_name, audit_db_user, audit_db_password)
		log("[I] --------- Check table ---------","info")
		if self.check_table(db_name, db_user, db_password, TABLE_NAME):
			pass
		else:
			self.import_db_file(audit_db_name, db_user, db_password ,file_name)
		log("[I] ---------------Granting privileges TO '"+ audit_db_user + "' on audit table-------------" , "info")
		self.grant_audit_db_user( audit_db_name ,db_user, audit_db_user, db_password,audit_db_password)

	def execute_java_patches(self, xa_db_host, db_user, db_password, db_name):
		my_dict = {}
		version = ""
		className = ""
		app_home = os.path.join(RANGER_ADMIN_HOME,"ews","webapp")
		ranger_log = os.path.join(RANGER_ADMIN_HOME,"ews","logs")
		javaFiles = os.path.join(app_home,"WEB-INF","classes","org","apache","ranger","patch")

		if not os.path.exists(javaFiles):
			log("[I] No java patches to apply!","info")
		else:
			files = os.listdir(javaFiles)
			if files:
				for filename in files:
					f = re.match(java_patch_regex,filename)
					if f:
						className = re.match("(Patch.*?)_.*.class",filename)
						className = className.group(1)
						version = re.match("Patch.*?_(.*).class",filename)
						version = version.group(1)
						key3 = int(version.strip("J"))
						my_dict[key3] = filename

			keylist = my_dict.keys()
			keylist.sort()
			for key in keylist:
				#print "%s: %s" % (key, my_dict[key])
				version = str(key)
				className = my_dict[key]
				className = className.strip(".class")
				if version != "":
					get_cmd = self.get_jisql_cmd(db_user, db_password)
					if is_unix:
						query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\"" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
						log("[I] java patch "+ className  +" is already applied" ,"info")
					else:
						if is_unix:
							query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\"" %(version)
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\" -c ;" %(version)
						jisql_log(query, db_password)
						output = check_output(query)
						if output.strip(version + " |"):
							#Fix to handle Ranger Upgrade failure for Oracle DB flavor 
							if is_unix:
								queryUpgradeCaseCheck = get_cmd + " -c \; -query \"select version from x_db_version_h where version = 'J%s' and active = 'N' and inst_by!='%s';\"" %(version,ranger_version)
							elif os_name == "WINDOWS":
								queryUpgradeCaseCheck = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N' and inst_by!='%s';\" -c ;" %(version,ranger_version)
							jisql_log(queryUpgradeCaseCheck, db_password)
							outputUpgradeCaseCheck = check_output(queryUpgradeCaseCheck)
							if outputUpgradeCaseCheck.strip(version + " |"):
								if is_unix:
									queryUpdate = get_cmd + " -c \; -query \"update x_db_version_h set active='Y' where version='J%s' and active='N' and inst_by!='%s';\"" %(version, ranger_version)
									jisql_log(queryUpdate, db_password)
									retUpdate = subprocess.call(shlex.split(queryUpdate))
								elif os_name == "WINDOWS":
									queryUpdate = get_cmd + " -query \"update x_db_version_h set active='Y' where version='J%s' and active='N' and inst_by!='%s';\" -c ;" %(version, ranger_version)
									jisql_log(queryUpdate, db_password)
									retUpdate = subprocess.call(queryUpdate)
								if retUpdate == 0:
									log ("[I] java patch "+ className +" status has been updated..","info")
							if is_unix:
								query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\"" %(version)
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\" -c ;" %(version)
							jisql_log(query, db_password)
							output = check_output(query)
							#End of Upgrade failure fix
							while(output.strip(version + " |")):
								log("[I] Java patch "+ className  +" is being applied by some other process" ,"info")
								time.sleep(retryPatchAfterSeconds)
								jisql_log(query, db_password)
								output = check_output(query)
						else:
							if is_unix:
								query = get_cmd + " -c \; -query \"insert into x_db_version_h (id,version, inst_at, inst_by, updated_at, updated_by,active) values ( X_DB_VERSION_H_SEQ.nextval,'J%s', sysdate, '%s', sysdate, '%s','N');\"" %(version, ranger_version, client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"insert into x_db_version_h (id,version, inst_at, inst_by, updated_at, updated_by,active) values ( X_DB_VERSION_H_SEQ.nextval,'J%s', sysdate, '%s', sysdate, '%s','N');\" -c ;" %(version, ranger_version, client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0:
								log ("[I] java patch "+ className +" is being applied..","info")
							else:
								log("[E] java patch "+ className +" failed", "error")
								sys.exit(1)
							if is_unix:
								path = os.path.join("%s","WEB-INF","classes","conf:%s","WEB-INF","classes","lib","*:%s","WEB-INF",":%s","META-INF",":%s","WEB-INF","lib","*:%s","WEB-INF","classes",":%s","WEB-INF","classes","META-INF:%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							elif os_name == "WINDOWS":
								path = os.path.join("%s","WEB-INF","classes","conf;%s","WEB-INF","classes","lib","*;%s","WEB-INF",";%s","META-INF",";%s","WEB-INF","lib","*;%s","WEB-INF","classes",";%s","WEB-INF","classes","META-INF;%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							get_java_cmd = "%s -XX:MetaspaceSize=100m -XX:MaxMetaspaceSize=200m -Xmx%s -Xms1g -Djava.security.egd=file:///dev/urandom -Dlogdir=%s -Dlog4j.configuration=db_patch.log4j.xml -cp %s org.apache.ranger.patch.%s"%(self.JAVA_BIN,max_memory,ranger_log,path,className)
							if is_unix:
								ret = subprocess.call(shlex.split(get_java_cmd))
							elif os_name == "WINDOWS":
								ret = subprocess.call(get_java_cmd)
							if ret == 0:
								if is_unix:
									query = get_cmd + " -c \; -query \"update x_db_version_h set active='Y' where version='J%s' and active='N' and updated_by='%s';\"" %(version, client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='J%s' and active='N' and updated_by='%s';\" -c ;" %(version, client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								if ret == 0:
									log ("[I] java patch "+ className +" is applied..","info")
								else:
									if is_unix:
										query = get_cmd + " -c \; -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\"" %(version, client_host)
										jisql_log(query, db_password)
										ret = subprocess.call(shlex.split(query))
									elif os_name == "WINDOWS":
										query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\" -c ;" %(version, client_host)
										jisql_log(query, db_password)
										ret = subprocess.call(query)
									log("[E] java patch "+ className +" failed", "error")
									sys.exit(1)
							else:
								if is_unix:
									query = get_cmd + " -c \; -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\"" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								log("[E] applying java patch "+ className +" failed", "error")
								sys.exit(1)

	def change_admin_default_password(self, xa_db_host, db_user, db_password, db_name,userName,oldPassword,newPassword):
		version = ""
		className = "ChangePasswordUtil"
                version = dbversionBasedOnUserName(userName)
		app_home = os.path.join(RANGER_ADMIN_HOME,"ews","webapp")
		ranger_log = os.path.join(RANGER_ADMIN_HOME,"ews","logs")
		filePath = os.path.join(app_home,"WEB-INF","classes","org","apache","ranger","patch","cliutil","ChangePasswordUtil.class")
		if os.path.exists(filePath):
			if version != "":
				get_cmd = self.get_jisql_cmd(db_user, db_password)
				if is_unix:
					query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
				elif os_name == "WINDOWS":
                                        query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
                                        log("[I] Ranger "+ userName +" default password has already been changed!!","info")
				else:
					if is_unix:
						query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
                                                countTries = 0
						while(output.strip(version + " |")):
                                                        if countTries < 3:
                                                                log("[I] Ranger Password change utility is being executed by some other process" ,"info")
                                                                time.sleep(retryPatchAfterSeconds)
                                                                jisql_log(query, db_password)
                                                                output = check_output(query)
                                                                countTries += 1
                                                        else:
                                                                log("[E] Tried updating the password "+ str(countTries) + " times","error")
                                                                log("[E] If Ranger "+  userName +" user password is not being changed by some other process then manually delete the entry from ranger database table x_db_version_h table where version is " + version ,"error")
                                                                sys.exit(1)
					else:
						if is_unix:
							query = get_cmd + " -c \; -query \"insert into x_db_version_h (id,version, inst_at, inst_by, updated_at, updated_by,active) values ( X_DB_VERSION_H_SEQ.nextval,'%s', sysdate, '%s', sysdate, '%s','N');\"" %(version, ranger_version, client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"insert into x_db_version_h (id,version, inst_at, inst_by, updated_at, updated_by,active) values ( X_DB_VERSION_H_SEQ.nextval,'%s', sysdate, '%s', sysdate, '%s','N');\" -c ;" %(version, ranger_version, client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
                                                        log ("[I] Ranger "+ userName +" default password change request is in process..","info")
						else:
                                                        log("[E] Ranger "+ userName +" default password change request failed", "error")
							sys.exit(1)
						if is_unix:
							path = os.path.join("%s","WEB-INF","classes","conf:%s","WEB-INF","classes","lib","*:%s","WEB-INF",":%s","META-INF",":%s","WEB-INF","lib","*:%s","WEB-INF","classes",":%s","WEB-INF","classes","META-INF:%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
						elif os_name == "WINDOWS":
							path = os.path.join("%s","WEB-INF","classes","conf;%s","WEB-INF","classes","lib","*;%s","WEB-INF",";%s","META-INF",";%s","WEB-INF","lib","*;%s","WEB-INF","classes",";%s","WEB-INF","classes","META-INF;%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
						get_java_cmd = "%s -XX:MetaspaceSize=100m -XX:MaxMetaspaceSize=200m -Xmx%s -Xms1g -Dlogdir=%s -Dlog4j.configuration=db_patch.log4j.xml -cp %s org.apache.ranger.patch.cliutil.%s %s %s %s -default"%(self.JAVA_BIN,max_memory,ranger_log,path,className,userName,oldPassword,newPassword)
						if is_unix:
							status = subprocess.call(shlex.split(get_java_cmd))
						elif os_name == "WINDOWS":
							status = subprocess.call(get_java_cmd)
						if status == 0 or status==2:
							if is_unix:
								query = get_cmd + " -c \; -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0 and status == 0:
                                                                log ("[I] Ranger "+ userName +" default password change request processed successfully..","info")
							elif ret == 0 and status == 2:
                                                                log ("[I] Ranger "+ userName +" default password change request process skipped!","info")
							else:
								if is_unix:
									query = get_cmd + " -c \; -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
                                                                log("[E] Ranger	"+ userName +" default password change request failed", "error")
								sys.exit(1)
						else:
							if is_unix:
								query = get_cmd + " -c \; -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
                                                        log("[E] Ranger "+ userName +" default password change request failed", "error")
							sys.exit(1)

	def create_version_history_table(self, db_name, db_user, db_password, file_name,table_name):
		name = basename(file_name)
		if os.path.isfile(file_name):
			isTableExist=self.check_table(db_name, db_user, db_password, table_name)
			if isTableExist==False:
				log("[I] Importing "+table_name+" table schema from file: " + name,"info")
			while(isTableExist==False):
				get_cmd = self.get_jisql_cmd(db_user, db_password)
				if is_unix:
					query = get_cmd + " -input %s -c \;" %file_name
					jisql_log(query, db_password)
					ret = subprocess.call(shlex.split(query))
				elif os_name == "WINDOWS":
					query = get_cmd + " -input %s -c ;" %file_name
					jisql_log(query, db_password)
					ret = subprocess.call(query)
				if ret == 0:
					log("[I] "+name + " file imported successfully","info")
				else:
					log("[E] "+name + " file import failed!","error")
					time.sleep(30)
				isTableExist=self.check_table(db_name, db_user, db_password, table_name)
		else:
			log("[E] Table schema file " + name+ " not found","error")
			sys.exit(1)

	def import_core_db_schema(self, db_name, db_user, db_password, file_name,first_table,last_table):
		version = 'CORE_DB_SCHEMA'
		if os.path.isfile(file_name):
			get_cmd = self.get_jisql_cmd(db_user, db_password)
			if is_unix:
				query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
			jisql_log(query, db_password)
			output = check_output(query)
			if output.strip(version + " |"):
				log("[I] "+version+" is already imported" ,"info")
			else:
				if is_unix:
					query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					while(output.strip(version + " |")):
						log("[I] "+ version  +" is being imported by some other process" ,"info")
						time.sleep(retryPatchAfterSeconds)
						jisql_log(query, db_password)
						output = check_output(query)
				else:
					if is_unix:
						query = get_cmd + " -c \; -query \"insert into x_db_version_h (id,version, inst_at, inst_by, updated_at, updated_by,active) values ( X_DB_VERSION_H_SEQ.nextval,'%s', sysdate, '%s', sysdate, '%s','N');\"" %(version, ranger_version, client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"insert into x_db_version_h (id,version, inst_at, inst_by, updated_at, updated_by,active) values ( X_DB_VERSION_H_SEQ.nextval,'%s', sysdate, '%s', sysdate, '%s','N');\" -c ;" %(version, ranger_version, client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret != 0:
						log("[E] "+ version +" import failed", "error")
						sys.exit(1)
					isFirstTableExist = self.check_table(db_name, db_user, db_password, first_table)
					isLastTableExist = self.check_table(db_name, db_user, db_password, last_table)
					isSchemaCreated=False
					if isFirstTableExist == True and isLastTableExist == True :
						isSchemaCreated=True
					elif isFirstTableExist == False and isLastTableExist == False :
						isImported=self.import_db_file(db_name, db_user, db_password, file_name)
						if(isImported==False):
							log("[I] "+ version  +" might being imported by some other process" ,"info")
							time.sleep(retryPatchAfterSeconds)
						isLastTableExist=self.check_table(db_name, db_user, db_password, last_table)
						if(isLastTableExist==True):
							isSchemaCreated=True
					elif isFirstTableExist == False or isLastTableExist == False :
						while(isFirstTableExist==False or isLastTableExist == False):
							log("[I] "+ version  +" is being imported by some other process" ,"info")
							time.sleep(retryPatchAfterSeconds)
							isFirstTableExist=self.check_table(db_name, db_user, db_password, first_table)
							isLastTableExist=self.check_table(db_name, db_user, db_password, last_table)
							if(isFirstTableExist==True and isLastTableExist==True):
								isSchemaCreated=True
					if isSchemaCreated == True:
						if is_unix:
							query = get_cmd + " -c \; -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] "+version +" import status has been updated", "info")
						else:
							if is_unix:
								query = get_cmd + " -c \; -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							log("[E] Updating "+version +" import status failed", "error")
							sys.exit(1)
					else:
						if is_unix:
							query = get_cmd + " -c \; -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						log("[E] "+version + " import failed!","error")
						sys.exit(1)

	def hasPendingPatches(self, db_name, db_user, db_password, version):
		get_cmd = self.get_jisql_cmd(db_user, db_password)
		if is_unix:
			query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and inst_by = '%s' and active = 'Y';\"" %(version,ranger_version)
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and inst_by = '%s' and active = 'Y';\" -c ;" %(version,ranger_version)
		jisql_log(query, db_password)
		output = check_output(query)
		if output.strip(version + " |"):
			return False
		else:
			return True

	def update_applied_patches_status(self,db_name, db_user, db_password,version):
		if self.hasPendingPatches(db_name, db_user, db_password,version) == True:
			get_cmd = self.get_jisql_cmd(db_user, db_password)
			if is_unix:
				query = get_cmd + " -c \; -query \"insert into x_db_version_h (id,version, inst_at, inst_by, updated_at, updated_by,active) values ( X_DB_VERSION_H_SEQ.nextval,'%s', sysdate, '%s', sysdate, '%s','Y');\"" %(version, ranger_version, client_host)
				jisql_log(query, db_password)
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"insert into x_db_version_h (id,version, inst_at, inst_by, updated_at, updated_by,active) values ( X_DB_VERSION_H_SEQ.nextval,'%s', sysdate, '%s', sysdate, '%s','Y');\" -c ;" %(version, ranger_version, client_host)
				jisql_log(query, db_password)
				ret = subprocess.call(query)
			if ret != 0:
				log("[E] "+ version +" status entry to x_db_version_h table failed", "error")
				sys.exit(1)
			else:
				log("[I] "+ version +" status entry to x_db_version_h table completed", "info")

        def is_new_install(self, xa_db_host, db_user, db_password, db_name):
                get_cmd = self.get_jisql_cmd(db_user, db_password)
                if is_unix:
                        query = get_cmd + " -c \; -query \"select * from v$version;\""
                elif os_name == "WINDOWS":
                        query = get_cmd + " -query \"select * from v$version;\" -c ;"
                output = check_output(query)
                if not output.strip('Production  |'):
                        sys.exit(0)
                get_cmd = self.get_jisql_cmd(db_user, db_password)
                version="J10001"
                if is_unix:
                        query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
                elif os_name == "WINDOWS":
                        query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
                output = check_output(query)
                if not output.strip(version + " |"):
                        sys.exit(0)

class PostgresConf(BaseDB):
	# Constructor
	def __init__(self, host,SQL_CONNECTOR_JAR,JAVA_BIN,db_ssl_enabled,db_ssl_required,db_ssl_verifyServerCertificate,javax_net_ssl_keyStore,javax_net_ssl_keyStorePassword,javax_net_ssl_trustStore,javax_net_ssl_trustStorePassword,db_ssl_auth_type):
		self.host = host.lower()
		self.SQL_CONNECTOR_JAR = SQL_CONNECTOR_JAR
		self.JAVA_BIN = JAVA_BIN
		self.db_ssl_enabled=db_ssl_enabled.lower()
		self.db_ssl_required=db_ssl_required.lower()
		self.db_ssl_verifyServerCertificate=db_ssl_verifyServerCertificate.lower()
		self.db_ssl_auth_type=db_ssl_auth_type.lower()
		self.javax_net_ssl_keyStore=javax_net_ssl_keyStore
		self.javax_net_ssl_keyStorePassword=javax_net_ssl_keyStorePassword
		self.javax_net_ssl_trustStore=javax_net_ssl_trustStore
		self.javax_net_ssl_trustStorePassword=javax_net_ssl_trustStorePassword

	def get_jisql_cmd(self, user, password, db_name):
		#TODO: User array for forming command
		path = RANGER_ADMIN_HOME
		self.JAVA_BIN = self.JAVA_BIN.strip("'")
		db_ssl_param=''
		db_ssl_cert_param=''
		if self.db_ssl_enabled == 'true':
			db_ssl_param="?ssl=%s" %(self.db_ssl_enabled)
			if self.db_ssl_verifyServerCertificate == 'true' or self.db_ssl_required == 'true':
				db_ssl_param="?ssl=%s" %(self.db_ssl_enabled)
				if self.db_ssl_auth_type == '1-way':
					db_ssl_cert_param=" -Djavax.net.ssl.trustStore=%s -Djavax.net.ssl.trustStorePassword=%s " %(self.javax_net_ssl_trustStore,self.javax_net_ssl_trustStorePassword)
				else:
					db_ssl_cert_param=" -Djavax.net.ssl.keyStore=%s -Djavax.net.ssl.keyStorePassword=%s -Djavax.net.ssl.trustStore=%s -Djavax.net.ssl.trustStorePassword=%s " %(self.javax_net_ssl_keyStore,self.javax_net_ssl_keyStorePassword,self.javax_net_ssl_trustStore,self.javax_net_ssl_trustStorePassword)
			else:
				db_ssl_param="?ssl=%s&sslfactory=org.postgresql.ssl.NonValidatingFactory" %(self.db_ssl_enabled)
		if is_unix:
			jisql_cmd = "%s %s -cp %s:%s/jisql/lib/* org.apache.util.sql.Jisql -driver postgresql -cstring jdbc:postgresql://%s/%s%s -u %s -p '%s' -noheader -trim -c \;" %(self.JAVA_BIN, db_ssl_cert_param,self.SQL_CONNECTOR_JAR,path, self.host, db_name, db_ssl_param,user, password)
		elif os_name == "WINDOWS":
			jisql_cmd = "%s %s -cp %s;%s\jisql\\lib\\* org.apache.util.sql.Jisql -driver postgresql -cstring jdbc:postgresql://%s/%s%s -u %s -p \"%s\" -noheader -trim" %(self.JAVA_BIN, db_ssl_cert_param,self.SQL_CONNECTOR_JAR, path, self.host, db_name, db_ssl_param,user, password)
		return jisql_cmd

	def check_connection(self, db_name, db_user, db_password):
		log("[I] Checking connection", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if is_unix:
			query = get_cmd + " -query \"SELECT 1;\""
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"SELECT 1;\" -c ;"
		jisql_log(query, db_password)
		output = check_output(query)
		if output.strip('1 |'):
			log("[I] connection success", "info")
			return True
		else:
			log("[E] Can't establish connection", "error")
			sys.exit(1)

	def import_db_file(self, db_name, db_user, db_password, file_name):
		isImported=False
		name = basename(file_name)
		if os.path.isfile(file_name):
			log("[I] Importing db schema to database " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if is_unix:
				query = get_cmd + " -input %s" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -input %s -c ;" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(query)
			if ret == 0:
				log("[I] "+name + " DB schema imported successfully","info")
				isImported=True
			else:
				log("[E] "+name + " DB schema import failed!","error")
		else:
			log("[E] DB schema file " + name+ " not found","error")
			sys.exit(1)
		return isImported

	def grant_audit_db_user(self, audit_db_name , db_user, audit_db_user, db_password, audit_db_password):
		log("[I] Granting permission to " + audit_db_user, "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password, audit_db_name)
		log("[I] Granting select and usage privileges to Postgres audit user '" + audit_db_user + "' on XA_ACCESS_AUDIT_SEQ", "info")
		if is_unix:
			query = get_cmd + " -query 'GRANT SELECT,USAGE ON XA_ACCESS_AUDIT_SEQ TO %s;'" % (audit_db_user)
			jisql_log(query, db_password)
			ret = subprocessCallWithRetry(shlex.split(query))
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"GRANT SELECT,USAGE ON XA_ACCESS_AUDIT_SEQ TO %s;\" -c ;" % (audit_db_user)
			jisql_log(query, db_password)
			ret = subprocessCallWithRetry(query)
		if ret != 0:
			log("[E] Granting select privileges to Postgres user '" + audit_db_user + "' failed", "error")
			sys.exit(1)

		log("[I] Granting insert privileges to Postgres audit user '" + audit_db_user + "' on XA_ACCESS_AUDIT table", "info")
		if is_unix:
			query = get_cmd + " -query 'GRANT INSERT ON XA_ACCESS_AUDIT TO %s;'" % (audit_db_user)
			jisql_log(query, db_password)
			ret = subprocessCallWithRetry(shlex.split(query))
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"GRANT INSERT ON XA_ACCESS_AUDIT TO %s;\" -c ;" % (audit_db_user)
			jisql_log(query, db_password)
			ret = subprocessCallWithRetry(query)
		if ret != 0:
			log("[E] Granting insert privileges to Postgres user '" + audit_db_user + "' failed", "error")
			sys.exit(1)

	def create_language_plpgsql(self,db_user, db_password, db_name):
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if is_unix:
			query = get_cmd + " -query \"SELECT 1 FROM pg_catalog.pg_language WHERE lanname='plpgsql';\""
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"SELECT 1 FROM pg_catalog.pg_language WHERE lanname='plpgsql';\" -c ;"
		jisql_log(query, db_password)
		output = check_output(query)
		if not output.strip('1 |'):
			if is_unix:
				query = get_cmd + " -query \"CREATE LANGUAGE plpgsql;\""
				jisql_log(query, db_password)
				ret = subprocessCallWithRetry(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"CREATE LANGUAGE plpgsql;\" -c ;"
				jisql_log(query, db_password)
				ret = subprocessCallWithRetry(query)
			if ret == 0:
				log("[I] LANGUAGE plpgsql created successfully", "info")
			else:
				log("[E] LANGUAGE plpgsql creation failed", "error")
				sys.exit(1)

	def import_db_patches(self, db_name, db_user, db_password, file_name):
		self.create_language_plpgsql(db_user, db_password, db_name)
		name = basename(file_name)
		if os.path.isfile(file_name):
			version = name.split('-')[0]
			log("[I] Executing patch on " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if is_unix:
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
			jisql_log(query, db_password)
			output = check_output(query)
			if output.strip(version + " |"):
				log("[I] Patch "+ name  +" is already applied" ,"info")
			else:
				if is_unix:
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					while(output.strip(version + " |")):
						log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
						time.sleep(retryPatchAfterSeconds)
						jisql_log(query, db_password)
						output = check_output(query)
				else:
					if is_unix:
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', current_timestamp, '%s', current_timestamp, '%s','N') ;\"" %(version,ranger_version,client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', current_timestamp, '%s', current_timestamp, '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret == 0:
						log ("[I] Patch "+ name +" is being applied..","info")
					else:
						log("[E] Patch "+ name +" failed", "error")
					if is_unix:
						query = get_cmd + " -input %s" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -input %s -c ;" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret != 0:
						if is_unix:
							query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
						jisql_log(query, db_password)
						output = check_output(query)
						if output.strip(version + " |"):
							ret=0
							log("[I] Patch "+ name  +" has been applied by some other process!" ,"info")
					if ret == 0:
						log("[I] "+name + " patch applied","info")
						if is_unix:
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] Patch version updated", "info")
						else:
							if is_unix:
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							log("[E] Updating patch version failed", "error")
							sys.exit(1)
					else:
						if is_unix:
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						log("[E] "+name + " import failed!","error")
						sys.exit(1)

	def import_auditdb_patches(self, xa_sqlObj,xa_db_host, audit_db_host, db_name, audit_db_name, db_user, audit_db_user, db_password, audit_db_password, file_name, TABLE_NAME):
		log("[I] --------- Checking XA_ACCESS_AUDIT table to apply audit db patches --------- ","info")
		self.create_language_plpgsql(db_user, db_password, audit_db_name)
		output = self.check_table(audit_db_name, db_user, db_password, TABLE_NAME)
		if output == True:
			name = basename(file_name)
			if os.path.isfile(file_name):
				version = name.split('-')[0]
				log("[I] Executing patch on " + audit_db_name + " from file: " + name,"info")
				get_cmd1 = xa_sqlObj.get_jisql_cmd(db_user, db_password, db_name)
				if is_unix:
					query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					log("[I] Patch "+ name  +" is already applied" ,"info")
				else:
					if is_unix:
						query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
						while(output.strip(version + " |")):
							log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
							time.sleep(retryPatchAfterSeconds)
							jisql_log(query, db_password)
							output = check_output(query)
					else:
						if is_unix:
							query = get_cmd1 + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', current_timestamp, '%s', current_timestamp, '%s','N') ;\"" %(version,ranger_version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd1 + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', current_timestamp, '%s', current_timestamp, '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log ("[I] Patch "+ name +" is being applied..","info")
						else:
							log("[E] Patch "+ name +" failed", "error")
						get_cmd2 = self.get_jisql_cmd(db_user, db_password, audit_db_name)
						if is_unix:
							query = get_cmd2 + " -input %s" %file_name
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd2 + " -input %s -c ;" %file_name
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] "+name + " patch applied","info")
							if is_unix:
								query = get_cmd1 + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd1 + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0:
								log("[I] Patch version updated", "info")
							else:
								if is_unix:
									query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								log("[E] Updating patch version failed", "error")
								sys.exit(1)
						else:
							if is_unix:
								query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							log("[E] "+name + " import failed!","error")
							sys.exit(1)
		else:
			log("[I] Table XA_ACCESS_AUDIT does not exists in " +audit_db_name,"error")
			sys.exit(1)

	def check_table(self, db_name, db_user, db_password, TABLE_NAME):
		log("[I] Verifying table " + TABLE_NAME +" in database " + db_name, "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if is_unix:
			query = get_cmd + " -query \"select * from (select table_name from information_schema.tables where table_catalog='%s' and table_name = '%s') as temp;\"" %(db_name , TABLE_NAME)
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"select * from (select table_name from information_schema.tables where table_catalog='%s' and table_name = '%s') as temp;\" -c ;" %(db_name , TABLE_NAME)
		jisql_log(query, db_password)
		output = check_output(query)
		if output.strip(TABLE_NAME +" |"):
			log("[I] Table " + TABLE_NAME +" already exists in database " + db_name, "info")
			return True
		else:
			log("[I] Table " + TABLE_NAME +" does not exist in database " + db_name, "info")
			return False

	def auditdb_operation(self, xa_db_host, audit_db_host, db_name, audit_db_name, db_user, audit_db_user, db_password, audit_db_password, file_name, TABLE_NAME):
		log("[I] --------- Check admin user connection ---------","info")
		self.check_connection(audit_db_name, db_user, db_password)
		log("[I] --------- Check audit user connection ---------","info")
		self.check_connection(audit_db_name, audit_db_user, audit_db_password)
		log("[I] --------- Check table ---------","info")
		output = self.check_table(audit_db_name, audit_db_user, audit_db_password, TABLE_NAME)
		if output == False:
			self.import_db_file(audit_db_name, db_user, db_password, file_name)
		self.grant_audit_db_user(audit_db_name ,db_user, audit_db_user, db_password,audit_db_password)

	def execute_java_patches(self, xa_db_host, db_user, db_password, db_name):
		my_dict = {}
		version = ""
		className = ""
		app_home = os.path.join(RANGER_ADMIN_HOME,"ews","webapp")
		ranger_log = os.path.join(RANGER_ADMIN_HOME,"ews","logs")
		javaFiles = os.path.join(app_home,"WEB-INF","classes","org","apache","ranger","patch")

		if not os.path.exists(javaFiles):
			log("[I] No java patches to apply!","info")
		else:
			files = os.listdir(javaFiles)
			if files:
				for filename in files:
					f = re.match(java_patch_regex,filename)
					if f:
						className = re.match("(Patch.*?)_.*.class",filename)
						className = className.group(1)
						version = re.match("Patch.*?_(.*).class",filename)
						version = version.group(1)
						key3 = int(version.strip("J"))
						my_dict[key3] = filename

			keylist = my_dict.keys()
			keylist.sort()
			for key in keylist:
				#print "%s: %s" % (key, my_dict[key])
				version = str(key)
				className = my_dict[key]
				className = className.strip(".class")
				if version != "":
					get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
					if is_unix:
						query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\"" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
						log("[I] Java patch "+ className  +" is already applied" ,"info")
					else:
						if is_unix:
							query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\"" %(version)
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\" -c ;" %(version)
						jisql_log(query, db_password)
						output = check_output(query)
						if output.strip(version + " |"):
							while(output.strip(version + " |")):
								log("[I] Java patch "+ className  +" is being applied by some other process" ,"info")
								time.sleep(retryPatchAfterSeconds)
								jisql_log(query, db_password)
								output = check_output(query)
						else:
							if is_unix:
								query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('J%s', current_timestamp, '%s', current_timestamp, '%s','N') ;\"" %(version,ranger_version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('J%s', current_timestamp, '%s', current_timestamp, '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0:
								log ("[I] java patch "+ className +" is being applied..","info")
							else:
								log("[E] java patch "+ className +" failed", "error")
								sys.exit(1)
							if is_unix:
								path = os.path.join("%s","WEB-INF","classes","conf:%s","WEB-INF","classes","lib","*:%s","WEB-INF",":%s","META-INF",":%s","WEB-INF","lib","*:%s","WEB-INF","classes",":%s","WEB-INF","classes","META-INF:%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							elif os_name == "WINDOWS":
								path = os.path.join("%s","WEB-INF","classes","conf;%s","WEB-INF","classes","lib","*;%s","WEB-INF",";%s","META-INF",";%s","WEB-INF","lib","*;%s","WEB-INF","classes",";%s","WEB-INF","classes","META-INF;%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							get_java_cmd = "%s -XX:MetaspaceSize=100m -XX:MaxMetaspaceSize=200m -Xmx%s -Xms1g -Dlogdir=%s -Dlog4j.configuration=db_patch.log4j.xml -cp %s org.apache.ranger.patch.%s"%(self.JAVA_BIN,max_memory,ranger_log,path,className)
							if is_unix:
								ret = subprocess.call(shlex.split(get_java_cmd))
							elif os_name == "WINDOWS":
								ret = subprocess.call(get_java_cmd)
							if ret == 0:
								if is_unix:
									query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='J%s' and active='N' and updated_by='%s';\"" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='J%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								if ret == 0:
									log ("[I] java patch "+ className +" is applied..","info")
								else:
									if is_unix:
										query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\"" %(version,client_host)
										jisql_log(query, db_password)
										ret = subprocess.call(shlex.split(query))
									elif os_name == "WINDOWS":
										query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
										jisql_log(query, db_password)
										ret = subprocess.call(query)
									log("[E] java patch "+ className +" failed", "error")
									sys.exit(1)
							else:
								if is_unix:
									query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\"" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								log("[E] applying java patch "+ className +" failed", "error")
								sys.exit(1)

	def change_admin_default_password(self, xa_db_host, db_user, db_password, db_name,userName,oldPassword,newPassword):
		version = ""
		className = "ChangePasswordUtil"
                version = dbversionBasedOnUserName(userName)
		app_home = os.path.join(RANGER_ADMIN_HOME,"ews","webapp")
		ranger_log = os.path.join(RANGER_ADMIN_HOME,"ews","logs")
		filePath = os.path.join(app_home,"WEB-INF","classes","org","apache","ranger","patch","cliutil","ChangePasswordUtil.class")
		if os.path.exists(filePath):
			if version != "":
				get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
				if is_unix:
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
				elif os_name == "WINDOWS":
                                        query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
                                        log("[I] Ranger "+ userName +" default password has already been changed!!","info")
				else:
					if is_unix:
						query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
                                                countTries = 0
						while(output.strip(version + " |")):
                                                        if countTries < 3:
                                                                log("[I] Ranger Password change utility is being executed by some other process" ,"info")
                                                                time.sleep(retryPatchAfterSeconds)
                                                                jisql_log(query, db_password)
                                                                output = check_output(query)
                                                                countTries += 1
                                                        else:
                                                                log("[E] Tried updating the password "+ str(countTries) + " times","error")
                                                                log("[E] If Ranger "+  userName +" user password is not being changed by some other process then manually delete the entry from ranger database table x_db_version_h table where version is " + version ,"error")
                                                                sys.exit(1)
					else:
						if is_unix:
							query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', current_timestamp, '%s', current_timestamp, '%s','N') ;\"" %(version,ranger_version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', current_timestamp, '%s', current_timestamp, '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
                                                        log ("[I] Ranger "+ userName +" default password change request is in process..","info")
						else:
                                                        log("[E] Ranger "+ userName +" default password change request failed", "error")
							sys.exit(1)
						if is_unix:
							path = os.path.join("%s","WEB-INF","classes","conf:%s","WEB-INF","classes","lib","*:%s","WEB-INF",":%s","META-INF",":%s","WEB-INF","lib","*:%s","WEB-INF","classes",":%s","WEB-INF","classes","META-INF:%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
						elif os_name == "WINDOWS":
							path = os.path.join("%s","WEB-INF","classes","conf;%s","WEB-INF","classes","lib","*;%s","WEB-INF",";%s","META-INF",";%s","WEB-INF","lib","*;%s","WEB-INF","classes",";%s","WEB-INF","classes","META-INF;%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
						get_java_cmd = "%s -XX:MetaspaceSize=100m -XX:MaxMetaspaceSize=200m -Xmx%s -Xms1g -Dlogdir=%s -Dlog4j.configuration=db_patch.log4j.xml -cp %s org.apache.ranger.patch.cliutil.%s %s %s %s -default"%(self.JAVA_BIN,max_memory,ranger_log,path,className,userName,oldPassword,newPassword)
						if is_unix:
							status = subprocess.call(shlex.split(get_java_cmd))
						elif os_name == "WINDOWS":
							status = subprocess.call(get_java_cmd)
						if status == 0 or status==2:
							if is_unix:
								query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0 and status == 0:
                                                                log ("[I] Ranger "+ userName +" default password change request processed successfully..","info")
							elif ret == 0 and status == 2:
                                                                log ("[I] Ranger "+ userName +" default password change request process skipped!","info")
							else:
								if is_unix:
									query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
                                                                log("[E] Ranger "+ userName +" default password change request failed", "error")
								sys.exit(1)
						else:
							if is_unix:
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
                                                        log("[E] Ranger "+ userName +" default password change request failed", "error")
							sys.exit(1)

	def create_version_history_table(self, db_name, db_user, db_password, file_name,table_name):
		name = basename(file_name)
		if os.path.isfile(file_name):
			isTableExist=self.check_table(db_name, db_user, db_password, table_name)
			if isTableExist==False:
				log("[I] Importing "+table_name+" table schema to database " + db_name + " from file: " + name,"info")
			while(isTableExist==False):
				get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
				if is_unix:
					query = get_cmd + " -input %s" %file_name
					jisql_log(query, db_password)
					ret = subprocess.call(shlex.split(query))
				elif os_name == "WINDOWS":
					query = get_cmd + " -input %s -c ;" %file_name
					jisql_log(query, db_password)
					ret = subprocess.call(query)
				if ret == 0:
					log("[I] "+name + " file imported successfully","info")
				else:
					log("[E] "+name + " file import failed!","error")
					time.sleep(30)
				isTableExist=self.check_table(db_name, db_user, db_password, table_name)
		else:
			log("[E] Table schema file " + name+ " not found","error")
			sys.exit(1)

	def import_core_db_schema(self, db_name, db_user, db_password, file_name,first_table,last_table):
		version = 'CORE_DB_SCHEMA'
		if os.path.isfile(file_name):
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if is_unix:
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
			jisql_log(query, db_password)
			output = check_output(query)
			if output.strip(version + " |"):
				log("[I] "+version+" is already imported" ,"info")
			else:
				if is_unix:
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					while(output.strip(version + " |")):
						log("[I] "+ version  +" is being imported by some other process" ,"info")
						time.sleep(retryPatchAfterSeconds)
						jisql_log(query, db_password)
						output = check_output(query)
				else:
					if is_unix:
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', current_timestamp, '%s', current_timestamp, '%s','N') ;\"" %(version,ranger_version,client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', now(), '%s', now(), '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret != 0:
						log("[E] "+ version +" import failed", "error")
						sys.exit(1)
					isFirstTableExist = self.check_table(db_name, db_user, db_password, first_table)
					isLastTableExist = self.check_table(db_name, db_user, db_password, last_table)
					isSchemaCreated=False
					if isFirstTableExist == True and isLastTableExist == True :
						isSchemaCreated=True
					elif isFirstTableExist == False and isLastTableExist == False :
						isImported=self.import_db_file(db_name, db_user, db_password, file_name)
						if(isImported==False):
							log("[I] "+ version  +" might being imported by some other process" ,"info")
							time.sleep(retryPatchAfterSeconds)
						isLastTableExist=self.check_table(db_name, db_user, db_password, last_table)
						if(isLastTableExist==True):
							isSchemaCreated=True
					elif isFirstTableExist == False or isLastTableExist == False :
						while(isFirstTableExist == False or isLastTableExist==False):
							log("[I] "+ version  +" is being imported by some other process" ,"info")
							time.sleep(retryPatchAfterSeconds)
							isFirstTableExist=self.check_table(db_name, db_user, db_password, first_table)
							isLastTableExist=self.check_table(db_name, db_user, db_password, last_table)
							if(isFirstTableExist == True and isLastTableExist==True):
								isSchemaCreated=True
					if isSchemaCreated == True:
						if is_unix:
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] "+version +" import status has been updated", "info")
						else:
							if is_unix:
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							log("[E] Updating "+version +" import status failed", "error")
							sys.exit(1)
					else:
						if is_unix:
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\"" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						log("[E] "+version + " import failed!","error")
						sys.exit(1)

	def hasPendingPatches(self, db_name, db_user, db_password, version):
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if is_unix:
			query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and inst_by = '%s' and active = 'Y';\"" %(version,ranger_version)
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and inst_by = '%s' and active = 'Y';\" -c ;" %(version,ranger_version)
		jisql_log(query, db_password)
		output = check_output(query)
		if output.strip(version + " |"):
			return False
		else:
			return True

	def update_applied_patches_status(self,db_name, db_user, db_password,version):
		if self.hasPendingPatches(db_name, db_user, db_password,version) == True:
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if is_unix:
				query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', current_timestamp, '%s', current_timestamp, '%s','Y') ;\"" %(version,ranger_version,client_host)
				jisql_log(query, db_password)
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', current_timestamp, '%s', current_timestamp, '%s','Y') ;\" -c ;" %(version,ranger_version,client_host)
				jisql_log(query, db_password)
				ret = subprocess.call(query)
			if ret != 0:
				log("[E] "+ version +" status entry to x_db_version_h table failed", "error")
				sys.exit(1)
			else:
				log("[I] "+ version +" status entry to x_db_version_h table completed", "info")

        def is_new_install(self, xa_db_host, db_user, db_password, db_name):
                get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
                if is_unix:
                        query = get_cmd + " -query \"SELECT 1;\""
                elif os_name == "WINDOWS":
                        query = get_cmd + " -query \"SELECT 1;\" -c ;"
                output = check_output(query)
                if not output.strip('1 |'):
                        sys.exit(0)
                get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
                version="J10001"
                if is_unix:
                        query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
                elif os_name == "WINDOWS":
                        query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
                output = check_output(query)
                if not output.strip(version + " |"):
                        sys.exit(0)

class SqlServerConf(BaseDB):
	# Constructor
	def __init__(self, host, SQL_CONNECTOR_JAR, JAVA_BIN):
		self.host = host
		self.SQL_CONNECTOR_JAR = SQL_CONNECTOR_JAR
		self.JAVA_BIN = JAVA_BIN

	def get_jisql_cmd(self, user, password, db_name):
		#TODO: User array for forming command
		path = RANGER_ADMIN_HOME
		self.JAVA_BIN = self.JAVA_BIN.strip("'")
		if is_unix:
			jisql_cmd = "%s -cp %s:%s/jisql/lib/* org.apache.util.sql.Jisql -user %s -p '%s' -driver mssql -cstring jdbc:sqlserver://%s\\;databaseName=%s -noheader -trim"%(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, path, user, password, self.host,db_name)
		elif os_name == "WINDOWS":
			jisql_cmd = "%s -cp %s;%s\\jisql\\lib\\* org.apache.util.sql.Jisql -user %s -p \"%s\" -driver mssql -cstring jdbc:sqlserver://%s;databaseName=%s -noheader -trim"%(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, path, user, password, self.host,db_name)
		return jisql_cmd

	def check_connection(self, db_name, db_user, db_password):
		log("[I] Checking connection", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if is_unix:
			query = get_cmd + " -c \; -query \"SELECT 1;\""
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"SELECT 1;\" -c ;"
		jisql_log(query, db_password)
		output = check_output(query)
		if output.strip('1 |'):
			log("[I] Connection success", "info")
			return True
		else:
			log("[E] Can't establish connection", "error")
			sys.exit(1)

	def import_db_file(self, db_name, db_user, db_password, file_name):
		isImported=False
		name = basename(file_name)
		if os.path.isfile(file_name):
			log("[I] Importing db schema to database " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if is_unix:
				query = get_cmd + " -input %s" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -input %s" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(query)
			if ret == 0:
				log("[I] "+name + " DB schema imported successfully","info")
				isImported=True
			else:
				log("[E] "+name + " DB Schema import failed!","error")
		else:
			log("[E] DB schema file " + name+ " not found","error")
			sys.exit(1)
		return isImported

	def check_table(self, db_name, db_user, db_password, TABLE_NAME):
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if is_unix:
			query = get_cmd + " -c \; -query \"SELECT TABLE_NAME FROM information_schema.tables where table_name = '%s';\"" %(TABLE_NAME)
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"SELECT TABLE_NAME FROM information_schema.tables where table_name = '%s';\" -c ;" %(TABLE_NAME)
		jisql_log(query, db_password)
		output = check_output(query)
		if output.strip(TABLE_NAME + " |"):
			log("[I] Table '" + TABLE_NAME + "' already exists in  database '" + db_name + "'","info")
			return True
		else:
			log("[I] Table '" + TABLE_NAME + "' does not exist in database '" + db_name + "'","info")
			return False

	def grant_audit_db_user(self, audit_db_name, db_user, audit_db_user, db_password, audit_db_password,TABLE_NAME):
		log("[I] Granting permission to audit user '" + audit_db_user + "' on db '" + audit_db_name + "'","info")
		get_cmd = self.get_jisql_cmd(db_user, db_password,audit_db_name)
		if is_unix:
			query = get_cmd + " -c \; -query \"USE %s GRANT SELECT,INSERT to %s;\"" %(audit_db_name ,audit_db_user)
			jisql_log(query, db_password)
			ret = subprocessCallWithRetry(shlex.split(query))
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"USE %s GRANT SELECT,INSERT to %s;\" -c ;" %(audit_db_name ,audit_db_user)
			jisql_log(query, db_password)
			ret = subprocessCallWithRetry(query)
		if ret != 0 :
			sys.exit(1)
		else:
			log("[I] Permission granted to audit user " + audit_db_user , "info")

	def import_db_patches(self, db_name, db_user, db_password, file_name):
		name = basename(file_name)
		if os.path.isfile(file_name):
			version = name.split('-')[0]
			log("[I] Executing patch on " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if is_unix:
				query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
			jisql_log(query, db_password)
			output = check_output(query)
			if output.strip(version + " |"):
				log("[I] Patch "+ name  +" is already applied" ,"info")
			else:
				if is_unix:
					query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					while(output.strip(version + " |")):
						log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
						time.sleep(retryPatchAfterSeconds)
						jisql_log(query, db_password)
						output = check_output(query)
				else:
					if is_unix:
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c \;" %(version,ranger_version,client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret == 0:
						log ("[I] Patch "+ name +" is being applied..","info")
					else:
						log("[E] Patch "+ name +" failed", "error")
					if is_unix:
						query = get_cmd + " -input %s" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -input %s" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret != 0:
						time.sleep(1)
						if is_unix:
							query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
						jisql_log(query, db_password)
						output = check_output(query)
						if output.strip(version + " |"):
							ret=0
							log("[I] Patch "+ name  +" has been applied by some other process!" ,"info")
					if ret == 0:
						log("[I] "+name + " patch applied","info")
						if is_unix:
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] Patch version updated", "info")
						else:
							if is_unix:
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							log("[E] Updating patch version failed", "error")
							sys.exit(1)
					else:
						if is_unix:
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						log("[E] "+name + " import failed!","error")
						sys.exit(1)

	def import_auditdb_patches(self, xa_sqlObj,xa_db_host, audit_db_host, db_name, audit_db_name, db_user, audit_db_user, db_password, audit_db_password, file_name, TABLE_NAME):
		log("[I] --------- Checking XA_ACCESS_AUDIT table to apply audit db patches --------- ","info")
		output = self.check_table(audit_db_name, db_user, db_password, TABLE_NAME)
		if output == True:
			name = basename(file_name)
			if os.path.isfile(file_name):
				version = name.split('-')[0]
				log("[I] Executing patch on " + audit_db_name + " from file: " + name,"info")
				get_cmd1 = xa_sqlObj.get_jisql_cmd(db_user, db_password, db_name)
				if is_unix:
					query = get_cmd1 + " -c \; query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					log("[I] Patch "+ name  +" is already applied" ,"info")
				else:
					if is_unix:
						query = get_cmd1 + " -c \; query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
						while(output.strip(version + " |")):
							log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
							time.sleep(retryPatchAfterSeconds)
							jisql_log(query, db_password)
							output = check_output(query)
					else:
						get_cmd2 = self.get_jisql_cmd(db_user, db_password, audit_db_name)
						if is_unix:
							query = get_cmd1 + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c \;" %(version,ranger_version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd1 + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log ("[I] Patch "+ name +" is being applied..","info")
						else:
							log("[E] Patch "+ name +" failed", "error")
						if is_unix:
							query = get_cmd2 + " -input %s" %file_name
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd2 + " -input %s" %file_name
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] "+name + " patch applied","info")
							if is_unix:
								query = get_cmd1 + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd1 + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0:
								log("[I] Patch version updated", "info")
							else:
								if is_unix:
									query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								log("[E] Updating patch version failed", "error")
								sys.exit(1)
						else:
							if is_unix:
								query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							log("[E] "+name + " import failed!","error")
							sys.exit(1)
		else:
			log("[I] Table XA_ACCESS_AUDIT does not exists in " +audit_db_name,"error")
			sys.exit(1)

	def auditdb_operation(self, xa_db_host, audit_db_host, db_name, audit_db_name,db_user, audit_db_user, db_password, audit_db_password, file_name, TABLE_NAME):
		log("[I] --------- Check admin user connection --------- ","info")
		self.check_connection(audit_db_name, db_user, db_password)
		log("[I] --------- Check audit user connection --------- ","info")
		self.check_connection(audit_db_name, audit_db_user, audit_db_password)
		log("[I] --------- Check audit table exists --------- ","info")
		output = self.check_table(audit_db_name, db_user, db_password, TABLE_NAME)
		if output == False:
			self.import_db_file(audit_db_name ,db_user, db_password, file_name)
		self.grant_audit_db_user( audit_db_name ,db_user, audit_db_user, db_password,audit_db_password,TABLE_NAME)

	def execute_java_patches(self, xa_db_host, db_user, db_password, db_name):
		my_dict = {}
		version = ""
		className = ""
		app_home = os.path.join(RANGER_ADMIN_HOME,"ews","webapp")
		ranger_log = os.path.join(RANGER_ADMIN_HOME,"ews","logs")
		javaFiles = os.path.join(app_home,"WEB-INF","classes","org","apache","ranger","patch")

		if not os.path.exists(javaFiles):
			log("[I] No java patches to apply!","info")
		else:
			files = os.listdir(javaFiles)
			if files:
				for filename in files:
					f = re.match(java_patch_regex,filename)
					if f:
						className = re.match("(Patch.*?)_.*.class",filename)
						className = className.group(1)
						version = re.match("Patch.*?_(.*).class",filename)
						version = version.group(1)
						key3 = int(version.strip("J"))
						my_dict[key3] = filename

			keylist = my_dict.keys()
			keylist.sort()
			for key in keylist:
				#print "%s: %s" % (key, my_dict[key])
				version = str(key)
				className = my_dict[key]
				className = className.strip(".class")
				if version != "":
					get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
					if is_unix:
						query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\" -c \;" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
						log("[I] Java patch "+ className  +" is already applied" ,"info")
					else:
						if is_unix:
							query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\" -c \;" %(version)
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\" -c ;" %(version)
						jisql_log(query, db_password)
						output = check_output(query)
						if output.strip(version + " |"):
							while(output.strip(version + " |")):
								log("[I] Java patch "+ className  +" is being applied by some other process" ,"info")
								time.sleep(retryPatchAfterSeconds)
								jisql_log(query, db_password)
								output = check_output(query)
						else:
							if is_unix:
								query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('J%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c \;" %(version,ranger_version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('J%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0:
								log ("[I] java patch "+ className +" is being applied..","info")
							else:
								log("[E] java patch "+ className +" failed", "error")
								sys.exit(1)
							if is_unix:
								path = os.path.join("%s","WEB-INF","classes","conf:%s","WEB-INF","classes","lib","*:%s","WEB-INF",":%s","META-INF",":%s","WEB-INF","lib","*:%s","WEB-INF","classes",":%s","WEB-INF","classes","META-INF:%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							elif os_name == "WINDOWS":
								path = os.path.join("%s","WEB-INF","classes","conf;%s","WEB-INF","classes","lib","*;%s","WEB-INF",";%s","META-INF",";%s","WEB-INF","lib","*;%s","WEB-INF","classes",";%s","WEB-INF","classes","META-INF;%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							get_java_cmd = "%s -XX:MetaspaceSize=100m -XX:MaxMetaspaceSize=200m -Xmx%s -Xms1g -Dlogdir=%s -Dlog4j.configuration=db_patch.log4j.xml -cp %s org.apache.ranger.patch.%s"%(self.JAVA_BIN,max_memory,ranger_log,path,className)
							if is_unix:
								ret = subprocess.call(shlex.split(get_java_cmd))
							elif os_name == "WINDOWS":
								ret = subprocess.call(get_java_cmd)
							if ret == 0:
								if is_unix:
									query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='J%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='J%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								if ret == 0:
									log ("[I] java patch "+ className +" is applied..","info")
								else:
									if is_unix:
										query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
										jisql_log(query, db_password)
										ret = subprocess.call(shlex.split(query))
									elif os_name == "WINDOWS":
										query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
										jisql_log(query, db_password)
										ret = subprocess.call(query)
									log("[E] java patch "+ className +" failed", "error")
									sys.exit(1)
							else:
								if is_unix:
									query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								log("[E] applying java patch "+ className +" failed", "error")
								sys.exit(1)

	def change_admin_default_password(self, xa_db_host, db_user, db_password, db_name,userName,oldPassword,newPassword):
		version = ""
		className = "ChangePasswordUtil"
                version = dbversionBasedOnUserName(userName)
		app_home = os.path.join(RANGER_ADMIN_HOME,"ews","webapp")
		ranger_log = os.path.join(RANGER_ADMIN_HOME,"ews","logs")
		filePath = os.path.join(app_home,"WEB-INF","classes","org","apache","ranger","patch","cliutil","ChangePasswordUtil.class")
		if os.path.exists(filePath):
			if version != "":
				get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
				if is_unix:
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c \;" %(version)
				elif os_name == "WINDOWS":
                                        query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
                                        log("[I] Ranger "+ userName +" default password has already been changed!!","info")
				else:
					if is_unix:
						query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c \;" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
                                                countTries = 0
						while(output.strip(version + " |")):
                                                        if countTries < 3:
                                                                log("[I] Ranger Password change utility is being executed by some other process" ,"info")
                                                                time.sleep(retryPatchAfterSeconds)
                                                                jisql_log(query, db_password)
                                                                output = check_output(query)
                                                                countTries += 1
                                                        else:
                                                                log("[E] Tried updating the password "+ str(countTries) + " times","error")
                                                                log("[E] If Ranger "+  userName +" user password is not being changed by some other process then manually delete the entry from ranger database table x_db_version_h table where version is " + version ,"error")
                                                                sys.exit(1)
					else:
						if is_unix:
							query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c \;" %(version,ranger_version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
                                                        log ("[I] Ranger "+ userName +" default password change request is in process..","info")
						else:
                                                        log("[E] Ranger "+ userName +" default password change request failed", "error")
                                                        sys.exit(1)
						if is_unix:
							path = os.path.join("%s","WEB-INF","classes","conf:%s","WEB-INF","classes","lib","*:%s","WEB-INF",":%s","META-INF",":%s","WEB-INF","lib","*:%s","WEB-INF","classes",":%s","WEB-INF","classes","META-INF:%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
						elif os_name == "WINDOWS":
							path = os.path.join("%s","WEB-INF","classes","conf;%s","WEB-INF","classes","lib","*;%s","WEB-INF",";%s","META-INF",";%s","WEB-INF","lib","*;%s","WEB-INF","classes",";%s","WEB-INF","classes","META-INF;%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
						get_java_cmd = "%s -XX:MetaspaceSize=100m -XX:MaxMetaspaceSize=200m -Xmx%s -Xms1g -Dlogdir=%s -Dlog4j.configuration=db_patch.log4j.xml -cp %s org.apache.ranger.patch.cliutil.%s %s %s %s -default"%(self.JAVA_BIN,max_memory,ranger_log,path,className,userName,oldPassword,newPassword)
						if is_unix:
							status = subprocess.call(shlex.split(get_java_cmd))
						elif os_name == "WINDOWS":
							status = subprocess.call(get_java_cmd)
						if status == 0 or status==2:
							if is_unix:
								query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0 and status == 0:
                                                                log ("[I] Ranger "+ userName +" default password change request processed successfully..","info")
							elif ret == 0 and status == 2:
                                                                log ("[I] Ranger "+ userName +" default password change request process skipped!","info")
							else:
								if is_unix:
									query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
                                                                log("[E] Ranger "+ userName +" default password change request failed", "error")
								sys.exit(1)
						else:
							if is_unix:
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
                                                        log("[E] Ranger "+ userName +" default password change request failed", "error")
							sys.exit(1)

	def create_version_history_table(self, db_name, db_user, db_password, file_name,table_name):
		name = basename(file_name)
		if os.path.isfile(file_name):
			isTableExist=self.check_table(db_name, db_user, db_password, table_name)
			if isTableExist==False:
				log("[I] Importing "+table_name+" table schema to database " + db_name + " from file: " + name,"info")
			while(isTableExist==False):
				get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
				if is_unix:
					query = get_cmd + " -input %s" %file_name
					jisql_log(query, db_password)
					ret = subprocess.call(shlex.split(query))
				elif os_name == "WINDOWS":
					query = get_cmd + " -input %s" %file_name
					jisql_log(query, db_password)
					ret = subprocess.call(query)
				if ret == 0:
					log("[I] "+name + " file imported successfully","info")
				else:
					log("[E] "+name + " file import failed!","error")
					time.sleep(30)
				isTableExist=self.check_table(db_name, db_user, db_password, table_name)
		else:
			log("[E] Table schema file " + name+ " not found","error")
			sys.exit(1)

	def import_core_db_schema(self, db_name, db_user, db_password, file_name,first_table,last_table):
		version = 'CORE_DB_SCHEMA'
		if os.path.isfile(file_name):
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if is_unix:
				query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
			jisql_log(query, db_password)
			output = check_output(query)
			if output.strip(version + " |"):
				log("[I] "+version+" is already imported" ,"info")
			else:
				if is_unix:
					query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					while(output.strip(version + " |")):
						log("[I] "+ version  +" is being imported by some other process" ,"info")
						time.sleep(retryPatchAfterSeconds)
						jisql_log(query, db_password)
						output = check_output(query)
				else:
					if is_unix:
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c \;" %(version,ranger_version,client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret != 0:
						log("[E] "+ version +" import failed", "error")
						sys.exit(1)
					isFirstTableExist = self.check_table(db_name, db_user, db_password, first_table)
					isLastTableExist = self.check_table(db_name, db_user, db_password, last_table)
					isSchemaCreated=False
					if isFirstTableExist == True and isLastTableExist == True :
						isSchemaCreated=True
					elif isFirstTableExist == False and isLastTableExist == False :
						isImported=self.import_db_file(db_name, db_user, db_password, file_name)
						if(isImported==False):
							log("[I] "+ version  +" might being imported by some other process" ,"info")
							time.sleep(retryPatchAfterSeconds)
						isLastTableExist=self.check_table(db_name, db_user, db_password, last_table)
						if(isLastTableExist==True):
							isSchemaCreated=True
					elif isFirstTableExist == False or isLastTableExist == False :
						while(isFirstTableExist == False or isLastTableExist==False):
							log("[I] "+ version  +" is being imported by some other process" ,"info")
							time.sleep(retryPatchAfterSeconds)
							isFirstTableExist=self.check_table(db_name, db_user, db_password, first_table)
							isLastTableExist=self.check_table(db_name, db_user, db_password, last_table)
							if(isFirstTableExist == True and isLastTableExist==True):
								isSchemaCreated=True
					if isSchemaCreated == True:
						if is_unix:
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] "+version +" import status has been updated", "info")
						else:
							if is_unix:
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							log("[E] Updating "+version +" import status failed", "error")
							sys.exit(1)
					else:
						if is_unix:
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						log("[E] "+version + " import failed!","error")
						sys.exit(1)

	def hasPendingPatches(self, db_name, db_user, db_password, version):
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if is_unix:
			query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and inst_by = '%s' and active = 'Y';\"" %(version,ranger_version)
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and inst_by = '%s' and active = 'Y';\" -c ;" %(version,ranger_version)
		jisql_log(query, db_password)
		output = check_output(query)
		if output.strip(version + " |"):
			return False
		else:
			return True

	def update_applied_patches_status(self,db_name, db_user, db_password,version):
		if self.hasPendingPatches(db_name, db_user, db_password,version) == True:
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if is_unix:
				query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','Y') ;\" -c \;" %(version,ranger_version,client_host)
				jisql_log(query, db_password)
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','Y') ;\" -c ;" %(version,ranger_version,client_host)
				jisql_log(query, db_password)
				ret = subprocess.call(query)
			if ret != 0:
				log("[E] "+ version +" status entry to x_db_version_h table failed", "error")
				sys.exit(1)
			else:
				log("[I] "+ version +" status entry to x_db_version_h table completed", "info")

        def is_new_install(self, xa_db_host, db_user, db_password, db_name):
                get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
                if is_unix:
                        query = get_cmd + " -c \; -query \"SELECT 1;\""
                elif os_name == "WINDOWS":
                        query = get_cmd + " -query \"SELECT 1;\" -c ;"
                output = check_output(query)
                if not output.strip('1 |'):
                        sys.exit(0)
                get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
                version="J10001"
                if is_unix:
                        query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c \;" %(version)
                elif os_name == "WINDOWS":
                        query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
                output = check_output(query)
                if not output.strip(version + " |"):
                        sys.exit(0)

class SqlAnywhereConf(BaseDB):
	# Constructor
	def __init__(self, host, SQL_CONNECTOR_JAR, JAVA_BIN):
		self.host = host
		self.SQL_CONNECTOR_JAR = SQL_CONNECTOR_JAR
		self.JAVA_BIN = JAVA_BIN

	def get_jisql_cmd(self, user, password, db_name):
		path = RANGER_ADMIN_HOME
		self.JAVA_BIN = self.JAVA_BIN.strip("'")
		if is_unix:
			jisql_cmd = "%s -cp %s:%s/jisql/lib/* org.apache.util.sql.Jisql -user %s -password '%s' -driver sapsajdbc4 -cstring jdbc:sqlanywhere:database=%s;host=%s -noheader -trim"%(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, path,user, password,db_name,self.host)
		elif os_name == "WINDOWS":
			jisql_cmd = "%s -cp %s;%s\\jisql\\lib\\* org.apache.util.sql.Jisql -user %s -password '%s' -driver sapsajdbc4 -cstring jdbc:sqlanywhere:database=%s;host=%s -noheader -trim"%(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, path, user, password,db_name,self.host)
		return jisql_cmd

	def check_connection(self, db_name, db_user, db_password):
		log("[I] Checking connection", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if is_unix:
			query = get_cmd + " -c \; -query \"SELECT 1;\""
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"SELECT 1;\" -c ;"
		jisql_log(query, db_password)
		output = check_output(query)
		if output.strip('1 |'):
			log("[I] Connection success", "info")
			return True
		else:
			log("[E] Can't establish connection", "error")
			sys.exit(1)

	def import_db_file(self, db_name, db_user, db_password, file_name):
		isImported=False
		name = basename(file_name)
		if os.path.isfile(file_name):
			log("[I] Importing db schema to database " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if is_unix:
				query = get_cmd + " -input %s" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -input %s" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(query)
			if ret == 0:
				log("[I] "+name + " DB schema imported successfully","info")
				isImported=True
			else:
				log("[E] "+name + " DB Schema import failed!","error")
		else:
			log("[E] DB schema file " + name+ " not found","error")
			sys.exit(1)
		return isImported

	def check_table(self, db_name, db_user, db_password, TABLE_NAME):
		self.set_options(db_name, db_user, db_password, TABLE_NAME)
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if is_unix:
			query = get_cmd + " -c \; -query \"SELECT name FROM sysobjects where name = '%s' and type='U';\"" %(TABLE_NAME)
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"SELECT name FROM sysobjects where name = '%s' and type='U';\" -c ;" %(TABLE_NAME)
		jisql_log(query, db_password)
		output = check_output(query)
		if output.strip(TABLE_NAME + " |"):
			log("[I] Table '" + TABLE_NAME + "' already exists in  database '" + db_name + "'","info")
			return True
		else:
			log("[I] Table '" + TABLE_NAME + "' does not exist in database '" + db_name + "'","info")
			return False

	def grant_audit_db_user(self, audit_db_name, db_user, audit_db_user, db_password, audit_db_password,TABLE_NAME):
		log("[I] Granting permission to audit user '" + audit_db_user + "' on db '" + audit_db_name + "'","info")
		get_cmd = self.get_jisql_cmd(db_user, db_password,audit_db_name)
		if is_unix:
			query = get_cmd + " -c \; -query \"GRANT INSERT ON XA_ACCESS_AUDIT to %s;\"" %(audit_db_user)
			jisql_log(query, db_password)
			ret = subprocessCallWithRetry(shlex.split(query))
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"GRANT INSERT ON XA_ACCESS_AUDIT to %s;\" -c ;" %(audit_db_user)
			jisql_log(query, db_password)
			ret = subprocessCallWithRetry(query)
		if ret != 0 :
			sys.exit(1)
		else:
			log("[I] Permission granted to audit user " + audit_db_user , "info")

	def import_db_patches(self, db_name, db_user, db_password, file_name):
		name = basename(file_name)
		if os.path.isfile(file_name):
			version = name.split('-')[0]
			log("[I] Executing patch on " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if is_unix:
				query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
			jisql_log(query, db_password)
			output = check_output(query)
			if output.strip(version + " |"):
				log("[I] Patch "+ name  +" is already applied" ,"info")
			else:
				if is_unix:
					query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					while output.strip(version + " |"):
						log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
						time.sleep(retryPatchAfterSeconds)
						jisql_log(query, db_password)
						output = check_output(query)
				else:
					if is_unix:
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c \;" %(version,ranger_version,client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret == 0:
						log ("[I] Patch "+ name +" is being applied..","info")
					else:
						log("[E] Patch "+ name +" failed", "error")
					if is_unix:
						query = get_cmd + " -input %s" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -input %s" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret != 0:
						time.sleep(5)
						if is_unix:
							query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
						jisql_log(query, db_password)
						output = check_output(query)
						if output.strip(version + " |"):
							ret=0
							log("[I] Patch "+ name  +" has been applied by some other process!" ,"info")
					if ret == 0:
						log("[I] "+name + " patch applied","info")
						if is_unix:
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] Patch version updated", "info")
						else:
							log("[E] Updating patch version failed", "error")
							sys.exit(1)
					else:
						if is_unix:
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						log("[E] "+name + " import failed!","error")
						sys.exit(1)

	def import_auditdb_patches(self, xa_sqlObj,xa_db_host, audit_db_host, db_name, audit_db_name, db_user, audit_db_user, db_password, audit_db_password, file_name, TABLE_NAME):
		log("[I] --------- Checking XA_ACCESS_AUDIT table to apply audit db patches --------- ","info")
		output = self.check_table(audit_db_name, db_user, db_password, TABLE_NAME)
		if output == True:
			name = basename(file_name)
			if os.path.isfile(file_name):
				version = name.split('-')[0]
				log("[I] Executing patch on " + audit_db_name + " from file: " + name,"info")
				get_cmd1 = xa_sqlObj.get_jisql_cmd(db_user, db_password, db_name)
				if is_unix:
					query = get_cmd1 + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					log("[I] Patch "+ name  +" is already applied" ,"info")
				else:
					if is_unix:
						query = get_cmd1 + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
						while output.strip(version + " |"):
							log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
							time.sleep(retryPatchAfterSeconds)
							jisql_log(query, db_password)
							output = check_output(query)
					else:
						if is_unix:
							query = get_cmd1 + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c \;" %(version,ranger_version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd1 + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log ("[I] Patch "+ name +" is being applied..","info")
						else:
							log("[E] Patch "+ name +" failed", "error")
					get_cmd2 = self.get_jisql_cmd(db_user, db_password, audit_db_name)
					if is_unix:
						query = get_cmd2 + " -input %s" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd2 + " -input %s" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret == 0:
						if is_unix:
							log("[I] "+name + " patch applied","info")
							query = get_cmd1 + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd1 + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] Patch version updated", "info")
						else:
							if is_unix:
								query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							log("[E] Updating patch version failed", "error")
							sys.exit(1)
					else:
						if is_unix:
							query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						log("[E] "+name + " import failed!","error")
						sys.exit(1)
		else:
			log("[I] Table XA_ACCESS_AUDIT does not exists in " +audit_db_name,"error")
			sys.exit(1)

	def auditdb_operation(self, xa_db_host, audit_db_host, db_name, audit_db_name,db_user, audit_db_user, db_password, audit_db_password, file_name, TABLE_NAME):
		log("[I] --------- Check admin user connection --------- ","info")
		self.check_connection(audit_db_name, db_user, db_password)
		log("[I] --------- Check audit user connection --------- ","info")
		self.check_connection(audit_db_name, audit_db_user, audit_db_password)
		log("[I] --------- Check audit table exists --------- ","info")
		output = self.check_table(audit_db_name, db_user, db_password, TABLE_NAME)
		if output == False:
			self.import_db_file(audit_db_name ,db_user, db_password, file_name)
		self.grant_audit_db_user( audit_db_name ,db_user, audit_db_user, db_password,audit_db_password,TABLE_NAME)

	def execute_java_patches(self, xa_db_host, db_user, db_password, db_name):
		my_dict = {}
		version = ""
		className = ""
		app_home = os.path.join(RANGER_ADMIN_HOME,"ews","webapp")
		ranger_log = os.path.join(RANGER_ADMIN_HOME,"ews","logs")
		javaFiles = os.path.join(app_home,"WEB-INF","classes","org","apache","ranger","patch")

		if not os.path.exists(javaFiles):
			log("[I] No java patches to apply!","info")
		else:
			files = os.listdir(javaFiles)
			if files:
				for filename in files:
					f = re.match(java_patch_regex,filename)
					if f:
						className = re.match("(Patch.*?)_.*.class",filename)
						className = className.group(1)
						version = re.match("Patch.*?_(.*).class",filename)
						version = version.group(1)
						key3 = int(version.strip("J"))
						my_dict[key3] = filename

			keylist = my_dict.keys()
			keylist.sort()
			for key in keylist:
				#print "%s: %s" % (key, my_dict[key])
				version = str(key)
				className = my_dict[key]
				className = className.strip(".class")
				if version != "":
					get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
					if is_unix:
						query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\" -c \;" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
						log("[I] Java patch "+ className  +" is already applied" ,"info")
					else:
						if is_unix:
							query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\" -c \;" %(version)
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\" -c ;" %(version)
						jisql_log(query, db_password)
						output = check_output(query)
						if output.strip(version + " |"):
							while(output.strip(version + " |")):
								log("[I] Java patch "+ className  +" is being applied by some other process" ,"info")
								time.sleep(retryPatchAfterSeconds)
								jisql_log(query, db_password)
								output = check_output(query)
						else:
							if is_unix:
								query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('J%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c \;" %(version,ranger_version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('J%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0:
								log ("[I] java patch "+ className +" is being applied..","info")
							else:
								log("[E] java patch "+ className +" failed", "error")
								sys.exit(1)
							if is_unix:
								path = os.path.join("%s","WEB-INF","classes","conf:%s","WEB-INF","classes","lib","*:%s","WEB-INF",":%s","META-INF",":%s","WEB-INF","lib","*:%s","WEB-INF","classes",":%s","WEB-INF","classes","META-INF:%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							elif os_name == "WINDOWS":
								path = os.path.join("%s","WEB-INF","classes","conf;%s","WEB-INF","classes","lib","*;%s","WEB-INF",";%s","META-INF",";%s","WEB-INF","lib","*;%s","WEB-INF","classes",";%s","WEB-INF","classes","META-INF;%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							get_java_cmd = "%s -XX:MetaspaceSize=100m -XX:MaxMetaspaceSize=200m -Xmx%s -Xms1g -Dlogdir=%s -Dlog4j.configuration=db_patch.log4j.xml -cp %s org.apache.ranger.patch.%s"%(self.JAVA_BIN,max_memory,ranger_log,path,className)
							if is_unix:
								ret = subprocess.call(shlex.split(get_java_cmd))
							elif os_name == "WINDOWS":
								ret = subprocess.call(get_java_cmd)
							if ret == 0:
								if is_unix:
									query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='J%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='J%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								if ret == 0:
									log ("[I] java patch "+ className +" is applied..","info")
								else:
									if is_unix:
										query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
										jisql_log(query, db_password)
										ret = subprocess.call(shlex.split(query))
									elif os_name == "WINDOWS":
										query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
										jisql_log(query, db_password)
										ret = subprocess.call(query)
									log("[E] java patch "+ className +" failed", "error")
									sys.exit(1)
							else:
								if is_unix:
									query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								log("[E] applying java patch "+ className +" failed", "error")
								sys.exit(1)

	def set_options(self, db_name, db_user, db_password, TABLE_NAME):
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if is_unix:
			query = get_cmd + " -c \; -query \"set option public.reserved_keywords='LIMIT';\""
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"set option public.reserved_keywords='LIMIT';\" -c ;"
		jisql_log(query, db_password)
		ret = subprocessCallWithRetry(shlex.split(query))
		if is_unix:
			query = get_cmd + " -c \; -query \"set option public.max_statement_count=0;\""
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"set option public.max_statement_count=0;\" -c;"
		jisql_log(query, db_password)
		ret = subprocessCallWithRetry(shlex.split(query))
		if is_unix:
			query = get_cmd + " -c \; -query \"set option public.max_cursor_count=0;\""
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"set option public.max_cursor_count=0;\" -c;"
		jisql_log(query, db_password)
		ret = subprocessCallWithRetry(shlex.split(query))

	def change_admin_default_password(self, xa_db_host, db_user, db_password, db_name,userName,oldPassword,newPassword):
		version = ""
		className = "ChangePasswordUtil"
                version = dbversionBasedOnUserName(userName)
		app_home = os.path.join(RANGER_ADMIN_HOME,"ews","webapp")
		ranger_log = os.path.join(RANGER_ADMIN_HOME,"ews","logs")
		filePath = os.path.join(app_home,"WEB-INF","classes","org","apache","ranger","patch","cliutil","ChangePasswordUtil.class")
		if os.path.exists(filePath):
			if version != "":
				get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
				if is_unix:
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c \;" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
                                        log("[I] Ranger "+ userName +" default password has already been changed!!","info")
				else:
					if is_unix:
						query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c \;" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
                                                countTries = 0
						while(output.strip(version + " |")):
                                                        if countTries < 3:
                                                                log("[I] Ranger Password change utility is being executed by some other process" ,"info")
                                                                time.sleep(retryPatchAfterSeconds)
                                                                jisql_log(query, db_password)
                                                                output = check_output(query)
                                                                countTries += 1
                                                        else:
                                                                log("[E] Tried updating the password "+ str(countTries) + " times","error")
                                                                log("[E] If Ranger "+  userName +" user password is not being changed by some other process then manually delete the entry from ranger database table x_db_version_h table where version is " + version ,"error")
                                                                sys.exit(1)
					else:
						if is_unix:
							query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c \;" %(version,ranger_version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
                                                        log ("[I] Ranger "+ userName +" default password change request is in process..","info")
						else:
                                                        log("[E] Ranger "+ userName +" default password change request failed", "error")
                                                        sys.exit(1)
						if is_unix:
							path = os.path.join("%s","WEB-INF","classes","conf:%s","WEB-INF","classes","lib","*:%s","WEB-INF",":%s","META-INF",":%s","WEB-INF","lib","*:%s","WEB-INF","classes",":%s","WEB-INF","classes","META-INF:%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
						elif os_name == "WINDOWS":
							path = os.path.join("%s","WEB-INF","classes","conf;%s","WEB-INF","classes","lib","*;%s","WEB-INF",";%s","META-INF",";%s","WEB-INF","lib","*;%s","WEB-INF","classes",";%s","WEB-INF","classes","META-INF;%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
						get_java_cmd = "%s -XX:MetaspaceSize=100m -XX:MaxMetaspaceSize=200m -Xmx%s -Xms1g -Dlogdir=%s -Dlog4j.configuration=db_patch.log4j.xml -cp %s org.apache.ranger.patch.cliutil.%s %s %s %s -default"%(self.JAVA_BIN,max_memory,ranger_log,path,className,userName,oldPassword,newPassword)
						if is_unix:
							status = subprocess.call(shlex.split(get_java_cmd))
						elif os_name == "WINDOWS":
							status = subprocess.call(get_java_cmd)
						if status == 0 or status==2:
							if is_unix:
								query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0 and status == 0:
                                                                log ("[I] Ranger "+ userName +" default password change request processed successfully..","info")
							elif ret == 0 and status == 2:
                                                                log ("[I] Ranger "+ userName +" default password change request process skipped!","info")
							else:
								if is_unix:
									query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
                                                                log("[E] Ranger "+ userName +" default password change request failed", "error")
								sys.exit(1)
						else:
							if is_unix:
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
                                                        log("[E] Ranger "+ userName +" default password change request failed", "error")
							sys.exit(1)

	def create_version_history_table(self, db_name, db_user, db_password, file_name,table_name):
		name = basename(file_name)
		if os.path.isfile(file_name):
			isTableExist=self.check_table(db_name, db_user, db_password, table_name)
			if isTableExist==False:
				log("[I] Importing "+table_name+" table schema to database " + db_name + " from file: " + name,"info")
			while(isTableExist==False):
				get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
				if is_unix:
					query = get_cmd + " -input %s" %file_name
					jisql_log(query, db_password)
					ret = subprocess.call(shlex.split(query))
				elif os_name == "WINDOWS":
					query = get_cmd + " -input %s" %file_name
					jisql_log(query, db_password)
					ret = subprocess.call(query)
				if ret == 0:
					log("[I] "+name + " file imported successfully","info")
				else:
					log("[E] "+name + " file import failed!","error")
					time.sleep(30)
				isTableExist=self.check_table(db_name, db_user, db_password, table_name)
		else:
			log("[E] Table schema file " + name+ " not found","error")
			sys.exit(1)

	def import_core_db_schema(self, db_name, db_user, db_password, file_name,first_table,last_table):
		version = 'CORE_DB_SCHEMA'
		if os.path.isfile(file_name):
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if is_unix:
				query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
			jisql_log(query, db_password)
			output = check_output(query)
			if output.strip(version + " |"):
				log("[I] "+version+" is already imported" ,"info")
			else:
				if is_unix:
					query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					while(output.strip(version + " |")):
						log("[I] "+ version  +" is being imported by some other process" ,"info")
						time.sleep(retryPatchAfterSeconds)
						jisql_log(query, db_password)
						output = check_output(query)
				else:
					if is_unix:
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c \;" %(version,ranger_version,client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c ;" %(version,ranger_version,client_host)
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret != 0:
						log("[E] "+ version +" import failed", "error")
						sys.exit(1)
					isFirstTableExist = self.check_table(db_name, db_user, db_password, first_table)
					isLastTableExist = self.check_table(db_name, db_user, db_password, last_table)
					isSchemaCreated=False
					if isFirstTableExist == True and isLastTableExist == True :
						isSchemaCreated=True
					elif isFirstTableExist == False and isLastTableExist == False :
						isImported=self.import_db_file(db_name, db_user, db_password, file_name)
						if(isImported==False):
							log("[I] "+ version  +" might being imported by some other process" ,"info")
							time.sleep(retryPatchAfterSeconds)
						isLastTableExist=self.check_table(db_name, db_user, db_password, last_table)
						if(isLastTableExist==True):
							isSchemaCreated=True
					elif isFirstTableExist == False or isLastTableExist == False :
						while(isFirstTableExist == False or isLastTableExist==False):
							log("[I] "+ version  +" is being imported by some other process" ,"info")
							time.sleep(retryPatchAfterSeconds)
							isFirstTableExist = self.check_table(db_name, db_user, db_password, first_table)
							isLastTableExist=self.check_table(db_name, db_user, db_password, last_table)
							if(isFirstTableExist == True and isLastTableExist==True):
								isSchemaCreated=True
					if isSchemaCreated == True:
						if is_unix:
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] "+version +" import status has been updated", "info")
						else:
							if is_unix:
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							log("[E] Updating "+version +" import status failed", "error")
							sys.exit(1)
					else:
						if is_unix:
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c \;"  %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N' and updated_by='%s';\" -c ;" %(version,client_host)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						log("[E] "+version + " import failed!","error")
						sys.exit(1)

	def hasPendingPatches(self, db_name, db_user, db_password, version):
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if is_unix:
			query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and inst_by = '%s' and active = 'Y';\"" %(version,ranger_version)
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and inst_by = '%s' and active = 'Y';\" -c ;" %(version,ranger_version)
		jisql_log(query, db_password)
		output = check_output(query)
		if output.strip(version + " |"):
			return False
		else:
			return True

	def update_applied_patches_status(self,db_name, db_user, db_password,version):
		if self.hasPendingPatches(db_name, db_user, db_password,version) == True:
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if is_unix:
				query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','Y') ;\" -c \;" %(version,ranger_version,client_host)
				jisql_log(query, db_password)
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','Y') ;\" -c ;" %(version,ranger_version,client_host)
				jisql_log(query, db_password)
				ret = subprocess.call(query)
			if ret != 0:
				log("[E] "+ version +" status entry to x_db_version_h table failed", "error")
				sys.exit(1)
			else:
				log("[I] "+ version +" status entry to x_db_version_h table completed", "info")

        def is_new_install(self, xa_db_host, db_user, db_password, db_name):
                get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
                if is_unix:
                        query = get_cmd + " -c \; -query \"SELECT 1;\""
                elif os_name == "WINDOWS":
                        query = get_cmd + " -query \"SELECT 1;\" -c ;"
                output = check_output(query)
                if not output.strip('1 |'):
                        sys.exit(0)
                get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
                version="J10001"
                if is_unix:
                        query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c \;" %(version)
                elif os_name == "WINDOWS":
                        query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
                output = check_output(query)
                if not output.strip(version + " |"):
                        sys.exit(0)

def main(argv):
	populate_global_dict()

	FORMAT = '%(asctime)-15s %(message)s'
	logging.basicConfig(format=FORMAT, level=logging.DEBUG)

	if (not 'JAVA_HOME' in os.environ) or (os.environ['JAVA_HOME'] == ""):
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
	#get ranger version
	global ranger_version
	try:
		lib_home = os.path.join(RANGER_ADMIN_HOME,"ews","webapp","WEB-INF","lib","*")
		get_ranger_version_cmd="%s -cp %s org.apache.ranger.common.RangerVersionInfo"%(JAVA_BIN,lib_home)
		ranger_version = check_output(get_ranger_version_cmd).split("\n")[1]
	except Exception, error:
		ranger_version=''

	try:
		if ranger_version=="" or ranger_version=="ranger-admin - None":
			script_path = os.path.join(RANGER_ADMIN_HOME,"ews","ranger-admin-services.sh")
			ranger_version=check_output(script_path +" version").split("\n")[1]
	except Exception, error:
		ranger_version=''

	try:
		if ranger_version=="" or ranger_version=="ranger-admin - None":
			ranger_version=check_output("ranger-admin version").split("\n")[1]
	except Exception, error:
		ranger_version=''

	if ranger_version=="" or ranger_version is None:
		log("[E] Unable to find ranger version details, Exiting..", "error")
		sys.exit(1)

	XA_DB_FLAVOR=globalDict['DB_FLAVOR']
	AUDIT_DB_FLAVOR=globalDict['DB_FLAVOR']
	XA_DB_FLAVOR = XA_DB_FLAVOR.upper()
	AUDIT_DB_FLAVOR = AUDIT_DB_FLAVOR.upper()

	log("[I] DB FLAVOR :" + XA_DB_FLAVOR ,"info")
	xa_db_host = globalDict['db_host']
	audit_db_host = globalDict['db_host']

	mysql_dbversion_catalog = os.path.join('db','mysql','create_dbversion_catalog.sql')
	mysql_core_file = globalDict['mysql_core_file']
	mysql_audit_file = globalDict['mysql_audit_file']
	mysql_patches = os.path.join('db','mysql','patches')
	mysql_auditdb_patches = os.path.join('db','mysql','patches','audit')

	oracle_dbversion_catalog = os.path.join('db','oracle','create_dbversion_catalog.sql')
	oracle_core_file = globalDict['oracle_core_file'] 
	oracle_audit_file = globalDict['oracle_audit_file'] 
	oracle_patches = os.path.join('db','oracle','patches')
	oracle_auditdb_patches = os.path.join('db','oracle','patches','audit')

	postgres_dbversion_catalog = os.path.join('db','postgres','create_dbversion_catalog.sql')
	postgres_core_file = globalDict['postgres_core_file']
	postgres_audit_file = globalDict['postgres_audit_file']
	postgres_patches = os.path.join('db','postgres','patches')
	postgres_auditdb_patches = os.path.join('db','postgres','patches','audit')

	sqlserver_dbversion_catalog = os.path.join('db','sqlserver','create_dbversion_catalog.sql')
	sqlserver_core_file = globalDict['sqlserver_core_file']
	sqlserver_audit_file = globalDict['sqlserver_audit_file']
	sqlserver_patches = os.path.join('db','sqlserver','patches')
	sqlserver_auditdb_patches = os.path.join('db','sqlserver','patches','audit')

	sqlanywhere_dbversion_catalog = os.path.join('db','sqlanywhere','create_dbversion_catalog.sql')
	sqlanywhere_core_file = globalDict['sqlanywhere_core_file']
	sqlanywhere_audit_file = globalDict['sqlanywhere_audit_file']
	sqlanywhere_patches = os.path.join('db','sqlanywhere','patches')
	sqlanywhere_auditdb_patches = os.path.join('db','sqlanywhere','patches','audit')

	db_name = globalDict['db_name']
	db_user = globalDict['db_user']
	db_password = globalDict['db_password']

	x_db_version = 'x_db_version_h'
	xa_access_audit = 'xa_access_audit'

	audit_db_name=''
	audit_db_user=''
	audit_db_password=''
	audit_store = None
	if 'audit_store' in globalDict:
		audit_store = globalDict['audit_store']
		audit_store=audit_store.lower()

	if audit_store =='db':
		if 'audit_db_name' in globalDict:
			audit_db_name = globalDict['audit_db_name']
		if 'audit_db_user' in globalDict:
			audit_db_user = globalDict['audit_db_user']
		if 'audit_db_password' in globalDict:
			audit_db_password = globalDict['audit_db_password']

	db_ssl_enabled='false'
	db_ssl_required='false'
	db_ssl_verifyServerCertificate='false'
	db_ssl_auth_type='2-way'
	javax_net_ssl_keyStore=''
	javax_net_ssl_keyStorePassword=''
	javax_net_ssl_trustStore=''
	javax_net_ssl_trustStorePassword=''

	if XA_DB_FLAVOR == "MYSQL" or XA_DB_FLAVOR == "POSTGRES":
		if 'db_ssl_enabled' in globalDict:
			db_ssl_enabled=globalDict['db_ssl_enabled'].lower()
			if db_ssl_enabled == 'true':
				if 'db_ssl_required' in globalDict:
					db_ssl_required=globalDict['db_ssl_required'].lower()
				if 'db_ssl_verifyServerCertificate' in globalDict:
					db_ssl_verifyServerCertificate=globalDict['db_ssl_verifyServerCertificate'].lower()
				if 'db_ssl_auth_type' in globalDict:
					db_ssl_auth_type=globalDict['db_ssl_auth_type'].lower()
				if db_ssl_verifyServerCertificate == 'true':
					if 'javax_net_ssl_trustStore' in globalDict:
						javax_net_ssl_trustStore=globalDict['javax_net_ssl_trustStore']
					if 'javax_net_ssl_trustStorePassword' in globalDict:
						javax_net_ssl_trustStorePassword=globalDict['javax_net_ssl_trustStorePassword']
					if not os.path.exists(javax_net_ssl_trustStore):
						log("[E] Invalid file Name! Unable to find truststore file:"+javax_net_ssl_trustStore,"error")
						sys.exit(1)
					if javax_net_ssl_trustStorePassword is None or javax_net_ssl_trustStorePassword =="":
						log("[E] Invalid ssl truststore password!","error")
						sys.exit(1)
					if db_ssl_auth_type == '2-way':
						if 'javax_net_ssl_keyStore' in globalDict:
							javax_net_ssl_keyStore=globalDict['javax_net_ssl_keyStore']
						if 'javax_net_ssl_keyStorePassword' in globalDict:
							javax_net_ssl_keyStorePassword=globalDict['javax_net_ssl_keyStorePassword']
						if not os.path.exists(javax_net_ssl_keyStore):
							log("[E] Invalid file Name! Unable to find keystore file:"+javax_net_ssl_keyStore,"error")
							sys.exit(1)
						if javax_net_ssl_keyStorePassword is None or javax_net_ssl_keyStorePassword =="":
							log("[E] Invalid ssl keystore password!","error")
							sys.exit(1)

	if XA_DB_FLAVOR == "MYSQL":
		MYSQL_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
		xa_sqlObj = MysqlConf(xa_db_host, MYSQL_CONNECTOR_JAR, JAVA_BIN,db_ssl_enabled,db_ssl_required,db_ssl_verifyServerCertificate,javax_net_ssl_keyStore,javax_net_ssl_keyStorePassword,javax_net_ssl_trustStore,javax_net_ssl_trustStorePassword,db_ssl_auth_type)
		xa_db_version_file = os.path.join(RANGER_ADMIN_HOME , mysql_dbversion_catalog)
		xa_db_core_file = os.path.join(RANGER_ADMIN_HOME , mysql_core_file)
		xa_patch_file = os.path.join(RANGER_ADMIN_HOME ,mysql_patches)
		audit_patch_file = os.path.join(RANGER_ADMIN_HOME ,mysql_auditdb_patches)
		first_table='x_asset'
		last_table='xa_access_audit'
		
	elif XA_DB_FLAVOR == "ORACLE":
		ORACLE_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
		xa_sqlObj = OracleConf(xa_db_host, ORACLE_CONNECTOR_JAR, JAVA_BIN)
		xa_db_version_file = os.path.join(RANGER_ADMIN_HOME ,oracle_dbversion_catalog)
		xa_db_core_file = os.path.join(RANGER_ADMIN_HOME ,oracle_core_file)
		xa_patch_file = os.path.join(RANGER_ADMIN_HOME ,oracle_patches)
		audit_patch_file = os.path.join(RANGER_ADMIN_HOME ,oracle_auditdb_patches)
		first_table='X_PORTAL_USER'
		last_table='X_AUDIT_MAP'

	elif XA_DB_FLAVOR == "POSTGRES":
		db_user=db_user.lower()
		db_name=db_name.lower()
		POSTGRES_CONNECTOR_JAR = globalDict['SQL_CONNECTOR_JAR']
		xa_sqlObj = PostgresConf(xa_db_host, POSTGRES_CONNECTOR_JAR, JAVA_BIN,db_ssl_enabled,db_ssl_required,db_ssl_verifyServerCertificate,javax_net_ssl_keyStore,javax_net_ssl_keyStorePassword,javax_net_ssl_trustStore,javax_net_ssl_trustStorePassword,db_ssl_auth_type)
		xa_db_version_file = os.path.join(RANGER_ADMIN_HOME , postgres_dbversion_catalog)
		xa_db_core_file = os.path.join(RANGER_ADMIN_HOME , postgres_core_file)
		xa_patch_file = os.path.join(RANGER_ADMIN_HOME , postgres_patches)
		audit_patch_file = os.path.join(RANGER_ADMIN_HOME ,postgres_auditdb_patches)
		first_table='x_portal_user'
		last_table='x_group_module_perm'

	elif XA_DB_FLAVOR == "MSSQL":
		SQLSERVER_CONNECTOR_JAR = globalDict['SQL_CONNECTOR_JAR']
		xa_sqlObj = SqlServerConf(xa_db_host, SQLSERVER_CONNECTOR_JAR, JAVA_BIN)
		xa_db_version_file = os.path.join(RANGER_ADMIN_HOME ,sqlserver_dbversion_catalog)
		xa_db_core_file = os.path.join(RANGER_ADMIN_HOME , sqlserver_core_file)
		xa_patch_file = os.path.join(RANGER_ADMIN_HOME , sqlserver_patches)
		audit_patch_file = os.path.join(RANGER_ADMIN_HOME ,sqlserver_auditdb_patches)
		first_table='x_portal_user'
		last_table='x_group_module_perm'

	elif XA_DB_FLAVOR == "SQLA":
		if not os_name == "WINDOWS" :
			if os.environ['LD_LIBRARY_PATH'] == "":
				log("[E] ---------- LD_LIBRARY_PATH environment property not defined, aborting installation. ----------", "error")
				sys.exit(1)
		SQLANYWHERE_CONNECTOR_JAR = globalDict['SQL_CONNECTOR_JAR']
		xa_sqlObj = SqlAnywhereConf(xa_db_host, SQLANYWHERE_CONNECTOR_JAR, JAVA_BIN)
		xa_db_version_file = os.path.join(RANGER_ADMIN_HOME ,sqlanywhere_dbversion_catalog)
		xa_db_core_file = os.path.join(RANGER_ADMIN_HOME , sqlanywhere_core_file)
		xa_patch_file = os.path.join(RANGER_ADMIN_HOME , sqlanywhere_patches)
		audit_patch_file = os.path.join(RANGER_ADMIN_HOME ,sqlanywhere_auditdb_patches)
		first_table='x_portal_user'
		last_table='x_group_module_perm'

	else:
		log("[E] --------- NO SUCH SUPPORTED DB FLAVOUR!! ---------", "error")
		sys.exit(1)

	if AUDIT_DB_FLAVOR == "MYSQL":
		MYSQL_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
		audit_sqlObj = MysqlConf(audit_db_host,MYSQL_CONNECTOR_JAR,JAVA_BIN,db_ssl_enabled,db_ssl_required,db_ssl_verifyServerCertificate,javax_net_ssl_keyStore,javax_net_ssl_keyStorePassword,javax_net_ssl_trustStore,javax_net_ssl_trustStorePassword,db_ssl_auth_type)
		audit_db_file = os.path.join(RANGER_ADMIN_HOME ,mysql_audit_file)

	elif AUDIT_DB_FLAVOR == "ORACLE":
		ORACLE_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
		audit_sqlObj = OracleConf(audit_db_host, ORACLE_CONNECTOR_JAR, JAVA_BIN)
		audit_db_file = os.path.join(RANGER_ADMIN_HOME , oracle_audit_file)

	elif AUDIT_DB_FLAVOR == "POSTGRES":
		audit_db_user=audit_db_user.lower()
		audit_db_name=audit_db_name.lower()
		POSTGRES_CONNECTOR_JAR = globalDict['SQL_CONNECTOR_JAR']
		audit_sqlObj = PostgresConf(audit_db_host,POSTGRES_CONNECTOR_JAR,JAVA_BIN,db_ssl_enabled,db_ssl_required,db_ssl_verifyServerCertificate,javax_net_ssl_keyStore,javax_net_ssl_keyStorePassword,javax_net_ssl_trustStore,javax_net_ssl_trustStorePassword,db_ssl_auth_type)
		audit_db_file = os.path.join(RANGER_ADMIN_HOME , postgres_audit_file)

	elif AUDIT_DB_FLAVOR == "MSSQL":
		SQLSERVER_CONNECTOR_JAR = globalDict['SQL_CONNECTOR_JAR']
		audit_sqlObj = SqlServerConf(audit_db_host, SQLSERVER_CONNECTOR_JAR, JAVA_BIN)
		audit_db_file = os.path.join(RANGER_ADMIN_HOME , sqlserver_audit_file)

	elif AUDIT_DB_FLAVOR == "SQLA":
		SQLANYWHERE_CONNECTOR_JAR = globalDict['SQL_CONNECTOR_JAR']
		audit_sqlObj = SqlAnywhereConf(audit_db_host, SQLANYWHERE_CONNECTOR_JAR, JAVA_BIN)
		audit_db_file = os.path.join(RANGER_ADMIN_HOME , sqlanywhere_audit_file)
	else:
		log("[E] --------- NO SUCH SUPPORTED DB FLAVOUR!! ---------", "error")
		sys.exit(1)

	log("[I] --------- Verifying Ranger DB connection ---------","info")
	xa_sqlObj.check_connection(db_name, db_user, db_password)

	if len(argv)==1:
		log("[I] --------- Verifying version history table ---------","info")
		output = xa_sqlObj.check_table(db_name, db_user, db_password, x_db_version)
		if output == False:
			xa_sqlObj.create_version_history_table(db_name, db_user, db_password, xa_db_version_file,x_db_version)

		log("[I] --------- Importing Ranger Core DB Schema ---------","info")
		xa_sqlObj.import_core_db_schema(db_name, db_user, db_password, xa_db_core_file,first_table,last_table)
		if XA_DB_FLAVOR == "ORACLE":
			if xa_sqlObj.check_table(db_name, db_user, db_password, xa_access_audit):
				if audit_db_user != "" and db_user != audit_db_user:
					xa_sqlObj.create_synonym(db_name, db_user, db_password,audit_db_user)

		applyDBPatches=xa_sqlObj.hasPendingPatches(db_name, db_user, db_password, "DB_PATCHES")
		if applyDBPatches == True:
			log("[I] --------- Applying Ranger DB patches ---------","info")
			xa_sqlObj.apply_patches(db_name, db_user, db_password, xa_patch_file)
		else:
			log("[I] DB_PATCHES have already been applied","info")

		if audit_store == "db" and audit_db_password!='':
			log("[I] --------- Starting Audit Operation ---------","info")
			audit_sqlObj.auditdb_operation(xa_db_host, audit_db_host, db_name, audit_db_name, db_user, audit_db_user, db_password, audit_db_password, audit_db_file, xa_access_audit)
			log("[I] --------- Applying Audit DB patches ---------","info")
			audit_sqlObj.apply_auditdb_patches(xa_sqlObj,xa_db_host, audit_db_host, db_name, audit_db_name, db_user, audit_db_user, db_password, audit_db_password, audit_patch_file, xa_access_audit)

	if len(argv)>1:
		for i in range(len(argv)):
			if str(argv[i]) == "-javapatch":
				applyJavaPatches=xa_sqlObj.hasPendingPatches(db_name, db_user, db_password, "JAVA_PATCHES")
				if applyJavaPatches == True:
					log("[I] ----------------- Applying java patches ------------", "info")
					xa_sqlObj.execute_java_patches(xa_db_host, db_user, db_password, db_name)
					xa_sqlObj.update_applied_patches_status(db_name,db_user, db_password,"JAVA_PATCHES")
				else:
					log("[I] JAVA_PATCHES have already been applied","info")

                        if str(argv[i]) == "-checkupgrade":
                                xa_sqlObj.is_new_install(xa_db_host, db_user, db_password, db_name)

			if str(argv[i]) == "-changepassword":
				if len(argv)==5:
					userName=argv[2]
					oldPassword=argv[3]
					newPassword=argv[4]
					if oldPassword==newPassword:
						log("[E] Old Password and New Password argument are same. Exiting!!", "error")
						sys.exit(1)
					if userName != "" and oldPassword != "" and newPassword != "":
						password_validation(newPassword)
						xa_sqlObj.change_admin_default_password(xa_db_host, db_user, db_password, db_name,userName,oldPassword,newPassword)
				else:
					log("[E] Invalid argument list.", "error")
					log("[I] Usage : python db_setup.py -changepassword <loginID> <currentPassword> <newPassword>","info")
					sys.exit(1)

main(sys.argv)
