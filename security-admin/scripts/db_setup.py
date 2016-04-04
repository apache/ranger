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
import time
import datetime
from time import gmtime, strftime
globalDict = {}

os_name = platform.system()
os_name = os_name.upper()

jisql_debug=True

if os_name == "LINUX":
	RANGER_ADMIN_HOME = os.getenv("RANGER_ADMIN_HOME")
	if RANGER_ADMIN_HOME is None:
		RANGER_ADMIN_HOME = os.getcwd()
elif os_name == "WINDOWS":
	RANGER_ADMIN_HOME = os.getenv("RANGER_ADMIN_HOME")

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
		read_config_file = open(os.path.join(RANGER_ADMIN_HOME,'install.properties'))
	elif os_name == "WINDOWS":
		read_config_file = open(os.path.join(RANGER_ADMIN_HOME,'bin','install_config.properties'))
		library_path = os.path.join(RANGER_ADMIN_HOME,"cred","lib","*")
	for each_line in read_config_file.read().split('\n') :
		if len(each_line) == 0 : continue
		if re.search('=', each_line):
			key , value = each_line.strip().split("=",1)
			key = key.strip()
			if 'PASSWORD' in key:
				jceks_file_path = os.path.join(RANGER_ADMIN_HOME, 'jceks','ranger_db.jceks')
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

class BaseDB(object):

	def check_connection(self, db_name, db_user, db_password):
		log("[I] ---------- Verifying DB connection ----------", "info")

	def check_table(self, db_name, db_user, db_password, TABLE_NAME):
		log("[I] ---------- Verifying table ----------", "info")

	def import_db_file(self, db_name, db_user, db_password, file_name):
		log("[I] ---------- Importing db schema ----------", "info")

	def upgrade_db(self, db_name, db_user, db_password, DBVERSION_CATALOG_CREATION):
		self.import_db_file(db_name, db_user, db_password, DBVERSION_CATALOG_CREATION)
		log("[I] Baseline DB upgraded successfully", "info")

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

class MysqlConf(BaseDB):
	# Constructor
	def __init__(self, host,SQL_CONNECTOR_JAR,JAVA_BIN):
		self.host = host
		self.SQL_CONNECTOR_JAR = SQL_CONNECTOR_JAR
		self.JAVA_BIN = JAVA_BIN

	def get_jisql_cmd(self, user, password ,db_name):
		path = RANGER_ADMIN_HOME
		self.JAVA_BIN = self.JAVA_BIN.strip("'")
		if os_name == "LINUX":
			jisql_cmd = "%s -cp %s:%s/jisql/lib/* org.apache.util.sql.Jisql -driver mysqlconj -cstring jdbc:mysql://%s/%s -u '%s' -p '%s' -noheader -trim -c \;" %(self.JAVA_BIN,self.SQL_CONNECTOR_JAR,path,self.host,db_name,user,password)
		elif os_name == "WINDOWS":
			jisql_cmd = "%s -cp %s;%s\jisql\\lib\\* org.apache.util.sql.Jisql -driver mysqlconj -cstring jdbc:mysql://%s/%s -u \"%s\" -p \"%s\" -noheader -trim" %(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, path, self.host, db_name, user, password)
		return jisql_cmd

	def check_connection(self, db_name, db_user, db_password):
		log("[I] Checking connection..", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if os_name == "LINUX":
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
			if os_name == "LINUX":
				query = get_cmd + " -query \"GRANT INSERT ON %s.%s TO '%s'@'%s';\"" %(audit_db_name,TABLE_NAME,audit_db_user,host)
				jisql_log(query, db_password)
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"GRANT INSERT ON %s.%s TO '%s'@'%s';\" -c ;" %(audit_db_name,TABLE_NAME,audit_db_user,host)
				jisql_log(query, db_password)
				ret = subprocess.call(query)
			if ret == 0:
				log("[I] Granting privileges to '" + audit_db_user+"' done on '"+ audit_db_name+"'", "info")
			else:
				log("[E] Granting privileges to '" +audit_db_user+"' failed on '" + audit_db_name+"'", "error")
				sys.exit(1)

	def import_db_file(self, db_name, db_user, db_password, file_name):
		name = basename(file_name)
		if os.path.isfile(file_name):
			log("[I] Importing db schema to database " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if os_name == "LINUX":
				query = get_cmd + " -input %s" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -input %s -c ;" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(query)
			if ret == 0:
				log("[I] "+name + " DB schema imported successfully","info")
			else:
				log("[E] "+name + " DB schema import failed!","error")
				sys.exit(1)
		else:
			log("[E] DB schema file " + name+ " not found","error")
			sys.exit(1)

	def import_db_patches(self, db_name, db_user, db_password, file_name):
		name = basename(file_name)
		if os.path.isfile(file_name):
			version = name.split('-')[0]
			log("[I] Executing patch on  " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if os_name == "LINUX":
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
			jisql_log(query, db_password)
			output = check_output(query)
			if output.strip(version + " |"):
				log("[I] Patch "+ name  +" is already applied" ,"info")
			else:
				if os_name == "LINUX":
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					while(output.strip(version + " |")):
						log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
						time.sleep(300)
						jisql_log(query, db_password)
						output = check_output(query)
				else:
					if os_name == "LINUX":
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', now(), user(), now(), user(),'N') ;\"" %(version)
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', now(), user(), now(), user(),'N') ;\" -c ;" %(version)
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret == 0:
						log ("[I] Patch "+ name +" is being applied..","info")
					else:
						log("[E] Patch "+ name +" failed", "error")
						sys.exit(1)
					if os_name == "LINUX":
						query = get_cmd + " -input %s" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -input %s -c ;" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret == 0:
						log("[I] "+name + " patch applied","info")
						if os_name == "LINUX":
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\"" %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\" -c ;" %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] Patch version updated", "info")
						else:
							log("[E] Updating patch version failed", "error")
							sys.exit(1)
					else:
						if os_name == "LINUX":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N';\"" %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N';\" -c ;" %(version)
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
				if os_name == "LINUX":
					query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					log("[I] Patch "+ name  +" is already applied" ,"info")
				else:
					if os_name == "LINUX":
						query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
						while(output.strip(version + " |")):
							log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
							time.sleep(300)
							jisql_log(query, db_password)
							output = check_output(query)
					else:
						if os_name == "LINUX":
							query = get_cmd1 + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', now(), user(), now(), user(),'N') ;\"" %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd1 + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', now(), user(), now(), user(),'N') ;\" -c ;" %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log ("[I] Patch "+ name +" is being applied..","info")
						else:
							log("[E] Patch "+ name +" failed", "error")
							sys.exit(1)
						get_cmd2 = self.get_jisql_cmd(db_user, db_password, audit_db_name)
						if os_name == "LINUX":
							query = get_cmd2 + " -input %s" %file_name
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd2 + " -input %s -c ;" %file_name
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] "+name + " patch applied","info")
							if os_name == "LINUX":
								query = get_cmd1 + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\"" %(version)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd1 + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\" -c ;" %(version)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0:
								log("[I] Patch version updated", "info")
							else:
								log("[E] Updating patch version failed", "error")
								sys.exit(1)
						else:
							if os_name == "LINUX":
								query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N';\"" %(version)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N';\" -c ;" %(version)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							log("[E] "+name + " import failed!","error")
							sys.exit(1)
		else:
			log("[I] Table XA_ACCESS_AUDIT does not exists in " +audit_db_name,"error")
			sys.exit(1)

	def check_table(self, db_name, db_user, db_password, TABLE_NAME):
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if os_name == "LINUX":
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
					f = re.match("^Patch.*?.class$",filename)
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
					if os_name == "LINUX":
						query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\"" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
						log("[I] Java patch "+ className  +" is already applied" ,"info")
					else:
						if os_name == "LINUX":
							query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\"" %(version)
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\" -c ;" %(version)
						jisql_log(query, db_password)
						output = check_output(query)
						if output.strip(version + " |"):
							while(output.strip(version + " |")):
								log("[I] Java patch "+ className  +" is being applied by some other process" ,"info")
								time.sleep(300)
								jisql_log(query, db_password)
								output = check_output(query)
						else:
							if os_name == "LINUX":
								query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('J%s', now(), user(), now(), user(),'N') ;\"" %(version)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('J%s', now(), user(), now(), user(),'N') ;\" -c ;" %(version)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0:
								log ("[I] java patch "+ className +" is being applied..","info")
							else:
								log("[E] java patch "+ className +" failed", "error")
								sys.exit(1)
							if os_name == "LINUX":
								path = os.path.join("%s","WEB-INF","classes","conf:%s","WEB-INF","classes","lib","*:%s","WEB-INF",":%s","META-INF",":%s","WEB-INF","lib","*:%s","WEB-INF","classes",":%s","WEB-INF","classes","META-INF:%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							elif os_name == "WINDOWS":
								path = os.path.join("%s","WEB-INF","classes","conf;%s","WEB-INF","classes","lib","*;%s","WEB-INF",";%s","META-INF",";%s","WEB-INF","lib","*;%s","WEB-INF","classes",";%s","WEB-INF","classes","META-INF;%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							get_java_cmd = "%s -Dlogdir=%s -Dlog4j.configuration=db_patch.log4j.xml -cp %s org.apache.ranger.patch.%s"%(self.JAVA_BIN,ranger_log,path,className)
							if os_name == "LINUX":
								ret = subprocess.call(shlex.split(get_java_cmd))
							elif os_name == "WINDOWS":
								ret = subprocess.call(get_java_cmd)
							if ret == 0:
								if os_name == "LINUX":
									query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='J%s' and active='N';\"" %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='J%s' and active='N';\" -c ;" %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								if ret == 0:
									log ("[I] java patch "+ className +" is applied..","info")
								else:
									if os_name == "LINUX":
										query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N';\"" %(version)
										jisql_log(query, db_password)
										ret = subprocess.call(shlex.split(query))
									elif os_name == "WINDOWS":
										query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N';\" -c ;" %(version)
										jisql_log(query, db_password)
										ret = subprocess.call(query)
									log("[E] java patch "+ className +" failed", "error")
									sys.exit(1)
							else:
								if os_name == "LINUX":
									query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N';\"" %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N';\" -c ;" %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								log("[E] applying java patch "+ className +" failed", "error")
								sys.exit(1)


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

		if os_name == "LINUX":
			jisql_cmd = "%s -cp %s:%s/jisql/lib/* org.apache.util.sql.Jisql -driver oraclethin -cstring %s -u '%s' -p '%s' -noheader -trim" %(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, path, cstring, user, password)
		elif os_name == "WINDOWS":
			jisql_cmd = "%s -cp %s;%s\jisql\\lib\\* org.apache.util.sql.Jisql -driver oraclethin -cstring %s -u \"%s\" -p \"%s\" -noheader -trim" %(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, path, cstring, user, password)
		return jisql_cmd


	def check_connection(self, db_name, db_user, db_password):
		log("[I] Checking connection", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password)
		if os_name == "LINUX":
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
		if os_name == "LINUX":
			query = get_cmd + " -c \; -query 'GRANT SELECT ON %s.XA_ACCESS_AUDIT_SEQ TO %s;'" % (db_user,audit_db_user)
			jisql_log(query, db_password)
			ret = subprocess.call(shlex.split(query))
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"GRANT SELECT ON %s.XA_ACCESS_AUDIT_SEQ TO %s;\" -c ;" % (db_user,audit_db_user)
			jisql_log(query, db_password)
			ret = subprocess.call(query)
		if ret != 0:
			sys.exit(1)
		if os_name == "LINUX":
			query = get_cmd + " -c \; -query 'GRANT INSERT ON %s.XA_ACCESS_AUDIT TO %s;'" % (db_user,audit_db_user)
			jisql_log(query, db_password)
			ret = subprocess.call(shlex.split(query))
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"GRANT INSERT ON %s.XA_ACCESS_AUDIT TO %s;\" -c ;" % (db_user,audit_db_user)
			jisql_log(query, db_password)
			ret = subprocess.call(query)
		if ret != 0:
			sys.exit(1)

	def import_db_file(self, db_name, db_user, db_password, file_name):
		name = basename(file_name)
		if os.path.isfile(file_name):
			log("[I] Importing script " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password)
			if os_name == "LINUX":
				query = get_cmd + " -input %s -c \;" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -input %s -c ;" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(query)
			if ret == 0:
				log("[I] "+name + " imported successfully","info")
			else:
				log("[E] "+name + " import failed!","error")
				sys.exit(1)

	def create_synonym(self,db_name, db_user, db_password,audit_db_user):
		log("[I] ----------------- Creating Synonym ------------", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password)
		if os_name == "LINUX":
			query = get_cmd + " -c \; -query 'CREATE OR REPLACE SYNONYM %s.XA_ACCESS_AUDIT FOR %s.XA_ACCESS_AUDIT;'" % (audit_db_user,db_user)
			jisql_log(query, db_password)
			ret = subprocess.call(shlex.split(query))
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"CREATE OR REPLACE SYNONYM %s.XA_ACCESS_AUDIT FOR %s.XA_ACCESS_AUDIT;\" -c ;" % (audit_db_user,db_user)
			jisql_log(query, db_password)
			ret = subprocess.call(query)
		if ret != 0:
			sys.exit(1)
		if os_name == "LINUX":
			query = get_cmd + " -c \; -query 'CREATE OR REPLACE SYNONYM %s.XA_ACCESS_AUDIT_SEQ FOR %s.XA_ACCESS_AUDIT_SEQ;'" % (audit_db_user,db_user)
			jisql_log(query, db_password)
			ret = subprocess.call(shlex.split(query))
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"CREATE OR REPLACE SYNONYM %s.XA_ACCESS_AUDIT_SEQ FOR %s.XA_ACCESS_AUDIT_SEQ;\" -c ;" % (audit_db_user,db_user)
			jisql_log(query, db_password)
			ret = subprocess.call(query)
		if ret != 0:
			sys.exit(1)

	def import_db_patches(self, db_name, db_user, db_password, file_name):
		if os.path.isfile(file_name):
			name = basename(file_name)
			version = name.split('-')[0]
			log("[I] Executing patch on " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password)
			if os_name == "LINUX":
				query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
			jisql_log(query, db_password)
			output = check_output(query)
			if output.strip(version +" |"):
				log("[I] Patch "+ name  +" is already applied" ,"info")
			else:
				if os_name == "LINUX":
					query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version +" |"):
					while(output.strip(version + " |")):
						log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
						time.sleep(300)
						jisql_log(query, db_password)
						output = check_output(query)
				else:
					if os_name == "LINUX":
						query = get_cmd + " -c \; -query \"insert into x_db_version_h (id,version, inst_at, inst_by, updated_at, updated_by,active) values ( X_DB_VERSION_H_SEQ.nextval,'%s', sysdate, '%s', sysdate, '%s','N');\"" %(version, db_user, db_user)
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"insert into x_db_version_h (id,version, inst_at, inst_by, updated_at, updated_by,active) values ( X_DB_VERSION_H_SEQ.nextval,'%s', sysdate, '%s', sysdate, '%s','N');\" -c ;" %(version, db_user, db_user)
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret == 0:
						log ("[I] Patch "+ name +" is being applied..","info")
					else:
						log("[E] Patch "+ name +" failed", "error")
					if os_name == "LINUX":
						query = get_cmd + " -input %s -c /" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -input %s -c /" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret == 0:
						log("[I] "+name + " patch applied","info")
						if os_name == "LINUX":
							query = get_cmd + " -c \; -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\"" %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\" -c ;" %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] Patch version updated", "info")
						else:
							log("[E] Updating patch version failed", "error")
							sys.exit(1)
					else:
						if os_name == "LINUX":
							query = get_cmd + " -c \; -query \"delete from x_db_version_h where version='%s' and active='N';\"" %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N';\" -c ;" %(version)
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
				if os_name == "LINUX":
					query = get_cmd1 + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version +" |"):
					log("[I] Patch "+ name  +" is already applied" ,"info")
				else:
					if os_name == "LINUX":
						query = get_cmd1 + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version +" |"):
						while(output.strip(version + " |")):
							log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
							time.sleep(300)
							jisql_log(query, db_password)
							output = check_output(query)
					else:
						if os_name == "LINUX":
							query = get_cmd1 + " -c \; -query \"insert into x_db_version_h (id,version, inst_at, inst_by, updated_at, updated_by,active) values ( X_DB_VERSION_H_SEQ.nextval,'%s', sysdate, '%s', sysdate, '%s','N');\"" %(version, db_user, db_user)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd1 + " -query \"insert into x_db_version_h (id,version, inst_at, inst_by, updated_at, updated_by,active) values ( X_DB_VERSION_H_SEQ.nextval,'%s', sysdate, '%s', sysdate, '%s','N');\" -c ;" %(version, db_user, db_user)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log ("[I] Patch "+ name +" is being applied..","info")
						else:
							log("[E] Patch "+ name +" failed", "error")
						get_cmd2 = self.get_jisql_cmd(db_user, db_password)
						if os_name == "LINUX":
							query = get_cmd2 + " -input %s -c /" %file_name
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd2 + " -input %s -c /" %file_name
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] "+name + " patch applied","info")
							if os_name == "LINUX":
								query = get_cmd1 + " -c \; -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\"" %(version)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd1 + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\" -c ;" %(version)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0:
								log("[I] Patch version updated", "info")
							else:
								log("[E] Updating patch version failed", "error")
								sys.exit(1)
						else:
							if os_name == "LINUX":
								query = get_cmd1 + " -c \; -query \"delete from x_db_version_h where version='%s' and active='N';\"" %(version)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N';\" -c ;" %(version)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							log("[E] "+name + " Import failed!","error")
							sys.exit(1)
			else:
				log("[I] Patch file not found","error")
				sys.exit(1)

	def check_table(self, db_name, db_user, db_password, TABLE_NAME):
		get_cmd = self.get_jisql_cmd(db_user ,db_password)
		if os_name == "LINUX":
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
			if os_name == "LINUX":
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
					f = re.match("^Patch.*?.class$",filename)
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
					if os_name == "LINUX":
						query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\"" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
						log("[I] Java patch "+ className  +" is already applied" ,"info")
					else:
						if os_name == "LINUX":
							query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\"" %(version)
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\" -c ;" %(version)
						jisql_log(query, db_password)
						output = check_output(query)
						if output.strip(version + " |"):
							while(output.strip(version + " |")):
								log("[I] Java patch "+ className  +" is being applied by some other process" ,"info")
								time.sleep(300)
								jisql_log(query, db_password)
								output = check_output(query)
						else:
							if os_name == "LINUX":
								query = get_cmd + " -c \; -query \"insert into x_db_version_h (id,version, inst_at, inst_by, updated_at, updated_by,active) values ( X_DB_VERSION_H_SEQ.nextval,'J%s', sysdate, '%s', sysdate, '%s','N');\"" %(version, db_user, db_user)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"insert into x_db_version_h (id,version, inst_at, inst_by, updated_at, updated_by,active) values ( X_DB_VERSION_H_SEQ.nextval,'J%s', sysdate, '%s', sysdate, '%s','N');\" -c ;" %(version, db_user, db_user)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0:
								log ("[I] java patch "+ className +" is being applied..","info")
							else:
								log("[E] java patch "+ className +" failed", "error")
								sys.exit(1)
							if os_name == "LINUX":
								path = os.path.join("%s","WEB-INF","classes","conf:%s","WEB-INF","classes","lib","*:%s","WEB-INF",":%s","META-INF",":%s","WEB-INF","lib","*:%s","WEB-INF","classes",":%s","WEB-INF","classes","META-INF:%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							elif os_name == "WINDOWS":
								path = os.path.join("%s","WEB-INF","classes","conf;%s","WEB-INF","classes","lib","*;%s","WEB-INF",";%s","META-INF",";%s","WEB-INF","lib","*;%s","WEB-INF","classes",";%s","WEB-INF","classes","META-INF;%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							get_java_cmd = "%s -Dlogdir=%s -Dlog4j.configuration=db_patch.log4j.xml -cp %s org.apache.ranger.patch.%s"%(self.JAVA_BIN,ranger_log,path,className)
							if os_name == "LINUX":
								ret = subprocess.call(shlex.split(get_java_cmd))
							elif os_name == "WINDOWS":
								ret = subprocess.call(get_java_cmd)
							if ret == 0:
								if os_name == "LINUX":
									query = get_cmd + " -c \; -query \"update x_db_version_h set active='Y' where version='J%s' and active='N';\"" %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='J%s' and active='N';\" -c ;" %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								if ret == 0:
									log ("[I] java patch "+ className +" is applied..","info")
								else:
									if os_name == "LINUX":
										query = get_cmd + " -c \; -query \"delete from x_db_version_h where version='J%s' and active='N';\"" %(version)
										jisql_log(query, db_password)
										ret = subprocess.call(shlex.split(query))
									elif os_name == "WINDOWS":
										query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N';\" -c ;" %(version)
										jisql_log(query, db_password)
										ret = subprocess.call(query)
									log("[E] java patch "+ className +" failed", "error")
									sys.exit(1)
							else:
								if os_name == "LINUX":
									query = get_cmd + " -c \; -query \"delete from x_db_version_h where version='J%s' and active='N';\"" %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N';\" -c ;" %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								log("[E] applying java patch "+ className +" failed", "error")
								sys.exit(1)


class PostgresConf(BaseDB):
	# Constructor
	def __init__(self, host, SQL_CONNECTOR_JAR, JAVA_BIN):
		self.host = host
		self.SQL_CONNECTOR_JAR = SQL_CONNECTOR_JAR
		self.JAVA_BIN = JAVA_BIN

	def get_jisql_cmd(self, user, password, db_name):
		#TODO: User array for forming command
		path = RANGER_ADMIN_HOME
		self.JAVA_BIN = self.JAVA_BIN.strip("'")
		if os_name == "LINUX":
			jisql_cmd = "%s -cp %s:%s/jisql/lib/* org.apache.util.sql.Jisql -driver postgresql -cstring jdbc:postgresql://%s/%s -u %s -p '%s' -noheader -trim -c \;" %(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, path, self.host, db_name, user, password)
		elif os_name == "WINDOWS":
			jisql_cmd = "%s -cp %s;%s\jisql\\lib\\* org.apache.util.sql.Jisql -driver postgresql -cstring jdbc:postgresql://%s/%s -u %s -p \"%s\" -noheader -trim" %(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, path, self.host, db_name, user, password)
		return jisql_cmd

	def check_connection(self, db_name, db_user, db_password):
		log("[I] Checking connection", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if os_name == "LINUX":
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
		name = basename(file_name)
		if os.path.isfile(file_name):
			log("[I] Importing db schema to database " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if os_name == "LINUX":
				query = get_cmd + " -input %s" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -input %s -c ;" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(query)
			if ret == 0:
				log("[I] "+name + " DB schema imported successfully","info")
			else:
				log("[E] "+name + " DB schema import failed!","error")
				sys.exit(1)

	def grant_audit_db_user(self, audit_db_name , db_user, audit_db_user, db_password, audit_db_password):
		log("[I] Granting permission to " + audit_db_user, "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password, audit_db_name)
		log("[I] Granting select and usage privileges to Postgres audit user '" + audit_db_user + "' on XA_ACCESS_AUDIT_SEQ", "info")
		if os_name == "LINUX":
			query = get_cmd + " -query 'GRANT SELECT,USAGE ON XA_ACCESS_AUDIT_SEQ TO %s;'" % (audit_db_user)
			jisql_log(query, db_password)
			ret = subprocess.call(shlex.split(query))
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"GRANT SELECT,USAGE ON XA_ACCESS_AUDIT_SEQ TO %s;\" -c ;" % (audit_db_user)
			jisql_log(query, db_password)
			ret = subprocess.call(query)
		if ret != 0:
			log("[E] Granting select privileges to Postgres user '" + audit_db_user + "' failed", "error")
			sys.exit(1)

		log("[I] Granting insert privileges to Postgres audit user '" + audit_db_user + "' on XA_ACCESS_AUDIT table", "info")
		if os_name == "LINUX":
			query = get_cmd + " -query 'GRANT INSERT ON XA_ACCESS_AUDIT TO %s;'" % (audit_db_user)
			jisql_log(query, db_password)
			ret = subprocess.call(shlex.split(query))
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"GRANT INSERT ON XA_ACCESS_AUDIT TO %s;\" -c ;" % (audit_db_user)
			jisql_log(query, db_password)
			ret = subprocess.call(query)
		if ret != 0:
			log("[E] Granting insert privileges to Postgres user '" + audit_db_user + "' failed", "error")
			sys.exit(1)

	def create_language_plpgsql(self,db_user, db_password, db_name):
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if os_name == "LINUX":
			query = get_cmd + " -query \"SELECT 1 FROM pg_catalog.pg_language WHERE lanname='plpgsql';\""
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"SELECT 1 FROM pg_catalog.pg_language WHERE lanname='plpgsql';\" -c ;"
		jisql_log(query, db_password)
		output = check_output(query)
		if not output.strip('1 |'):
			if os_name == "LINUX":
				query = get_cmd + " -query \"CREATE LANGUAGE plpgsql;\""
				jisql_log(query, db_password)
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"CREATE LANGUAGE plpgsql;\" -c ;"
				jisql_log(query, db_password)
				ret = subprocess.call(query)
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
			if os_name == "LINUX":
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
			jisql_log(query, db_password)
			output = check_output(query)
			if output.strip(version + " |"):
				log("[I] Patch "+ name  +" is already applied" ,"info")
			else:
				if os_name == "LINUX":
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					while(output.strip(version + " |")):
						log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
						time.sleep(300)
						jisql_log(query, db_password)
						output = check_output(query)
				else:
					if os_name == "LINUX":
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', current_timestamp, '%s', current_timestamp, '%s','N') ;\"" %(version,db_user,db_user)
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', current_timestamp, '%s', current_timestamp, '%s','N') ;\" -c ;" %(version,db_user,db_user)
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret == 0:
						log ("[I] Patch "+ name +" is being applied..","info")
					else:
						log("[E] Patch "+ name +" failed", "error")
					if os_name == "LINUX":
						query = get_cmd + " -input %s" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -input %s -c ;" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret == 0:
						log("[I] "+name + " patch applied","info")
						if os_name == "LINUX":
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\"" %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\" -c ;" %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] Patch version updated", "info")
						else:
							log("[E] Updating patch version failed", "error")
							sys.exit(1)
					else:
						if os_name == "LINUX":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N';\"" %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N';\" -c ;" %(version)
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
				if os_name == "LINUX":
					query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					log("[I] Patch "+ name  +" is already applied" ,"info")
				else:
					if os_name == "LINUX":
						query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
						while(output.strip(version + " |")):
							log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
							time.sleep(300)
							jisql_log(query, db_password)
							output = check_output(query)
					else:
						if os_name == "LINUX":
							query = get_cmd1 + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', current_timestamp, '%s', current_timestamp, '%s','N') ;\"" %(version,db_user,db_user)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd1 + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', current_timestamp, '%s', current_timestamp, '%s','N') ;\" -c ;" %(version,db_user,db_user)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log ("[I] Patch "+ name +" is being applied..","info")
						else:
							log("[E] Patch "+ name +" failed", "error")
						get_cmd2 = self.get_jisql_cmd(db_user, db_password, audit_db_name)
						if os_name == "LINUX":
							query = get_cmd2 + " -input %s" %file_name
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd2 + " -input %s -c ;" %file_name
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] "+name + " patch applied","info")
							if os_name == "LINUX":
								query = get_cmd1 + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\"" %(version)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd1 + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\" -c ;" %(version)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0:
								log("[I] Patch version updated", "info")
							else:
								log("[E] Updating patch version failed", "error")
								sys.exit(1)
						else:
							if os_name == "LINUX":
								query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N';\"" %(version)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N';\" -c ;" %(version)
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
		if os_name == "LINUX":
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
					f = re.match("^Patch.*?.class$",filename)
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
					if os_name == "LINUX":
						query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\"" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
						log("[I] Java patch "+ className  +" is already applied" ,"info")
					else:
						if os_name == "LINUX":
							query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\"" %(version)
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\" -c ;" %(version)
						jisql_log(query, db_password)
						output = check_output(query)
						if output.strip(version + " |"):
							while(output.strip(version + " |")):
								log("[I] Java patch "+ className  +" is being applied by some other process" ,"info")
								time.sleep(300)
								jisql_log(query, db_password)
								output = check_output(query)
						else:
							if os_name == "LINUX":
								query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('J%s', current_timestamp, '%s', current_timestamp, '%s','N') ;\"" %(version,db_user,db_user)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('J%s', current_timestamp, '%s', current_timestamp, '%s','N') ;\" -c ;" %(version,db_user,db_user)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0:
								log ("[I] java patch "+ className +" is being applied..","info")
							else:
								log("[E] java patch "+ className +" failed", "error")
								sys.exit(1)
							if os_name == "LINUX":
								path = os.path.join("%s","WEB-INF","classes","conf:%s","WEB-INF","classes","lib","*:%s","WEB-INF",":%s","META-INF",":%s","WEB-INF","lib","*:%s","WEB-INF","classes",":%s","WEB-INF","classes","META-INF:%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							elif os_name == "WINDOWS":
								path = os.path.join("%s","WEB-INF","classes","conf;%s","WEB-INF","classes","lib","*;%s","WEB-INF",";%s","META-INF",";%s","WEB-INF","lib","*;%s","WEB-INF","classes",";%s","WEB-INF","classes","META-INF;%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							get_java_cmd = "%s -Dlogdir=%s -Dlog4j.configuration=db_patch.log4j.xml -cp %s org.apache.ranger.patch.%s"%(self.JAVA_BIN,ranger_log,path,className)
							if os_name == "LINUX":
								ret = subprocess.call(shlex.split(get_java_cmd))
							elif os_name == "WINDOWS":
								ret = subprocess.call(get_java_cmd)
							if ret == 0:
								if os_name == "LINUX":
									query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='J%s' and active='N';\"" %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='J%s' and active='N';\" -c ;" %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								if ret == 0:
									log ("[I] java patch "+ className +" is applied..","info")
								else:
									if os_name == "LINUX":
										query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N';\"" %(version)
										jisql_log(query, db_password)
										ret = subprocess.call(shlex.split(query))
									elif os_name == "WINDOWS":
										query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N';\" -c ;" %(version)
										jisql_log(query, db_password)
										ret = subprocess.call(query)
									log("[E] java patch "+ className +" failed", "error")
									sys.exit(1)
							else:
								if os_name == "LINUX":
									query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N';\"" %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N';\" -c ;" %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								log("[E] applying java patch "+ className +" failed", "error")
								sys.exit(1)


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
		if os_name == "LINUX":
			jisql_cmd = "%s -cp %s:%s/jisql/lib/* org.apache.util.sql.Jisql -user %s -p '%s' -driver mssql -cstring jdbc:sqlserver://%s\\;databaseName=%s -noheader -trim"%(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, path, user, password, self.host,db_name)
		elif os_name == "WINDOWS":
			jisql_cmd = "%s -cp %s;%s\\jisql\\lib\\* org.apache.util.sql.Jisql -user %s -p \"%s\" -driver mssql -cstring jdbc:sqlserver://%s;databaseName=%s -noheader -trim"%(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, path, user, password, self.host,db_name)
		return jisql_cmd

	def check_connection(self, db_name, db_user, db_password):
		log("[I] Checking connection", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if os_name == "LINUX":
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
		name = basename(file_name)
		if os.path.isfile(file_name):
			log("[I] Importing db schema to database " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if os_name == "LINUX":
				query = get_cmd + " -input %s" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -input %s" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(query)
			if ret == 0:
				log("[I] "+name + " DB schema imported successfully","info")
			else:
				log("[E] "+name + " DB Schema import failed!","error")
				sys.exit(1)

	def check_table(self, db_name, db_user, db_password, TABLE_NAME):
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if os_name == "LINUX":
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
		if os_name == "LINUX":
			query = get_cmd + " -c \; -query \"USE %s GRANT SELECT,INSERT to %s;\"" %(audit_db_name ,audit_db_user)
			jisql_log(query, db_password)
			ret = subprocess.call(shlex.split(query))
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"USE %s GRANT SELECT,INSERT to %s;\" -c ;" %(audit_db_name ,audit_db_user)
			jisql_log(query, db_password)
			ret = subprocess.call(query)
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
			if os_name == "LINUX":
				query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
			jisql_log(query, db_password)
			output = check_output(query)
			if output.strip(version + " |"):
				log("[I] Patch "+ name  +" is already applied" ,"info")
			else:
				if os_name == "LINUX":
					query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					while(output.strip(version + " |")):
						log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
						time.sleep(300)
						jisql_log(query, db_password)
						output = check_output(query)
				else:
					if os_name == "LINUX":
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c \;" %(version,db_user,db_user)
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c ;" %(version,db_user,db_user)
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret == 0:
						log ("[I] Patch "+ name +" is being applied..","info")
					else:
						log("[E] Patch "+ name +" failed", "error")
					if os_name == "LINUX":
						query = get_cmd + " -input %s" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -input %s" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret == 0:
						log("[I] "+name + " patch applied","info")
						if os_name == "LINUX":
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\" -c \;"  %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\" -c ;" %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] Patch version updated", "info")
						else:
							log("[E] Updating patch version failed", "error")
							sys.exit(1)
					else:
						if os_name == "LINUX":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N';\" -c \;"  %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N';\" -c ;" %(version)
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
				if os_name == "LINUX":
					query = get_cmd1 + " -c \; query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					log("[I] Patch "+ name  +" is already applied" ,"info")
				else:
					if os_name == "LINUX":
						query = get_cmd1 + " -c \; query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
						while(output.strip(version + " |")):
							log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
							time.sleep(300)
							jisql_log(query, db_password)
							output = check_output(query)
					else:
						get_cmd2 = self.get_jisql_cmd(db_user, db_password, audit_db_name)
						if os_name == "LINUX":
							query = get_cmd1 + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c \;" %(version,db_user,db_user)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd1 + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c ;" %(version,db_user,db_user)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log ("[I] Patch "+ name +" is being applied..","info")
						else:
							log("[E] Patch "+ name +" failed", "error")
						if os_name == "LINUX":
							query = get_cmd2 + " -input %s" %file_name
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd2 + " -input %s" %file_name
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] "+name + " patch applied","info")
							if os_name == "LINUX":
								query = get_cmd1 + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\" -c \;"  %(version)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd1 + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\" -c ;" %(version)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0:
								log("[I] Patch version updated", "info")
							else:
								log("[E] Updating patch version failed", "error")
								sys.exit(1)
						else:
							if os_name == "LINUX":
								query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N';\" -c \;"  %(version)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd1 + " -query \"delete from x_db_version_h where version='%s' and active='N';\" -c ;" %(version)
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
					f = re.match("^Patch.*?.class$",filename)
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
					if os_name == "LINUX":
						query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\" -c \;" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
						log("[I] Java patch "+ className  +" is already applied" ,"info")
					else:
						if os_name == "LINUX":
							query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\" -c \;" %(version)
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\" -c ;" %(version)
						jisql_log(query, db_password)
						output = check_output(query)
						if output.strip(version + " |"):
							while(output.strip(version + " |")):
								log("[I] Java patch "+ className  +" is being applied by some other process" ,"info")
								time.sleep(300)
								jisql_log(query, db_password)
								output = check_output(query)
						else:
							if os_name == "LINUX":
								query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('J%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c \;" %(version,db_user,db_user)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('J%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c ;" %(version,db_user,db_user)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0:
								log ("[I] java patch "+ className +" is being applied..","info")
							else:
								log("[E] java patch "+ className +" failed", "error")
								sys.exit(1)
							if os_name == "LINUX":
								path = os.path.join("%s","WEB-INF","classes","conf:%s","WEB-INF","classes","lib","*:%s","WEB-INF",":%s","META-INF",":%s","WEB-INF","lib","*:%s","WEB-INF","classes",":%s","WEB-INF","classes","META-INF:%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							elif os_name == "WINDOWS":
								path = os.path.join("%s","WEB-INF","classes","conf;%s","WEB-INF","classes","lib","*;%s","WEB-INF",";%s","META-INF",";%s","WEB-INF","lib","*;%s","WEB-INF","classes",";%s","WEB-INF","classes","META-INF;%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							get_java_cmd = "%s -Dlogdir=%s -Dlog4j.configuration=db_patch.log4j.xml -cp %s org.apache.ranger.patch.%s"%(self.JAVA_BIN,ranger_log,path,className)
							if os_name == "LINUX":
								ret = subprocess.call(shlex.split(get_java_cmd))
							elif os_name == "WINDOWS":
								ret = subprocess.call(get_java_cmd)
							if ret == 0:
								if os_name == "LINUX":
									query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='J%s' and active='N';\" -c \;"  %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='J%s' and active='N';\" -c ;" %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								if ret == 0:
									log ("[I] java patch "+ className +" is applied..","info")
								else:
									if os_name == "LINUX":
										query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N';\" -c \;"  %(version)
										jisql_log(query, db_password)
										ret = subprocess.call(shlex.split(query))
									elif os_name == "WINDOWS":
										query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N';\" -c ;" %(version)
										jisql_log(query, db_password)
										ret = subprocess.call(query)
									log("[E] java patch "+ className +" failed", "error")
									sys.exit(1)
							else:
								if os_name == "LINUX":
									query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N';\" -c \;"  %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N';\" -c ;" %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								log("[E] applying java patch "+ className +" failed", "error")
								sys.exit(1)


class SqlAnywhereConf(BaseDB):
	# Constructor
	def __init__(self, host, SQL_CONNECTOR_JAR, JAVA_BIN):
		self.host = host
		self.SQL_CONNECTOR_JAR = SQL_CONNECTOR_JAR
		self.JAVA_BIN = JAVA_BIN

	def get_jisql_cmd(self, user, password, db_name):
		path = RANGER_ADMIN_HOME
		self.JAVA_BIN = self.JAVA_BIN.strip("'")
		if os_name == "LINUX":
			jisql_cmd = "%s -cp %s:%s/jisql/lib/* org.apache.util.sql.Jisql -user %s -p '%s' -driver sapsajdbc4 -cstring jdbc:sqlanywhere:database=%s;host=%s -noheader -trim"%(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, path,user, password,db_name,self.host)
		elif os_name == "WINDOWS":
			jisql_cmd = "%s -cp %s;%s\\jisql\\lib\\* org.apache.util.sql.Jisql -user %s -p \"%s\" -driver sapsajdbc4 -cstring jdbc:sqlanywhere:database=%s;host=%s -noheader -trim"%(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, path, user, password,db_name,self.host)
		return jisql_cmd

	def check_connection(self, db_name, db_user, db_password):
		log("[I] Checking connection", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if os_name == "LINUX":
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
		name = basename(file_name)
		if os.path.isfile(file_name):
			log("[I] Importing db schema to database " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
			if os_name == "LINUX":
				query = get_cmd + " -input %s" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(shlex.split(query))
			elif os_name == "WINDOWS":
				query = get_cmd + " -input %s" %file_name
				jisql_log(query, db_password)
				ret = subprocess.call(query)
			if ret == 0:
				log("[I] "+name + " DB schema imported successfully","info")
			else:
				log("[E] "+name + " DB Schema import failed!","error")
				sys.exit(1)

	def check_table(self, db_name, db_user, db_password, TABLE_NAME):
		self.set_options(db_name, db_user, db_password, TABLE_NAME)
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if os_name == "LINUX":
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
		if os_name == "LINUX":
			query = get_cmd + " -c \; -query \"GRANT INSERT ON XA_ACCESS_AUDIT to %s;\"" %(audit_db_user)
			jisql_log(query, db_password)
			ret = subprocess.call(shlex.split(query))
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"GRANT INSERT ON XA_ACCESS_AUDIT to %s;\" -c ;" %(audit_db_user)
			jisql_log(query, db_password)
			ret = subprocess.call(query)
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
			if os_name == "LINUX":
				query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
			elif os_name == "WINDOWS":
				query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
			jisql_log(query, db_password)
			output = check_output(query)
			if output.strip(version + " |"):
				log("[I] Patch "+ name  +" is already applied" ,"info")
			else:
				if os_name == "LINUX":
					query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					while output.strip(version + " |"):
						log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
						time.sleep(300)
						jisql_log(query, db_password)
						output = check_output(query)
				else:
					if os_name == "LINUX":
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c \;" %(version,db_user,db_user)
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c ;" %(version,db_user,db_user)
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret == 0:
						log ("[I] Patch "+ name +" is being applied..","info")
					else:
						log("[E] Patch "+ name +" failed", "error")
					if os_name == "LINUX":
						query = get_cmd + " -input %s" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd + " -input %s" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret == 0:
						log("[I] "+name + " patch applied","info")
						if os_name == "LINUX":
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\" -c \;"  %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\" -c ;" %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] Patch version updated", "info")
						else:
							log("[E] Updating patch version failed", "error")
							sys.exit(1)
					else:
						if os_name == "LINUX":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N';\" -c \;"  %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N';\" -c ;" %(version)
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
				if os_name == "LINUX":
					query = get_cmd1 + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
				elif os_name == "WINDOWS":
					query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\" -c ;" %(version)
				jisql_log(query, db_password)
				output = check_output(query)
				if output.strip(version + " |"):
					log("[I] Patch "+ name  +" is already applied" ,"info")
				else:
					if os_name == "LINUX":
						query = get_cmd1 + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'N';\"" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd1 + " -query \"select version from x_db_version_h where version = '%s' and active = 'N';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
						while output.strip(version + " |"):
							log("[I] Patch "+ name  +" is being applied by some other process" ,"info")
							time.sleep(300)
							jisql_log(query, db_password)
							output = check_output(query)
					else:
						if os_name == "LINUX":
							query = get_cmd1 + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c \;" %(version,db_user,db_user)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd1 + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c ;" %(version,db_user,db_user)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log ("[I] Patch "+ name +" is being applied..","info")
						else:
							log("[E] Patch "+ name +" failed", "error")
					get_cmd2 = self.get_jisql_cmd(db_user, db_password, audit_db_name)
					if os_name == "LINUX":
						query = get_cmd2 + " -input %s" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(shlex.split(query))
					elif os_name == "WINDOWS":
						query = get_cmd2 + " -input %s" %file_name
						jisql_log(query, db_password)
						ret = subprocess.call(query)
					if ret == 0:
						if os_name == "LINUX":
							log("[I] "+name + " patch applied","info")
							query = get_cmd1 + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\" -c \;"  %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd1 + " -query \"update x_db_version_h set active='Y' where version='%s' and active='N';\" -c ;" %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(query)
						if ret == 0:
							log("[I] Patch version updated", "info")
						else:
							log("[E] Updating patch version failed", "error")
							sys.exit(1)
					else:
						if os_name == "LINUX":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N';\" -c \;"  %(version)
							jisql_log(query, db_password)
							ret = subprocess.call(shlex.split(query))
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"delete from x_db_version_h where version='%s' and active='N';\" -c ;" %(version)
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
					f = re.match("^Patch.*?.class$",filename)
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
					if os_name == "LINUX":
						query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\" -c \;" %(version)
					elif os_name == "WINDOWS":
						query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'Y';\" -c ;" %(version)
					jisql_log(query, db_password)
					output = check_output(query)
					if output.strip(version + " |"):
						log("[I] Java patch "+ className  +" is already applied" ,"info")
					else:
						if os_name == "LINUX":
							query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\" -c \;" %(version)
						elif os_name == "WINDOWS":
							query = get_cmd + " -query \"select version from x_db_version_h where version = 'J%s' and active = 'N';\" -c ;" %(version)
						jisql_log(query, db_password)
						output = check_output(query)
						if output.strip(version + " |"):
							while(output.strip(version + " |")):
								log("[I] Java patch "+ className  +" is being applied by some other process" ,"info")
								time.sleep(300)
								jisql_log(query, db_password)
								output = check_output(query)
						else:
							if os_name == "LINUX":
								query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('J%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c \;" %(version,db_user,db_user)
								jisql_log(query, db_password)
								ret = subprocess.call(shlex.split(query))
							elif os_name == "WINDOWS":
								query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by,active) values ('J%s', GETDATE(), '%s', GETDATE(), '%s','N') ;\" -c ;" %(version,db_user,db_user)
								jisql_log(query, db_password)
								ret = subprocess.call(query)
							if ret == 0:
								log ("[I] java patch "+ className +" is being applied..","info")
							else:
								log("[E] java patch "+ className +" failed", "error")
								sys.exit(1)
							if os_name == "LINUX":
								path = os.path.join("%s","WEB-INF","classes","conf:%s","WEB-INF","classes","lib","*:%s","WEB-INF",":%s","META-INF",":%s","WEB-INF","lib","*:%s","WEB-INF","classes",":%s","WEB-INF","classes","META-INF:%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							elif os_name == "WINDOWS":
								path = os.path.join("%s","WEB-INF","classes","conf;%s","WEB-INF","classes","lib","*;%s","WEB-INF",";%s","META-INF",";%s","WEB-INF","lib","*;%s","WEB-INF","classes",";%s","WEB-INF","classes","META-INF;%s" )%(app_home ,app_home ,app_home, app_home, app_home, app_home ,app_home ,self.SQL_CONNECTOR_JAR)
							get_java_cmd = "%s -Dlogdir=%s -Dlog4j.configuration=db_patch.log4j.xml -cp %s org.apache.ranger.patch.%s"%(self.JAVA_BIN,ranger_log,path,className)
							if os_name == "LINUX":
								ret = subprocess.call(shlex.split(get_java_cmd))
							elif os_name == "WINDOWS":
								ret = subprocess.call(get_java_cmd)
							if ret == 0:
								if os_name == "LINUX":
									query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='J%s' and active='N';\" -c \;"  %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"update x_db_version_h set active='Y' where version='J%s' and active='N';\" -c ;" %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								if ret == 0:
									log ("[I] java patch "+ className +" is applied..","info")
								else:
									if os_name == "LINUX":
										query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N';\" -c \;"  %(version)
										jisql_log(query, db_password)
										ret = subprocess.call(shlex.split(query))
									elif os_name == "WINDOWS":
										query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N';\" -c ;" %(version)
										jisql_log(query, db_password)
										ret = subprocess.call(query)
									log("[E] java patch "+ className +" failed", "error")
									sys.exit(1)
							else:
								if os_name == "LINUX":
									query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N';\" -c \;"  %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(shlex.split(query))
								elif os_name == "WINDOWS":
									query = get_cmd + " -query \"delete from x_db_version_h where version='J%s' and active='N';\" -c ;" %(version)
									jisql_log(query, db_password)
									ret = subprocess.call(query)
								log("[E] applying java patch "+ className +" failed", "error")
								sys.exit(1)

	def set_options(self, db_name, db_user, db_password, TABLE_NAME):
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		if os_name == "LINUX":
			query = get_cmd + " -c \; -query \"set option public.reserved_keywords='LIMIT';\""
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"set option public.reserved_keywords='LIMIT';\" -c ;"
		jisql_log(query, db_password)
		ret = subprocess.call(shlex.split(query))
		if os_name == "LINUX":
			query = get_cmd + " -c \; -query \"set option public.max_statement_count=0;\""
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"set option public.max_statement_count=0;\" -c;"
		jisql_log(query, db_password)
		ret = subprocess.call(shlex.split(query))
		if os_name == "LINUX":
			query = get_cmd + " -c \; -query \"set option public.max_cursor_count=0;\""
		elif os_name == "WINDOWS":
			query = get_cmd + " -query \"set option public.max_cursor_count=0;\" -c;"
		jisql_log(query, db_password)
		ret = subprocess.call(shlex.split(query))

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
	x_user = 'x_portal_user'

	audit_db_name = globalDict['audit_db_name']
	audit_db_user = globalDict['audit_db_user']
	audit_db_password = globalDict['audit_db_password']

	if XA_DB_FLAVOR == "MYSQL":
		MYSQL_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
		xa_sqlObj = MysqlConf(xa_db_host, MYSQL_CONNECTOR_JAR, JAVA_BIN)
		xa_db_version_file = os.path.join(RANGER_ADMIN_HOME , mysql_dbversion_catalog)
		xa_db_core_file = os.path.join(RANGER_ADMIN_HOME , mysql_core_file)
		xa_patch_file = os.path.join(RANGER_ADMIN_HOME ,mysql_patches)
		audit_patch_file = os.path.join(RANGER_ADMIN_HOME ,mysql_auditdb_patches)
		
	elif XA_DB_FLAVOR == "ORACLE":
		ORACLE_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
		xa_sqlObj = OracleConf(xa_db_host, ORACLE_CONNECTOR_JAR, JAVA_BIN)
		xa_db_version_file = os.path.join(RANGER_ADMIN_HOME ,oracle_dbversion_catalog)
		xa_db_core_file = os.path.join(RANGER_ADMIN_HOME ,oracle_core_file)
		xa_patch_file = os.path.join(RANGER_ADMIN_HOME ,oracle_patches)
		audit_patch_file = os.path.join(RANGER_ADMIN_HOME ,oracle_auditdb_patches)

	elif XA_DB_FLAVOR == "POSTGRES":
		db_user=db_user.lower()
        	db_name=db_name.lower()
		POSTGRES_CONNECTOR_JAR = globalDict['SQL_CONNECTOR_JAR']
		xa_sqlObj = PostgresConf(xa_db_host, POSTGRES_CONNECTOR_JAR, JAVA_BIN)
		xa_db_version_file = os.path.join(RANGER_ADMIN_HOME , postgres_dbversion_catalog)
		xa_db_core_file = os.path.join(RANGER_ADMIN_HOME , postgres_core_file)
		xa_patch_file = os.path.join(RANGER_ADMIN_HOME , postgres_patches)
		audit_patch_file = os.path.join(RANGER_ADMIN_HOME ,postgres_auditdb_patches)

	elif XA_DB_FLAVOR == "MSSQL":
		SQLSERVER_CONNECTOR_JAR = globalDict['SQL_CONNECTOR_JAR']
		xa_sqlObj = SqlServerConf(xa_db_host, SQLSERVER_CONNECTOR_JAR, JAVA_BIN)
		xa_db_version_file = os.path.join(RANGER_ADMIN_HOME ,sqlserver_dbversion_catalog)
		xa_db_core_file = os.path.join(RANGER_ADMIN_HOME , sqlserver_core_file)
		xa_patch_file = os.path.join(RANGER_ADMIN_HOME , sqlserver_patches)
		audit_patch_file = os.path.join(RANGER_ADMIN_HOME ,sqlserver_auditdb_patches)

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

	else:
		log("[E] --------- NO SUCH SUPPORTED DB FLAVOUR!! ---------", "error")
		sys.exit(1)

	if AUDIT_DB_FLAVOR == "MYSQL":
		MYSQL_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
		audit_sqlObj = MysqlConf(audit_db_host,MYSQL_CONNECTOR_JAR,JAVA_BIN)
		audit_db_file = os.path.join(RANGER_ADMIN_HOME ,mysql_audit_file)

	elif AUDIT_DB_FLAVOR == "ORACLE":
		ORACLE_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
		audit_sqlObj = OracleConf(audit_db_host, ORACLE_CONNECTOR_JAR, JAVA_BIN)
		audit_db_file = os.path.join(RANGER_ADMIN_HOME , oracle_audit_file)

	elif AUDIT_DB_FLAVOR == "POSTGRES":
		audit_db_user=audit_db_user.lower()
	        audit_db_name=audit_db_name.lower()
		POSTGRES_CONNECTOR_JAR = globalDict['SQL_CONNECTOR_JAR']
		audit_sqlObj = PostgresConf(audit_db_host, POSTGRES_CONNECTOR_JAR, JAVA_BIN)
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

	if 'audit_store' in globalDict:
		audit_store = globalDict['audit_store']
	else:
		audit_store = None

	if audit_store is None or audit_store == "":
		audit_store = "db"
	audit_store=audit_store.lower()
	if len(argv)==1:

		log("[I] --------- Verifying Ranger DB tables ---------","info")
		if xa_sqlObj.check_table(db_name, db_user, db_password, x_user):
			pass
		else:
			log("[I] --------- Importing Ranger Core DB Schema ---------","info")
			xa_sqlObj.import_db_file(db_name, db_user, db_password, xa_db_core_file)
			if XA_DB_FLAVOR == "ORACLE":
				if xa_sqlObj.check_table(db_name, db_user, db_password, xa_access_audit):
					if db_user != audit_db_user:
						xa_sqlObj.create_synonym(db_name, db_user, db_password,audit_db_user)
		log("[I] --------- Verifying upgrade history table ---------","info")
		output = xa_sqlObj.check_table(db_name, db_user, db_password, x_db_version)
		if output == False:
			log("[I] --------- Creating version history table ---------","info")
			xa_sqlObj.upgrade_db(db_name, db_user, db_password, xa_db_version_file)
		log("[I] --------- Applying Ranger DB patches ---------","info")
		xa_sqlObj.apply_patches(db_name, db_user, db_password, xa_patch_file)
		if audit_store == "db":
			log("[I] --------- Starting Audit Operation ---------","info")
			audit_sqlObj.auditdb_operation(xa_db_host, audit_db_host, db_name, audit_db_name, db_user, audit_db_user, db_password, audit_db_password, audit_db_file, xa_access_audit)
			log("[I] --------- Applying Audit DB patches ---------","info")
			audit_sqlObj.apply_auditdb_patches(xa_sqlObj,xa_db_host, audit_db_host, db_name, audit_db_name, db_user, audit_db_user, db_password, audit_db_password, audit_patch_file, xa_access_audit)

	if len(argv)>1:
		for i in range(len(argv)):
			if str(argv[i]) == "-javapatch":
			        xa_sqlObj.execute_java_patches(xa_db_host, db_user, db_password, db_name)

main(sys.argv)
