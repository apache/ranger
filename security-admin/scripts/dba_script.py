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
import logging
import subprocess
import fileinput
import getpass
from os.path import basename
from subprocess import Popen,PIPE
from datetime import date
globalDict = {}

def check_output(query):
	p = subprocess.Popen(query, stdout=subprocess.PIPE)
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
	read_config_file = open(os.path.join(os.getcwd(),'install.properties'))
	for each_line in read_config_file.read().split('\n') :
		if len(each_line) == 0 : continue
		if re.search('=', each_line):
			key , value = each_line.strip().split("=",1)
			key = key.strip()

			if 'PASSWORD' in key:
				jceks_file_path = os.path.join(os.getenv('RANGER_HOME'), 'jceks','ranger_db.jceks')
				statuscode,value = call_keystore(library_path,key,'',jceks_file_path,'get')
				if statuscode == 1:
					value = ''
			value = value.strip()
			globalDict[key] = value

class BaseDB(object):

	def create_rangerdb_user(self, root_user, db_user, db_password, db_root_password):	
		log("[I] ---------- Creating user ----------", "info")

	def check_connection(self, db_name, db_user, db_password):
		log("[I] ---------- Verifying DB connection ----------", "info")

	def create_db(self, root_user, db_root_password, db_name, db_user, db_password):
		log("[I] ---------- Verifying database ----------", "info")

	def create_auditdb_user(self, xa_db_host , audit_db_host , db_name ,audit_db_name, xa_db_root_user, audit_db_root_user, db_user, audit_db_user, xa_db_root_password, audit_db_root_password, db_password, audit_db_password, DBA_MODE):
		log("[I] ----------------- Create audit user ------------", "info")



class MysqlConf(BaseDB):
	# Constructor
	def __init__(self, host,SQL_CONNECTOR_JAR,JAVA_BIN):
		self.host = host
		self.SQL_CONNECTOR_JAR = SQL_CONNECTOR_JAR
		self.JAVA_BIN = JAVA_BIN

	def get_jisql_cmd(self, user, password ,db_name):
		#TODO: User array for forming command
		jisql_cmd = "%s -cp %s:jisql/lib/* org.apache.util.sql.Jisql -driver mysqlconj -cstring jdbc:mysql://%s/%s -u %s -p %s -noheader -trim -c \;" %(self.JAVA_BIN,self.SQL_CONNECTOR_JAR,self.host,db_name,user,password)
		return jisql_cmd

	def verify_user(slef, root_user, db_root_password, host, db_user, get_cmd):
		log("[I] Verifying user " + db_user , "info")
		query = get_cmd + " -query \"select user from mysql.user where user='%s' and host='%s';\"" %(db_user,host)
		output = check_output(shlex.split(query))
		if output.strip(db_user + " |"):
			return True
		else:
			return False

	def check_connection(self, db_name, db_user, db_password):
		log("[I] Checking connection..", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		query = get_cmd + " -query \"SELECT version();\""
		output = check_output(shlex.split(query))
		if output.strip('Production  |'):
			log("[I] Checking connection passed.", "info")
			return True
		else:
			log("[E] Can't establish connection!! Exiting.." ,"error")
			sys.exit(1)

	def create_rangerdb_user(self, root_user, db_user, db_password, db_root_password):
		if self.check_connection('mysql', root_user, db_root_password):
			hosts_arr =["%", "localhost"]
			for host in hosts_arr:
				get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'mysql')
				if self.verify_user(root_user, db_root_password, host, db_user, get_cmd):
					log("[I] MySQL user " + db_user + " already exists for host " + host, "info")
				else:
					log("[I] MySQL user " + db_user + " does not exists for host " + host, "info")
					if db_password == "":
						query = get_cmd + " -query \"create user '%s'@'%s';\"" %(db_user, host)
						ret = subprocess.check_call(shlex.split(query))
						if ret == 0:
							if self.verify_user(root_user, db_root_password, host, db_user, get_cmd):
								log("[I] MySQL user " + db_user +" created for host " + host ,"info")
							else:
								log("[E] Creating MySQL user " + db_user +" failed","error")
								sys.exit(1)
					else:
						query = get_cmd + " -query \"create user '%s'@'%s' identified by '%s';\"" %(db_user,host,db_password)
						ret = subprocess.check_call(shlex.split(query))
						if ret == 0:
							if self.verify_user(root_user, db_root_password, host, db_user, get_cmd):
								log("[I] MySQL user " + db_user +" created for host " + host ,"info")
							else:
								log("[E] Creating MySQL user " + db_user +" failed","error")
								sys.exit(1)
						else:
							log("[E] Creating MySQL user " + db_user +" failed","error")
							sys.exit(1)


	def verify_db(self, root_user, db_root_password, db_name):
		log("[I] Verifying database " + db_name , "info")
		get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'mysql')
		query = get_cmd + " -query \"show databases like '%s';\"" %(db_name)
		output = check_output(shlex.split(query))
		if output.strip(db_name + " |"):
			return True
		else:
			return False


	def create_db(self, root_user, db_root_password, db_name, db_user, db_password):
		if self.verify_db(root_user, db_root_password, db_name):
			log("[I] Database "+db_name + " already exists.","info")
		else:
			log("[I] Database does not exist! Creating database " + db_name,"info")
			get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'mysql')
			query = get_cmd + " -query \"create database %s;\"" %(db_name)
			ret = subprocess.check_call(shlex.split(query))
			if ret != 0:
				log("[E] Database creation failed!!","error")
				sys.exit(1)
			else:
				if self.verify_db(root_user, db_root_password, db_name):
					log("[I] Creating database " + db_name + " succeeded", "info")
					return True
				else:
					log("[E] Database creation failed!!","error")
					sys.exit(1)


	def grant_xa_db_user(self, root_user, db_name, db_user, db_password, db_root_password, is_revoke):
		hosts_arr =["%", "localhost"]
		if is_revoke:
			for host in hosts_arr:
				get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'mysql')
				query = get_cmd + " -query \"REVOKE ALL PRIVILEGES,GRANT OPTION FROM '%s'@'%s';\"" %(db_user, host)
				ret = subprocess.check_call(shlex.split(query))
				if ret == 0:
					query = get_cmd + " -query \"FLUSH PRIVILEGES;\""
					ret = subprocess.check_call(shlex.split(query))
					if ret != 0:
						sys.exit(1)
				else:
					sys.exit(1)

		for host in hosts_arr:
			log("[I] ---------------Granting privileges TO user '"+db_user+"'@'"+host+"' on db '"+db_name+"'-------------" , "info")
			get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'mysql')
			query = get_cmd + " -query \"grant all privileges on %s.* to '%s'@'%s' with grant option;\"" %(db_name,db_user, host)
			ret = subprocess.check_call(shlex.split(query))
			if ret == 0:
				log("[I] ---------------FLUSH PRIVILEGES -------------" , "info")
				query = get_cmd + " -query \"FLUSH PRIVILEGES;\""
				ret = subprocess.check_call(shlex.split(query))
				if ret == 0:
					log("[I] Privileges granted to '" + db_user + "' on '"+db_name+"'", "info")
				else:
					log("[E] Granting privileges to '" +db_user+"' failed on '"+db_name+"'", "error")
					sys.exit(1)
			else:
				log("[E] Granting privileges to '" +db_user+"' failed on '"+db_name+"'", "error")
				sys.exit(1)


	def create_auditdb_user(self, xa_db_host, audit_db_host, db_name, audit_db_name, xa_db_root_user, audit_db_root_user, db_user, audit_db_user, xa_db_root_password, audit_db_root_password, db_password, audit_db_password, DBA_MODE):
		if DBA_MODE == "TRUE" :
			log("[I] --------- Setup audit user ---------","info")
			self.create_rangerdb_user(audit_db_root_user, audit_db_user, audit_db_password, audit_db_root_password)
			hosts_arr =["%", "localhost"]
			for host in hosts_arr:
				get_cmd = self.get_jisql_cmd(audit_db_root_user, audit_db_root_password ,'mysql')
				query = get_cmd + " -query \"REVOKE ALL PRIVILEGES,GRANT OPTION FROM '%s'@'%s';\"" %(audit_db_user, host)
				ret = subprocess.check_call(shlex.split(query))
				if ret == 0:
					query = get_cmd + " -query \"FLUSH PRIVILEGES;\""
					ret = subprocess.check_call(shlex.split(query))
					if ret != 0:
						sys.exit(1)
				else:
					sys.exit(1)
			self.create_db(audit_db_root_user, audit_db_root_password, audit_db_name, db_user, db_password)
			self.grant_xa_db_user(audit_db_root_user, audit_db_name, db_user, db_password, audit_db_root_password, False)
		
	

class OracleConf(BaseDB):
	# Constructor
	def __init__(self, host, SQL_CONNECTOR_JAR, JAVA_BIN):
		self.host = host 
		self.SQL_CONNECTOR_JAR = SQL_CONNECTOR_JAR
		self.JAVA_BIN = JAVA_BIN

	def get_jisql_cmd(self, user, password):
		#TODO: User array for forming command
		jisql_cmd = "%s -cp %s:jisql/lib/* org.apache.util.sql.Jisql -driver oraclethin -cstring jdbc:oracle:thin:@%s -u '%s' -p '%s' -noheader -trim" %(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, self.host, user, password)
		return jisql_cmd

	def check_connection(self, db_name, db_user, db_password):
		log("[I] Checking connection", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password)
		query = get_cmd + " -c \; -query \"select * from v$version;\""
		output = check_output(shlex.split(query))
		if output.strip('Production  |'):
			log("[I] Connection success", "info")
			return True
		else:
			log("[E] Can't establish connection!", "error")
			sys.exit(1)

	def verify_user(self, root_user, db_user, db_root_password):
		log("[I] Verifying user " + db_user ,"info")
		get_cmd = self.get_jisql_cmd(root_user, db_root_password)		
		query = get_cmd + " -c \; -query \"select username from all_users where upper(username)=upper('%s');\"" %(db_user)
		output = check_output(shlex.split(query))
		if output.strip(db_user + " |"):
			return True
		else:
			return False

	def create_rangerdb_user(self, root_user, db_user, db_password, db_root_password):
		if self.check_connection(self, root_user, db_root_password):
			if self.verify_user(root_user, db_user, db_root_password):
				log("[I] Oracle user " + db_user + " already exists!", "info")
			else:
				log("[I] User does not exists, Creating user : " + db_user, "info")
				get_cmd = self.get_jisql_cmd(root_user, db_root_password)
				query = get_cmd + " -c \; -query 'create user %s identified by \"%s\";'" %(db_user, db_password)
				ret = subprocess.check_call(shlex.split(query))
				if ret == 0:
					if self.verify_user(root_user, db_user, db_root_password):
						log("[I] User " + db_user + " created", "info")
						log("[I] Granting permission to " + db_user, "info")
						query = get_cmd + " -c \; -query 'GRANT CREATE SESSION,CREATE PROCEDURE,CREATE TABLE,CREATE VIEW,CREATE SEQUENCE,CREATE PUBLIC SYNONYM,CREATE TRIGGER,UNLIMITED Tablespace TO %s WITH ADMIN OPTION;'" % (db_user)
						ret = subprocess.check_call(shlex.split(query))
						if ret == 0:
							log("[I] Granting permissions to Oracle user '" + db_user + "' for %s done" %(self.host), "info")
						else:
							log("[E] Granting permissions to Oracle user '" + db_user + "' failed", "error")
							sys.exit(1)
					else:
						log("[E] Creating Oracle user '" + db_user + "' failed", "error")
						sys.exit(1)
				else:
					log("[E] Creating Oracle user '" + db_user + "' failed", "error")
					sys.exit(1)


	def verify_tablespace(self, root_user, db_root_password, db_name):
		log("[I] Verifying tablespace " + db_name, "info")
		get_cmd = self.get_jisql_cmd(root_user, db_root_password)		
		query = get_cmd + " -c \; -query \"SELECT DISTINCT UPPER(TABLESPACE_NAME) FROM USER_TablespaceS where UPPER(Tablespace_Name)=UPPER(\'%s\');\"" %(db_name)
		output = check_output(shlex.split(query))
		if output.strip(db_name+' |'):
			return True
		else:
			return False

	def create_db(self, root_user, db_root_password, db_name, db_user, db_password):
		if self.verify_tablespace(root_user, db_root_password, db_name):
			log("[I] Tablespace " + db_name + " already exists.","info")
			if self.verify_user(root_user, db_user, db_root_password):
				get_cmd = self.get_jisql_cmd(db_user ,db_password)
				query = get_cmd + " -c \; -query 'select default_tablespace from user_users;'"
				output = check_output(shlex.split(query)).strip()
				db_name = db_name.upper() +' |'
				if output == db_name:
					log("[I] User name " + db_user + " and tablespace " + db_name + " already exists.","info")
				else:
					log("[E] "+db_user + " user already assigned some other tablespace , give some other DB name.","error")
					sys.exit(1)
				#status = self.assign_tablespace(root_user, db_root_password, db_user, db_password, db_name, False)
				#return status
		else:
			log("[I] Tablespace does not exist. Creating tablespace: " + db_name,"info")
		        get_cmd = self.get_jisql_cmd(root_user, db_root_password)
			query = get_cmd + " -c \; -query \"create tablespace %s datafile '%s.dat' size 10M autoextend on;\"" %(db_name, db_name)
			ret = subprocess.check_call(shlex.split(query))
			if ret == 0:
				if self.verify_tablespace(root_user, db_root_password, db_name):
					log("[I] Creating tablespace "+db_name+" succeeded", "info")
					status = self.assign_tablespace(root_user, db_root_password, db_user, db_password, db_name, True)
					return status
				else:
					log("[E] Creating tablespace "+db_name+" failed", "error")
					sys.exit(1)
			else:
				log("[E] Creating tablespace "+db_name+" failed", "error")
				sys.exit(1)

	def assign_tablespace(self, root_user, db_root_password, db_user, db_password, db_name, status):
		log("[I] Assign default tablespace " +db_name + " to " + db_user, "info")
		# Assign default tablespace db_name
		get_cmd = self.get_jisql_cmd(root_user , db_root_password)
		query = get_cmd +" -c \; -query 'alter user %s identified by \"%s\" DEFAULT Tablespace %s;'" %(db_user, db_password, db_name)
		ret = subprocess.check_call(shlex.split(query))
		if ret == 0:
			log("[I] Granting permission to " + db_user, "info")
			query = get_cmd + " -c \; -query 'GRANT CREATE SESSION,CREATE PROCEDURE,CREATE TABLE,CREATE VIEW,CREATE SEQUENCE,CREATE PUBLIC SYNONYM,CREATE TRIGGER,UNLIMITED Tablespace TO %s WITH ADMIN OPTION;'" % (db_user)
			ret = subprocess.check_call(shlex.split(query))
			if ret == 0:
				log("[I] Granting Oracle user '" + db_user + "' done", "info")
				return status
			else:
				log("[E] Granting Oracle user '" + db_user + "' failed", "error")
				sys.exit(1)
		else:
			log("[E] Assigning default tablespace to user '" + db_user + "' failed", "error")
			sys.exit(1)


	def create_auditdb(self, audit_db_root_user, db_name ,audit_db_name, db_user, audit_db_user, db_password, audit_db_password, audit_db_root_password):
		if self.verify_tablespace(audit_db_root_user, audit_db_root_password, audit_db_name):
			log("[I] Tablespace " + audit_db_name + " already exists.","info")
			status1 = True
		else:
			log("[I] Tablespace does not exist. Creating tablespace: " + audit_db_name,"info")
			get_cmd = self.get_jisql_cmd(audit_db_root_user, audit_db_root_password)
			query = get_cmd + " -c \; -query \"create tablespace %s datafile '%s.dat' size 10M autoextend on;\"" %(audit_db_name, audit_db_name)
			ret = subprocess.check_call(shlex.split(query))
			if ret != 0:
				log("[E] Tablespace creation failed!!","error")
				sys.exit(1)
			else:
				log("[I] Creating tablespace "+ audit_db_name + " succeeded", "info")
				status1 = True

		if self.verify_tablespace(audit_db_root_user, audit_db_root_password, db_name):
			log("[I] Tablespace " + db_name + " already exists.","info")
			status2 = True
		else:
			log("[I] Tablespace does not exist. Creating tablespace: " + db_name,"info")
			get_cmd = self.get_jisql_cmd(audit_db_root_user, audit_db_root_password)
			query = get_cmd + " -c \; -query \"create tablespace %s datafile '%s.dat' size 10M autoextend on;\"" %(db_name, db_name)
			ret = subprocess.check_call(shlex.split(query))
			if ret != 0:
				log("[E] Tablespace creation failed!!","error")
				sys.exit(1)
			else:
				log("[I] Creating tablespace "+ db_name + " succeeded", "info")
				status2 = True

		if (status1 == True and status2 == True):
			log("[I] Assign default tablespace " + db_name + " to : " + audit_db_user, "info")
			# Assign default tablespace db_name
			get_cmd = self.get_jisql_cmd(audit_db_root_user , audit_db_root_password)
			query = get_cmd +" -c \; -query 'alter user %s identified by \"%s\" DEFAULT Tablespace %s;'" %(audit_db_user, audit_db_password, db_name)
			ret1 = subprocess.check_call(shlex.split(query))
 
			log("[I] Assign default tablespace " + audit_db_name + " to : " + audit_db_user, "info")
			# Assign default tablespace audit_db_name
			get_cmd = self.get_jisql_cmd(audit_db_root_user , audit_db_root_password)
			query = get_cmd +" -c \; -query 'alter user %s identified by \"%s\" DEFAULT Tablespace %s;'" %(audit_db_user, audit_db_password, audit_db_name)
			ret2 = subprocess.check_call(shlex.split(query))

			if (ret1 == 0 and ret2 == 0):
				log("[I] Granting permission to " + db_user, "info")
				query = get_cmd + " -c \; -query 'GRANT CREATE SESSION,CREATE PROCEDURE,CREATE TABLE,CREATE VIEW,CREATE SEQUENCE,CREATE PUBLIC SYNONYM,CREATE TRIGGER,UNLIMITED Tablespace TO %s WITH ADMIN OPTION;'" % (db_user)
				ret = subprocess.check_call(shlex.split(query))
				if ret == 0:
					return True
				else:
					log("[E] Granting Oracle user '" + db_user + "' failed", "error")
					sys.exit(1)
			else:
				return False


	def grant_xa_db_user(self, root_user, db_name, db_user, db_password, db_root_password, invoke):
		get_cmd = self.get_jisql_cmd(root_user ,db_root_password)
		query = get_cmd + " -c \; -query 'GRANT CREATE SESSION,CREATE PROCEDURE,CREATE TABLE,CREATE VIEW,CREATE SEQUENCE,CREATE PUBLIC SYNONYM,CREATE TRIGGER,UNLIMITED Tablespace TO %s WITH ADMIN OPTION;'" % (db_user)
		ret = subprocess.check_call(shlex.split(query))
		if ret == 0:
			log("[I] Granted permission to " + db_user, "info")
			return True
		else:
			log("[E] Granting Oracle user '" + db_user + "' failed", "error")
			sys.exit(1)


	def create_auditdb_user(self, xa_db_host , audit_db_host , db_name ,audit_db_name, xa_db_root_user, audit_db_root_user, db_user, audit_db_user, xa_db_root_password, audit_db_root_password, db_password, audit_db_password, DBA_MODE):
		if DBA_MODE == "TRUE":
			log("[I] --------- Setup audit user ---------","info")
			#self.create_rangerdb_user(audit_db_root_user, db_user, db_password, audit_db_root_password)
			if self.verify_user(audit_db_root_user, db_user, audit_db_root_password):
				log("[I] Oracle admin user " + db_user + " already exists!", "info")
			else:
				log("[I] User does not exists, Creating user " + db_user, "info")
				get_cmd = self.get_jisql_cmd(audit_db_root_user, audit_db_root_password)
				query = get_cmd + " -c \; -query 'create user %s identified by \"%s\";'" %(db_user, db_password)
				ret = subprocess.check_call(shlex.split(query))
				if ret == 0:
					if self.verify_user(audit_db_root_user, db_user, audit_db_root_password):
						log("[I] User " + db_user + " created", "info")
						log("[I] Granting permission to " + db_user, "info")
						query = get_cmd + " -c \; -query 'GRANT CREATE SESSION,CREATE PROCEDURE,CREATE TABLE,CREATE VIEW,CREATE SEQUENCE,CREATE PUBLIC SYNONYM,CREATE TRIGGER,UNLIMITED Tablespace TO %s WITH ADMIN OPTION;'" % (db_user)
						ret = subprocess.check_call(shlex.split(query))
						if ret == 0:
							log("[I] Granting permissions to Oracle user '" + db_user + "' for %s Done" %(self.host), "info")
						else:
							log("[E] Granting permissions to Oracle user '" + db_user + "' failed", "error")
							sys.exit(1)
					else:
						log("[E] Creating Oracle user '" + db_user + "' failed", "error")
						sys.exit(1)
				else:
					log("[E] Creating Oracle user '" + db_user + "' failed", "error")
					sys.exit(1)

			if self.verify_user(audit_db_root_user, audit_db_user, audit_db_root_password):
				log("[I] Oracle audit user " + audit_db_user + " already exist!", "info")
			else:
				log("[I] Audit user does not exists, Creating audit user " + audit_db_user, "info")
				get_cmd = self.get_jisql_cmd(audit_db_root_user, audit_db_root_password)
				query = get_cmd + " -c \; -query 'create user %s identified by \"%s\";'" %(audit_db_user, audit_db_password)
				ret = subprocess.check_call(shlex.split(query))
				if ret == 0:
					if self.verify_user(audit_db_root_user, audit_db_user, audit_db_root_password):
						query = get_cmd + " -c \; -query \"GRANT CREATE SESSION TO %s;\"" %(audit_db_user)
						ret = subprocess.check_call(shlex.split(query))
						if ret == 0:
							log("[I] Granting permission to " + audit_db_user + " done", "info")
						else:
							log("[E] Granting permission to " + audit_db_user + " failed", "error")
							sys.exit(1)
					else:
						log("[I] Creating audit user " + audit_db_user + " failed!", "info")
			
			self.create_auditdb(audit_db_root_user, db_name ,audit_db_name, db_user, audit_db_user, db_password, audit_db_password, audit_db_root_password)
		if DBA_MODE == "TRUE":
			self.grant_xa_db_user(audit_db_root_user, audit_db_name, db_user, db_password, audit_db_root_password, True)


class PostgresConf(BaseDB):
	# Constructor
	def __init__(self, host, SQL_CONNECTOR_JAR, JAVA_BIN):
		self.host = host
		self.SQL_CONNECTOR_JAR = SQL_CONNECTOR_JAR
		self.JAVA_BIN = JAVA_BIN


	def get_jisql_cmd(self, user, password, db_name):
		#TODO: User array for forming command
		jisql_cmd = "%s -cp %s:jisql/lib/* org.apache.util.sql.Jisql -driver postgresql -cstring jdbc:postgresql://%s:5432/%s -u %s -p %s -noheader -trim -c \;" %(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, self.host, db_name, user, password)
		return jisql_cmd

	def verify_user(self, root_user, db_root_password, db_user):
		log("[I] Verifying user " + db_user , "info")
		get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'postgres')
		query = get_cmd + " -query \"SELECT rolname FROM pg_roles WHERE rolname='%s';\"" %(db_user)
		output = check_output(shlex.split(query))
		if output.strip(db_user + " |"):
			return True
		else:
			return False

	def check_connection(self, db_name, db_user, db_password):
		log("[I] Checking connection", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		query = get_cmd + " -query \"SELECT 1;\""
		output = check_output(shlex.split(query))
		if output.strip('1 |'):
			log("[I] connection success", "info")
			return True
		else:
			log("[E] Can't establish connection", "error")
			sys.exit(1)

	def create_rangerdb_user(self, root_user, db_user, db_password, db_root_password):
		if self.check_connection('postgres', root_user, db_root_password):
			if self.verify_user(root_user, db_root_password, db_user):
				log("[I] Postgres user " + db_user + " already exists!", "info")
			else:
				log("[I] User does not exists, Creating user : " + db_user, "info")
				get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'postgres')
				query = get_cmd + " -query \"CREATE USER %s WITH LOGIN PASSWORD '%s';\"" %(db_user, db_password)
				ret = subprocess.check_call(shlex.split(query))
				if ret == 0:
					if self.verify_user(root_user, db_root_password, db_user):
						log("[I] Postgres user " + db_user + " created", "info")
					else:
						log("[E] Postgres user " +db_user+" creation failed", "error")
						sys.exit(1)
				else:
					log("[E] Postgres user " +db_user+" creation failed", "error")
					sys.exit(1)


	def verify_db(self, root_user, db_root_password, db_name):
		log("[I] Verifying database " + db_name , "info")
		get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'postgres')
		query = get_cmd + " -query \"SELECT datname FROM pg_database where datname='%s';\"" %(db_name)
		output = check_output(shlex.split(query))
		if output.strip(db_name + " |"):
			return True
		else:
			return False


	def create_db(self, root_user, db_root_password, db_name, db_user, db_password):
		if self.verify_db(root_user, db_root_password, db_name):
			log("[I] Database "+db_name + " already exists.", "info")
		else:
			log("[I] Database does not exist! Creating database : " + db_name,"info")
			get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'postgres')
			query = get_cmd + " -query \"create database %s with OWNER %s;\"" %(db_name, db_user)
			ret = subprocess.check_call(shlex.split(query))
			if ret != 0:
				log("[E] Database creation failed!!","error")
				sys.exit(1)
			else:
				if self.verify_db(root_user, db_root_password, db_name):
					log("[I] Creating database " + db_name + " succeeded", "info")
					return True
				else:
					log("[E] Database creation failed!!","error")
					sys.exit(1)


	def grant_xa_db_user(self, root_user, db_name, db_user, db_password, db_root_password , True):
		log("[I] Granting privileges TO user '"+db_user+"' on db '"+db_name+"'" , "info")
		get_cmd = self.get_jisql_cmd(root_user, db_root_password, db_name)
		query = get_cmd + " -query \"GRANT ALL PRIVILEGES ON DATABASE %s to %s;\"" %(db_name, db_user)
		ret = subprocess.check_call(shlex.split(query))
		if ret != 0:
			log("[E] Granting privileges on tables in schema public failed", "error")
			sys.exit(1)

		query = get_cmd + " -query \"GRANT ALL PRIVILEGES ON SCHEMA public TO %s;\"" %(db_user)
		ret = subprocess.check_call(shlex.split(query))
		if ret != 0:
			log("[E] Granting privileges on schema public failed", "error")
			sys.exit(1)

		query = get_cmd + " -query \"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO %s;\"" %(db_user)
		ret = subprocess.check_call(shlex.split(query))
		if ret != 0:
			log("[E] Granting privileges on database "+db_name+ " failed", "error")
			sys.exit(1)

		query = get_cmd + " -query \"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO %s;\"" %(db_user)
		ret = subprocess.check_call(shlex.split(query))
		if ret != 0:
			log("[E] Granting privileges on database "+db_name+ " failed", "error")
			sys.exit(1)
		log("[I] Granting privileges TO user '"+db_user+"' on db '"+db_name+"' Done" , "info")


	def create_auditdb_user(self, xa_db_host, audit_db_host, db_name, audit_db_name, xa_db_root_user, audit_db_root_user, db_user, audit_db_user, xa_db_root_password, audit_db_root_password, db_password, audit_db_password, DBA_MODE):
		if DBA_MODE == "TRUE":
			log("[I] --------- Setup audit user ---------","info")
			self.create_rangerdb_user(audit_db_root_user, db_user, db_password, audit_db_root_password)
			self.create_rangerdb_user(audit_db_root_user, audit_db_user, audit_db_password, audit_db_root_password)
			self.create_db(audit_db_root_user, audit_db_root_password, audit_db_name, db_user, db_password)

		if DBA_MODE == "TRUE":
			self.grant_xa_db_user(audit_db_root_user, audit_db_name, db_user, db_password, audit_db_root_password, True)


class SqlServerConf(BaseDB):
	# Constructor
	def __init__(self, host, SQL_CONNECTOR_JAR, JAVA_BIN):
		self.host = host
		self.SQL_CONNECTOR_JAR = SQL_CONNECTOR_JAR
		self.JAVA_BIN = JAVA_BIN


	def get_jisql_cmd(self, user, password, db_name):
		#TODO: User array for forming command
		jisql_cmd = "%s -cp %s:jisql/lib/* org.apache.util.sql.Jisql -user %s -password %s -driver mssql -cstring jdbc:sqlserver://%s:1433\\;databaseName=%s -noheader -trim"%(self.JAVA_BIN, self.SQL_CONNECTOR_JAR, user, password, self.host,db_name)
		return jisql_cmd

	def verify_user(self, root_user, db_root_password, db_user):
		log("[I] Verifying user " + db_user , "info")
		get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'msdb')
		query = get_cmd + " -c \; -query \"select loginname from master.dbo.syslogins where loginname = '%s';\"" %(db_user)
		output = check_output(shlex.split(query))
		if output.strip(db_user + " |"):
			return True
		else:
			return False

	def check_connection(self, db_name, db_user, db_password):
		log("[I] Checking connection", "info")
		get_cmd = self.get_jisql_cmd(db_user, db_password, db_name)
		query = get_cmd + " -c \; -query \"SELECT 1;\""
		output = check_output(shlex.split(query))
		if output.strip('1 |'):
			log("[I] Connection success", "info")
			return True
		else:
			log("[E] Can't establish connection", "error")
			sys.exit(1)

	def create_rangerdb_user(self, root_user, db_user, db_password, db_root_password):
		if self.check_connection('msdb', root_user, db_root_password):
			if self.verify_user(root_user, db_root_password, db_user):
				log("[I] SQL Server user " + db_user + " already exists!", "info")
			else:
				get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'msdb')
				log("[I] User does not exists, Creating Login user " + db_user, "info")
				query = get_cmd + " -c \; -query \"CREATE LOGIN %s WITH PASSWORD = '%s';\"" %(db_user,db_password)
				ret = subprocess.check_call(shlex.split(query))
				if ret == 0:
					if self.verify_user(root_user, db_root_password, db_user):
						 log("[I] SQL Server user " + db_user + " created", "info")
					else:
						log("[E] SQL Server user " +db_user+" creation failed", "error")
						sys.exit(1)
				else:
					log("[E] SQL Server user " +db_user+" creation failed", "error")
					sys.exit(1)


	def verify_db(self, root_user, db_root_password, db_name):
		log("[I] Verifying database " + db_name, "info")
		get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'msdb')
		query = get_cmd + " -c \; -query \"SELECT name from sys.databases where name='%s';\"" %(db_name)
		output = check_output(shlex.split(query))
		if output.strip(db_name + " |"):
			return True
		else:
			return False

	def create_db(self, root_user, db_root_password, db_name, db_user, db_password):
		if self.verify_db(root_user, db_root_password, db_name):
			log("[I] Database " + db_name + " already exists.","info")
		else:
			log("[I] Database does not exist! Creating database : " + db_name,"info")
			get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'msdb')
			query = get_cmd + " -c \; -query \"create database %s;\"" %(db_name)
			ret = subprocess.check_call(shlex.split(query))
			if ret != 0:
				log("[E] Database creation failed!!","error")
				sys.exit(1)
			else:
				if self.verify_db(root_user, db_root_password, db_name):
					self.create_user(root_user, db_name ,db_user, db_password, db_root_password)
					log("[I] Creating database " + db_name + " succeeded", "info")
					return True
#	        	               	self.import_db_file(db_name, root_user, db_user, db_password, db_root_password, file_name)
				else:
					log("[E] Database creation failed!!","error")
					sys.exit(1)


	def create_user(self, root_user, db_name ,db_user, db_password, db_root_password):
		get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'msdb')
		query = get_cmd + " -c \; -query \"USE %s SELECT name FROM sys.database_principals WHERE name = N'%s';\"" %(db_name, db_user)
		output = check_output(shlex.split(query))
		if output.strip(db_user + " |"):
			log("[I] User "+db_user+" exist ","info")
		else:
			query = get_cmd + " -c \; -query \"USE %s CREATE USER %s for LOGIN %s;\"" %(db_name ,db_user, db_user)
			ret = subprocess.check_call(shlex.split(query))
			if ret == 0:
				query = get_cmd + " -c \; -query \"USE %s SELECT name FROM sys.database_principals WHERE name = N'%s';\"" %(db_name ,db_user)
				output = check_output(shlex.split(query))
				if output.strip(db_user + " |"):
					log("[I] User "+db_user+" exist ","info")
				else:
					log("[E] Database creation failed!!","error")
					sys.exit(1)
			else:
				log("[E] Database creation failed!!","error")
				sys.exit(1)


	def grant_xa_db_user(self, root_user, db_name, db_user, db_password, db_root_password, True):
		log("[I] Granting permission to admin user '" + db_user + "' on db '" + db_name + "'" , "info")
		get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'msdb')
		query = get_cmd + " -c \; -query \"ALTER LOGIN [%s] WITH DEFAULT_DATABASE=[%s];\"" %(db_user, db_name)
		ret = subprocess.check_call(shlex.split(query))
		if ret != 0:
			sys.exit(1)
		query = get_cmd + " -c \; -query \" USE %s EXEC sp_addrolemember N'db_owner', N'%s';\"" %(db_name, db_user)
#                query = get_cmd + " -c \; -query \" USE %s GRANT ALL PRIVILEGES to %s;\"" %(db_name , db_user)
		ret = subprocess.check_call(shlex.split(query))
		if ret != 0:
			sys.exit(1)


	def create_auditdb_user(self, xa_db_host, audit_db_host, db_name, audit_db_name, xa_db_root_user, audit_db_root_user, db_user, audit_db_user, xa_db_root_password, audit_db_root_password, db_password, audit_db_password, DBA_MODE):
		if DBA_MODE == "TRUE":
			log("[I] --------- Setup audit user --------- ","info")
			self.create_rangerdb_user(audit_db_root_user, db_user, db_password, audit_db_root_password)
			#log("[I] --------- Setup audit user --------- ","info")
			self.create_rangerdb_user(audit_db_root_user, audit_db_user, audit_db_password, audit_db_root_password)
			self.create_db(audit_db_root_user, audit_db_root_password ,audit_db_name, audit_db_user, audit_db_password)
			self.create_user(xa_db_root_user, audit_db_name ,db_user, db_password, xa_db_root_password)
			self.grant_xa_db_user(audit_db_root_user, audit_db_name, db_user, db_password, audit_db_root_password, True)


def main(argv):
        
        quiteMode = False
        if len(argv) > 1 and str(argv[1]) == "-q":
           #print str(argv)
           quiteMode = True
	   populate_global_dict()

        print "Running DBA setup script. QuiteMode:" + str(quiteMode)
        
	FORMAT = '%(asctime)-15s %(message)s'
	logging.basicConfig(format=FORMAT, level=logging.DEBUG)

	DBA_MODE = 'TRUE'

        if (quiteMode):
	    JAVA_BIN=globalDict['JAVA_BIN']
	else:
            if os.environ['JAVA_HOME'] == "":
		log("[E] --------- JAVA_HOME environment property not defined, aborting installation! ---------", "error")
		sys.exit(1)
	    JAVA_BIN=os.environ['JAVA_HOME']+'/bin/java'
	    while os.path.isfile(JAVA_BIN) == False:
                log("Enter java executable path: :","info")
                JAVA_BIN=raw_input()
        #print "Using Java:" + str(JAVA_BIN)
       
        if (quiteMode):
	    XA_DB_FLAVOR=globalDict['DB_FLAVOR']
	    AUDIT_DB_FLAVOR=globalDict['DB_FLAVOR']
        else:
 	    XA_DB_FLAVOR=''
	    while XA_DB_FLAVOR == "":
	        log("Enter db flavour{MYSQL|ORACLE|POSTGRES|SQLSERVER} :","info")
                XA_DB_FLAVOR=raw_input()
	    AUDIT_DB_FLAVOR = XA_DB_FLAVOR
        XA_DB_FLAVOR = XA_DB_FLAVOR.upper()
	AUDIT_DB_FLAVOR = AUDIT_DB_FLAVOR.upper()
        #print "XA_DB_FLAVOR:" + str(XA_DB_FLAVOR)
        #print "AUDIT_DB_FLAVOR:" + str(AUDIT_DB_FLAVOR)

        if (quiteMode):
            CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
        else:
	    #CONNECTOR_JAR=''
	    if XA_DB_FLAVOR == "MYSQL" or XA_DB_FLAVOR == "ORACLE" or XA_DB_FLAVOR == "POSTGRES" or XA_DB_FLAVOR == "SQLSERVER":
	        log("Enter JDBC connector file for :"+XA_DB_FLAVOR,"info")
                CONNECTOR_JAR=raw_input()
                while os.path.isfile(CONNECTOR_JAR) == False:
	            log("JDBC connector file "+CONNECTOR_JAR+" does not exist, Please enter connector path :","error")
	            CONNECTOR_JAR=raw_input()
            else:
                log("[E] --------- NO SUCH SUPPORTED DB FLAVOUR!! ---------", "error")
                sys.exit(1)

        if (quiteMode):
	    xa_db_host = globalDict['db_host']
	    audit_db_host = globalDict['db_host']
	else:
            xa_db_host=''
	    while xa_db_host == "":
	        log("Enter DB Host :","info")
	        xa_db_host=raw_input()
	    audit_db_host=xa_db_host
        #print "xa_db_host:" + str(xa_db_host)
        #print "audit_db_host:" + str(audit_db_host)

	mysql_dbversion_catalog = 'db/mysql/create_dbversion_catalog.sql'
	#mysql_core_file = globalDict['mysql_core_file']
	mysql_core_file = 'db/mysql/xa_core_db.sql'
	#mysql_audit_file = globalDict['mysql_audit_file']
	mysql_audit_file = 'db/mysql/xa_audit_db.sql'
	mysql_patches = 'db/mysql/patches'

	oracle_dbversion_catalog = 'db/oracle/create_dbversion_catalog.sql'
	#oracle_core_file = globalDict['oracle_core_file'] 
	oracle_core_file = 'db/oracle/xa_core_db_oracle.sql'
	#oracle_audit_file = globalDict['oracle_audit_file'] 
	oracle_audit_file = 'db/oracle/xa_audit_db_oracle.sql' 
	oracle_patches = 'db/oracle/patches'

	postgres_dbversion_catalog = 'db/postgres/create_dbversion_catalog.sql'
	#postgres_core_file = globalDict['postgres_core_file']
	postgres_core_file = 'db/postgres/xa_core_db_postgres.sql'
	#postgres_audit_file = globalDict['postgres_audit_file']
	postgres_audit_file = 'db/postgres/xa_audit_db_postgres.sql'
	postgres_patches = 'db/postgres/patches'

	sqlserver_dbversion_catalog = 'db/sqlserver/create_dbversion_catalog.sql'
	#sqlserver_core_file = globalDict['sqlserver_core_file']
	sqlserver_core_file = 'db/sqlserver/xa_core_db_sqlserver.sql'
	#sqlserver_audit_file = globalDict['sqlserver_audit_file']
	sqlserver_audit_file = 'db/sqlserver/xa_audit_db_sqlserver.sql'
	sqlserver_patches = 'db/sqlserver/patches'

        
        if (quiteMode):
	    xa_db_root_user = globalDict['db_root_user']
	    xa_db_root_password = globalDict['db_root_password']
        else: 
	    xa_db_root_user=''
	    while xa_db_root_user == "":
                log("Enter db root user:","info")
                xa_db_root_user=raw_input()
	
	    log("Enter db root password:","info")
	    xa_db_root_password = getpass.getpass("Enter db root password:")

        if (quiteMode):
	    db_name = globalDict['db_name']
	else:
            db_name = ''
            while db_name == "":
                log("Enter DB Name :","info")
                db_name=raw_input()

        if (quiteMode):
	    db_user = globalDict['db_user']
        else: 
	    db_user=''
	    while db_user == "":
                log("Enter db user name:","info")
                db_user=raw_input()

        if (quiteMode):
	    db_password = globalDict['db_password']
        else:
	    db_password=''
	    while db_password == "":
                log("Enter db user password:","info")
                db_password = getpass.getpass("Enter db user password:")

	x_db_version = 'x_db_version_h'
	xa_access_audit = 'xa_access_audit'
	x_user = 'x_portal_user'

        if (quiteMode):
	    audit_db_name = globalDict['audit_db_name']
        else:
            audit_db_name=''
            while audit_db_name == "":
                log("Enter audit db name:","info")
                audit_db_name = raw_input()

        if (quiteMode):
	    audit_db_user = globalDict['audit_db_user']
        else:
            audit_db_user=''
            while audit_db_user == "":
                log("Enter audit user name:","info")
                audit_db_user = raw_input()

        if (quiteMode):
	    audit_db_password = globalDict['audit_db_password']
        else:
            audit_db_password=''
            while audit_db_password == "":		
                log("Enter audit db user password:","info")		
                audit_db_password = getpass.getpass("Enter audit db user password:")

	audit_db_root_user = xa_db_root_user	
	audit_db_root_password = xa_db_root_password
	#audit_db_root_user = globalDict['db_root_user'] 
	#audit_db_root_password = globalDict['db_root_password']
	#print "Enter audit_db_root_password :"
#	log("Enter audit db root user:","info")
#	audit_db_root_user = raw_input()
#	log("Enter db root password:","info")
#	xa_db_root_password = raw_input()

	if XA_DB_FLAVOR == "MYSQL":
		#MYSQL_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
		#MYSQL_CONNECTOR_JAR='/usr/share/java/mysql-connector-java.jar'
		MYSQL_CONNECTOR_JAR=CONNECTOR_JAR
		xa_sqlObj = MysqlConf(xa_db_host, MYSQL_CONNECTOR_JAR, JAVA_BIN)
		xa_db_version_file = os.path.join(os.getcwd(),mysql_dbversion_catalog)
		xa_db_core_file = os.path.join(os.getcwd(),mysql_core_file)
		xa_patch_file = os.path.join(os.getcwd(),mysql_patches)
		
	elif XA_DB_FLAVOR == "ORACLE":
#		ORACLE_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
		#ORACLE_CONNECTOR_JAR='/usr/share/java/ojdbc6.jar'
		ORACLE_CONNECTOR_JAR=CONNECTOR_JAR
		xa_db_root_user = xa_db_root_user+" AS SYSDBA"
		xa_sqlObj = OracleConf(xa_db_host, ORACLE_CONNECTOR_JAR, JAVA_BIN)
		xa_db_version_file = os.path.join(os.getcwd(),oracle_dbversion_catalog)
		xa_db_core_file = os.path.join(os.getcwd(),oracle_core_file)
		xa_patch_file = os.path.join(os.getcwd(),oracle_patches)

	elif XA_DB_FLAVOR == "POSTGRES":
#		POSTGRES_CONNECTOR_JAR = globalDict['SQL_CONNECTOR_JAR']
		#POSTGRES_CONNECTOR_JAR='/usr/share/java/postgresql.jar'
		POSTGRES_CONNECTOR_JAR=CONNECTOR_JAR
		xa_sqlObj = PostgresConf(xa_db_host, POSTGRES_CONNECTOR_JAR, JAVA_BIN)
		xa_db_version_file = os.path.join(os.getcwd(),postgres_dbversion_catalog)
		xa_db_core_file = os.path.join(os.getcwd(),postgres_core_file)
		xa_patch_file = os.path.join(os.getcwd(),postgres_patches)

	elif XA_DB_FLAVOR == "SQLSERVER":
#		SQLSERVER_CONNECTOR_JAR = globalDict['SQL_CONNECTOR_JAR']
		#SQLSERVER_CONNECTOR_JAR='/usr/share/java/sqljdbc4-2.0.jar'
		SQLSERVER_CONNECTOR_JAR=CONNECTOR_JAR
		xa_sqlObj = SqlServerConf(xa_db_host, SQLSERVER_CONNECTOR_JAR, JAVA_BIN)
		xa_db_version_file = os.path.join(os.getcwd(),sqlserver_dbversion_catalog)
		xa_db_core_file = os.path.join(os.getcwd(),sqlserver_core_file)
		xa_patch_file = os.path.join(os.getcwd(),sqlserver_patches)
	else:
		log("[E] --------- NO SUCH SUPPORTED DB FLAVOUR!! ---------", "error")
		sys.exit(1)

	if AUDIT_DB_FLAVOR == "MYSQL":
#		MYSQL_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
		#MYSQL_CONNECTOR_JAR='/usr/share/java/mysql-connector-java.jar'
		MYSQL_CONNECTOR_JAR=CONNECTOR_JAR
		audit_sqlObj = MysqlConf(audit_db_host,MYSQL_CONNECTOR_JAR,JAVA_BIN)
		audit_db_file = os.path.join(os.getcwd(),mysql_audit_file)

	elif AUDIT_DB_FLAVOR == "ORACLE":
		#ORACLE_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
		#ORACLE_CONNECTOR_JAR='/usr/share/java/ojdbc6.jar'
		ORACLE_CONNECTOR_JAR=CONNECTOR_JAR
		audit_db_root_user = audit_db_root_user+" AS SYSDBA"
		audit_sqlObj = OracleConf(audit_db_host, ORACLE_CONNECTOR_JAR, JAVA_BIN)
		audit_db_file = os.path.join(os.getcwd(),oracle_audit_file)

	elif AUDIT_DB_FLAVOR == "POSTGRES":
		#POSTGRES_CONNECTOR_JAR = globalDict['SQL_CONNECTOR_JAR']
		#POSTGRES_CONNECTOR_JAR='/usr/share/java/postgresql.jar'
		POSTGRES_CONNECTOR_JAR=CONNECTOR_JAR
		audit_sqlObj = PostgresConf(audit_db_host, POSTGRES_CONNECTOR_JAR, JAVA_BIN)
		audit_db_file = os.path.join(os.getcwd(),postgres_audit_file)

	elif AUDIT_DB_FLAVOR == "SQLSERVER":
		#SQLSERVER_CONNECTOR_JAR = globalDict['SQL_CONNECTOR_JAR']
		#SQLSERVER_CONNECTOR_JAR='/usr/share/java/sqljdbc4-2.0.jar'
		SQLSERVER_CONNECTOR_JAR=CONNECTOR_JAR
		audit_sqlObj = SqlServerConf(audit_db_host, SQLSERVER_CONNECTOR_JAR, JAVA_BIN)
		audit_db_file = os.path.join(os.getcwd(),sqlserver_audit_file)
	else:
		log("[E] --------- NO SUCH SUPPORTED DB FLAVOUR!! ---------", "error")
		sys.exit(1)

	# Methods Begin
	if DBA_MODE == "TRUE" :
		log("[I] --------- Creating Ranger Admin db user --------- ","info")
		xa_sqlObj.create_rangerdb_user(xa_db_root_user, db_user, db_password, xa_db_root_password)
		log("[I] --------- Creating Ranger Admin database ---------","info")
		xa_sqlObj.create_db(xa_db_root_user, xa_db_root_password, db_name, db_user, db_password)
		log("[I] --------- Granting permission to Ranger Admin db user ---------","info")
		xa_sqlObj.grant_xa_db_user(xa_db_root_user, db_name, db_user, db_password, xa_db_root_password, True)

		# Ranger Admin DB Host AND Ranger Audit DB Host are Different OR Same
		log("[I] --------- Verifying/Creating audit user --------- ","info")
		audit_sqlObj.create_auditdb_user(xa_db_host, audit_db_host, db_name, audit_db_name, xa_db_root_user, audit_db_root_user, db_user, audit_db_user, xa_db_root_password, audit_db_root_password, db_password, audit_db_password, DBA_MODE)
		log("[I] --------- Ranger Policy Manager DB and User Creation Process Completed..  --------- ","info")
main(sys.argv)
