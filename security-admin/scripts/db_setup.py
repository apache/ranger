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
from os.path import basename
#import commands
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

#'''
def populate_global_dict():
    global globalDict
    #RANGER_ADMIN_HOME = os.getenv("RANGER_ADMIN_HOME")
    read_config_file = open(os.path.join(os.getcwd(),'install.properties'))
    #library_path = os.path.join(RANGER_ADMIN_HOME,"cred","lib","*")
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
#'''		
#----------------------------------------------		
class BaseDB(object):

	def init_logfiles(self):
		FORMAT = '%(asctime)-15s %(message)s'
		logging.basicConfig(format=FORMAT, level=logging.DEBUG)	

	def create_rangerdb_user(self, root_user, db_user, db_password, db_root_password):	
		log("---------- Creating User ----------", "info")

	def check_table(self, db_name, root_user, db_root_password, TABLE_NAME):
		log("---------- Verifying table ----------", "info")
	def import_file_to_db(self, root_user, db_name, db_user, db_password, db_root_password, file_name):	
		log("---------- Importing db schema ----------", "info")

	def upgrade_db(self, db_name, root_user, db_user, db_password, db_root_password, DBVERSION_CATALOG_CREATION):
		log("\nCreating Baseline DB upgrade ... \n", "debug")
		self.import_db_file(db_name, root_user, db_user, db_password, db_root_password, DBVERSION_CATALOG_CREATION)
		log("\nBaseline DB upgraded successfully\n", "info")

	def apply_patches(self, db_name, root_user, db_user, db_password ,db_root_password, PATCHES_PATH):
		#first get all patches and then apply each patch
		if not os.path.exists(PATCHES_PATH):
			log("No Patches to apply.","info")
		else:
			files = os.listdir(PATCHES_PATH)
			# files: coming from os.listdir() sorted alphabetically, thus not numerically
			if files:
				sorted_files = sorted(files, key=lambda x: str(x.split('.')[0]))
				for filename in sorted_files:
					currentPatch = PATCHES_PATH + "/"+filename
					self.import_db_patches(db_name, root_user, db_user, db_password ,db_root_password, currentPatch)
			else:
				log("No Patches to apply.","info")
	
	def create_auditdb_user(self, xa_db_host , audit_db_host , db_name ,audit_db_name, xa_db_root_user, audit_db_root_user, db_user, audit_db_user, xa_db_root_password, audit_db_root_password, db_password, audit_db_password, file_name , TABLE_NAME):
		log("----------------- Create Audit User ------------", "info")



class MysqlConf(BaseDB):
	# Constructor
	def __init__(self, host,SQL_CONNECTOR_JAR,JAVA_BIN):
		self.host = host
		self.SQL_CONNECTOR_JAR = SQL_CONNECTOR_JAR
		self.JAVA_BIN = JAVA_BIN
		BaseDB.init_logfiles(self)

	def get_jisql_cmd(self, user, password ,db_name):
		#TODO: User array for forming command
		jisql_cmd = "%s -cp %s:jisql/lib/* org.apache.util.sql.Jisql -driver mysqlconj -cstring jdbc:mysql://%s/%s -u %s -p %s -noheader -trim -c \;" %(self.JAVA_BIN,self.SQL_CONNECTOR_JAR,self.host,db_name,user,password)
		return jisql_cmd

		
	def create_rangerdb_user(self, root_user, db_user, db_password, db_root_password):
		hosts_arr =["%", "localhost"]
		for host in hosts_arr:
			get_cmd = self.get_jisql_cmd(root_user, db_root_password ,'mysql')
			query = get_cmd + " -query \"select user from mysql.user where user='%s' and host='%s';\"" %(db_user,host)
			output = check_output(shlex.split(query))
			if output.strip(db_user + " |"):
				log( "\nMYSQL User: " + db_user + " already exists!", "debug")
			else:
				log("User does not exists", "info")
				if db_password == "":
					log ("Creating MySQL user: "+ db_user +" with DB password blank\n", "info")
					query = get_cmd + " -query \"create user '%s'@'%s';\"" %(db_user, host)
					ret = subprocess.check_call(shlex.split(query))
					if ret == 0:
						query = get_cmd + " -query \"select user from mysql.user where user='%s' and host='%s';\"" %(db_user,host)
	       	               			output = check_output(shlex.split(query))
						if output.strip(db_user + " |"):
							log("Mysql user " + db_user +" created","info")	
						else:
							log("Creating Mysql user " + db_user +" Failed","info")	
							sys.exit(1)
				else:
					log ("Creating MySQL user: "+ db_user +" with DB password\n", "info")
					query = get_cmd + " -query \"create user '%s'@'%s' identified by '%s';\"" %(db_user, host, db_password)
					ret = subprocess.check_call(shlex.split(query))
					if ret == 0:
						log("Mysql user " + db_user +" created","info")	
					else:
						log("Creating Mysql user " + db_user +" Failed","info")	
						sys.exit(1)

        def verify_db(self, root_user, db_root_password, db_name):
                log("\nVerifying Database: " + db_name + "\n", "debug")
                get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'mysql')
                query = get_cmd + " -query \"show databases like '%s';\"" %(db_name)
                output = check_output(shlex.split(query))
                if output.strip(db_name + " |"):
                        return True
                else:
                        return False


        def import_file_to_db(self, root_user, db_name, db_user, db_password, db_root_password, file_name):
                log ("\nImporting db schema to Database: " + db_name,"debug");
                if self.verify_db(root_user, db_root_password, db_name):
                        log("\nDatabase: "+db_name + " already exists. Ignoring import_db\n","info")
                else:
                        log("\nDatabase does not exist. Creating database : " + db_name,"info")
	                get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'mysql')
                        query = get_cmd + " -query \"create database %s;\"" %(db_name)
			ret = subprocess.check_call(shlex.split(query))
			if ret != 0:
				log("\nDatabase creation failed!!\n","info")
				sys.exit(1)
        		else:
				if self.verify_db(root_user, db_root_password, db_name):
           				log("Creating database: " + db_name + " succeeded", "info")
					self.import_db_file(db_name, root_user , db_user, db_password, db_root_password, file_name)
				else:
					log("\nDatabase creation failed!!\n","info")
					sys.exit(1)


	def grant_xa_db_user(self, root_user, db_name, db_user, db_password, db_root_password , is_revoke):
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
			log ("---------------GRANTING PRIVILEGES TO user '"+db_user+"'@'"+host+"' on db '"+db_name+"'-------------" , "info")
			get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'mysql')
			query = get_cmd + " -query \"grant all privileges on %s.* to '%s'@'%s' with grant option;\"" %(db_name,db_user, host)
			ret = subprocess.check_call(shlex.split(query))
			if ret == 0:
				log ("---------------FLUSH PRIVILEGES -------------" , "info")
				query = get_cmd + " -query \"FLUSH PRIVILEGES;\""
				ret = subprocess.check_call(shlex.split(query))
				if ret == 0:
					log("Privileges granted to '" + db_user + "' on '"+db_name+"'\n", "info")
				else:
					log("Granting privileges to '" +db_user+"' FAILED on '"+db_name+"'\n", "info")
					sys.exit(1)
			else:
				log("\nGranting privileges to '" +db_user+"' FAILED on '"+db_name+"'\n", "info")
				sys.exit(1)
	

	def grant_audit_db_user(self, audit_root_user, audit_db_name, audit_db_user, audit_db_password, audit_db_root_password,TABLE_NAME, is_revoke):
		hosts_arr =["%", "localhost"]
		if is_revoke == True:
			for host in hosts_arr:
				get_cmd = self.get_jisql_cmd(audit_root_user, audit_db_root_password, 'mysql')
				query = get_cmd + " -query \"REVOKE ALL PRIVILEGES,GRANT OPTION FROM '%s'@'%s';\"" %(audit_db_user, host)
				ret = subprocess.check_call(shlex.split(query))
				if ret == 0:
					query = get_cmd + " -query \"FLUSH PRIVILEGES;\""
					ret = subprocess.check_call(shlex.split(query))
					if ret != 0:
						sys.exit(1)
				else:
					sys.exit(1)
		
		for host in hosts_arr:
			log ("---------------GRANTING PRIVILEGES TO '"+ audit_db_user + "' on '" + audit_db_name+"'-------------" , "info")
			get_cmd = self.get_jisql_cmd(audit_root_user, audit_db_root_password, 'mysql')
			query = get_cmd + " -query \"GRANT INSERT ON %s.%s TO '%s'@'%s';\"" %(audit_db_name,TABLE_NAME,audit_db_user,host)
			ret = subprocess.check_call(shlex.split(query))
			if ret == 0:
				get_cmd = self.get_jisql_cmd(audit_root_user, audit_db_root_password, 'mysql')
				query = get_cmd + " -query \"FLUSH PRIVILEGES;\""
				ret = subprocess.check_call(shlex.split(query))
				if ret == 0:
					log("Granting privileges to '" + audit_db_user+"' Done on '"+ audit_db_name+"'\n", "info")
				else:
					log("Granting privileges to '" +audit_db_user+"' Failed on '" + audit_db_name+"'\n", "info")
					sys.exit(1)
			else:
				log("\nGranting privileges to '" + audit_db_user+"' Failed on '" + audit_db_name+"'\n", "info")
				sys.exit(1)
	

	def import_db_file(self, db_name, root_user, db_user, db_password, db_root_password, file_name):
		name = basename(file_name)
		if os.path.isfile(file_name):
			log("Importing db schema to database : " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(root_user, db_root_password, db_name)
			query = get_cmd + " -input %s" %file_name
			ret = subprocess.check_call(shlex.split(query))
			if ret == 0:
				log(name + " DB schema imported successfully\n","info")
			else:
				log(name + " DB Schema import failed!\n","info")
				sys.exit(1)
		else:
		    log("\nDB Schema file " + name+ " not found\n","exception")
		    sys.exit(1)

	def import_db_patches(self, db_name, root_user, db_user, db_password, db_root_password, file_name):
		name = basename(file_name)
		if os.path.isfile(file_name):
			version = name.split('-')[0]
			log("Executing patch on : " + db_name + " from file: " + name,"info")
			get_cmd = self.get_jisql_cmd(root_user, db_root_password, db_name)
			query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
			output = check_output(shlex.split(query))
			if output.strip(version + " |"):
				log("Patch "+ name  +" is already Applied" ,"info")	
			else:
				get_cmd = self.get_jisql_cmd(root_user, db_root_password, db_name)
				query = get_cmd + " -input %s" %file_name
				ret = subprocess.check_call(shlex.split(query))
				if ret == 0:
					log(name + " Patch Applied\n","info")
					query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by) values ('%s', now(), user(), now(), user()) ;\"" %(version)
                                        ret = subprocess.check_call(shlex.split(query))
                                        if ret == 0:
                                                log("Patch version updated", "info")
                                        else:
                                                log("Updating Patch version failed", "info")
                                                sys.exit(1)
				else:
					log(name + "\n Import failed!\n","info")
					sys.exit(1)
		else:
		    log("\nImport " +name + " file not found\n","exception")
		    sys.exit(1)
	

	def check_table(self, db_name, root_user, db_root_password, TABLE_NAME):
                if self.verify_db(root_user, db_root_password, db_name):
			log("Verifying table " + TABLE_NAME +" in database " + db_name, "debug")

			get_cmd = self.get_jisql_cmd(root_user, db_root_password, db_name)
			query = get_cmd + " -query \"show tables like '%s';\"" %(TABLE_NAME)
			output = check_output(shlex.split(query))	
			if output.strip(TABLE_NAME + " |"):
				log("Table " + TABLE_NAME +" already exists in  database " + db_name + "\n","info")
				return True 
			else:
				log("Table " + TABLE_NAME +" does not exist in database " + db_name + "\n","info")
				return False
		else:
			log("Database " + db_name +" does not exist\n","info")
			return False


	def create_auditdb_user(self, xa_db_host, audit_db_host, db_name, audit_db_name, xa_db_root_user, audit_db_root_user, db_user, audit_db_user, xa_db_root_password, audit_db_root_password, db_password, audit_db_password, file_name , TABLE_NAME):
                hosts_arr =["%", "localhost"]
                for host in hosts_arr:
                        get_cmd = self.get_jisql_cmd(audit_db_root_user, audit_db_root_password ,'mysql')
                        query = get_cmd + " -query \"select user from mysql.user where user='%s' and host='%s';\"" %(audit_db_user,host)
                        output = check_output(shlex.split(query))
                        if output.strip(audit_db_user + " |"):
                                log( "\nMYSQL User: " + audit_db_user + " already exists!", "debug")
                        else:
                                log("User does not exists", "info")
                                if audit_db_password == "":
                                        log ("Creating MySQL user: "+ audit_db_user +" with DB password blank\n", "info")
                                        query = get_cmd + " -query \" create user '%s'@'%s';\"" %(audit_db_user, host)
                                        ret = subprocess.check_call(shlex.split(query))
                                else:
                                        log ("Creating MySQL user: "+ audit_db_user +" with DB password\n", "info")
                                        query = get_cmd + " -query \"create user '%s'@'%s' identified by '%s';\"" %(audit_db_user,host,audit_db_password)
                                        ret = subprocess.check_call(shlex.split(query))
					if ret == 0:
                                		log( "\nMYSQL User: " + audit_db_user + " Created", "info")
					else:
						sys.exit(1)

			query = get_cmd + " -query \"REVOKE ALL PRIVILEGES,GRANT OPTION FROM '%s'@'%s';\"" %(audit_db_user, host)
			ret = subprocess.check_call(shlex.split(query))
			if ret == 0:
				query = get_cmd + " -query \"FLUSH PRIVILEGES;\""
				ret = subprocess.check_call(shlex.split(query))
			else:
				sys.exit(1)

		log("\n--------- Check audit table exists --------- \n","info")
		output = self.check_table(audit_db_name,audit_db_root_user,audit_db_root_password,TABLE_NAME)	
		if output == False:
			self.import_file_to_db(audit_db_root_user, audit_db_name ,audit_db_user, audit_db_password, audit_db_root_password, file_name)			
		self.grant_xa_db_user(audit_db_root_user, audit_db_name, db_user, db_password, audit_db_root_password, False)
		if audit_db_user == db_user:
			is_revoke = False
		else:
			is_revoke = True
		self.grant_audit_db_user(audit_db_root_user, audit_db_name, audit_db_user, audit_db_password, audit_db_root_password,TABLE_NAME, is_revoke)



class OracleConf(BaseDB):
	# Constructor
	def __init__(self, host,SQL_CONNECTOR_JAR,JAVA_BIN):
		self.host = host 
		self.SQL_CONNECTOR_JAR = SQL_CONNECTOR_JAR
		self.JAVA_BIN = JAVA_BIN
		BaseDB.init_logfiles(self)

	def get_jisql_cmd(self, user, password):
		#TODO: User array for forming command
		jisql_cmd = "%s -cp %s:jisql/lib/* org.apache.util.sql.Jisql -driver oraclethin -cstring jdbc:oracle:thin:@%s -u '%s' -p '%s' -noheader -trim" %(self.JAVA_BIN,self.SQL_CONNECTOR_JAR,self.host, user, password)
		return jisql_cmd

		
	def create_rangerdb_user(self, root_user, db_user, db_password, db_root_password):
		get_cmd = self.get_jisql_cmd(root_user, db_root_password)
		query = get_cmd + " -c \; -query \"select username from all_users where UPPER(username)=UPPER('%s');\"" %(db_user)
		output = check_output(shlex.split(query))
		if output.strip(db_user+" |"):			
			log( "Oracle User: " + db_user + " already exists!", "debug")
		else:
			log("User does not exists, Creating User : " + db_user, "info")
			query = get_cmd + " -c \; -query 'create user %s identified by \"%s\";'" %(db_user, db_password)
			ret = subprocess.check_call(shlex.split(query))
			if ret == 0:
				query = get_cmd + " -c \; -query \"select username from all_users where UPPER(username)=UPPER('%s');\"" %(db_user)
				output = check_output(shlex.split(query))
				if output.strip(db_user+" |"):
					log("Granting Permission to " + db_user, "info")
					query = get_cmd + " -c \; -query 'GRANT CREATE SESSION,CREATE PROCEDURE,CREATE TABLE,CREATE VIEW,CREATE SEQUENCE,CREATE PUBLIC SYNONYM,CREATE TRIGGER,UNLIMITED Tablespace TO %s WITH ADMIN OPTION;'" % (db_user)
					ret = subprocess.check_call(shlex.split(query))
					if ret == 0:
						log("\nGranting permissions to oracle user '" + db_user + "' for %s DONE\n" %(self.host), "info")
					else:
						log("\nGranting permissions to oracle user '" + db_user + "' FAILED\n", "info")
						sys.exit(1)
				else:
					log("\nCreating ORACLE user '" + db_user + "' FAILED\n", "info")
					sys.exit(1)


	def verify_user(self, root_user, db_user, db_root_password):
		log("Verifying User: " + db_user +"\n","debug")
		get_cmd = self.get_jisql_cmd(root_user, db_root_password)		
		query = get_cmd + " -c \; -query \"select count(*) from all_users where upper(username)=upper('%s');\"" %(db_user)
		output = check_output(shlex.split(query))
		if output.strip(db_user+" |"):
			return True
		else:
			return False
	

	def verify_tablespace(self, root_user,db_user, db_root_password, db_name):
		log("Verifying Tablespace: " + db_name+"\n","debug")
		get_cmd = self.get_jisql_cmd(root_user, db_root_password)		
		query = get_cmd + " -c \; -query \"SELECT DISTINCT UPPER(TABLESPACE_NAME) FROM USER_TablespaceS where UPPER(Tablespace_Name)=UPPER(\'%s\');\"" %(db_name)
		output = check_output(shlex.split(query))
		if output.strip(db_name+' |'):
			return True
		else:
			return False


	def import_file_to_db(self, root_user ,db_name, db_user, db_password, db_root_password,file_name):
		#Verifying Users
		if self.verify_user(root_user,db_user, db_root_password):
			log("User : " +db_user + " already exists.", "info")
		else:
			log("User does not exist " + db_user, "info")
			sys.exit(1)

		if self.verify_tablespace(root_user, db_user, db_root_password, db_name):
			log("Tablespace " + db_name + " already exists.","info")
			if re.search('xa_core_db' , file_name):
				status = False	
			else: 
				status = True
		else:
			log("Tablespace does not exist. Creating Tablespace: " + db_name,"info")	
		        get_cmd = self.get_jisql_cmd(root_user, db_root_password)
			query = get_cmd + " -c \; -query \"create tablespace %s datafile '%s.dat' size 10M autoextend on;\"" %(db_name, db_name)
			ret = subprocess.check_call(shlex.split(query))
			if ret != 0:
			    log("Tablespace creation failed!!\n","exception"	)
			    sys.exit(1)
        		else:
           			log("Creating Tablespace "+db_name+" succeeded", "info")    		
				status = self.verify_tablespace(root_user, db_user, db_root_password, db_name)
				if status == False:
					sys.exit(1)

		if status == True:
			log("ASSIGN DEFAULT Tablespace :" +db_name , "info")
			# ASSIGN DEFAULT Tablespace db_name	
			get_cmd = self.get_jisql_cmd(root_user , db_root_password)
			query = get_cmd +" -c \; -query 'alter user %s identified by \"%s\" DEFAULT Tablespace %s;'" %(db_user, db_password, db_name)
			ret = subprocess.check_call(shlex.split(query))
			if ret == 0:
                                log("Granting Permission to " + db_user, "info")
                                query = get_cmd + " -c \; -query 'GRANT CREATE SESSION,CREATE PROCEDURE,CREATE TABLE,CREATE VIEW,CREATE SEQUENCE,CREATE PUBLIC SYNONYM,CREATE TRIGGER,UNLIMITED Tablespace TO %s WITH ADMIN OPTION;'" % (db_user)
				ret = subprocess.check_call(shlex.split(query))
				if ret == 0:
					self.import_db_file(db_name, root_user ,db_user, db_password, db_root_password ,file_name)
					return True
				else:
					log("\nGranting Oracle user '" + db_user + "' FAILED\n", "info")
					sys.exit(1)
			else:
				return False

	def import_audit_file_to_db(self, audit_db_root_user, db_name ,audit_db_name, db_user, audit_db_user, db_password, audit_db_password, audit_db_root_password, file_name, TABLE_NAME):			
		#Verifying Users
		if self.verify_user(audit_db_root_user, db_user, audit_db_root_password):
			log("User : " +db_user + " already exists.", "info")
		else:
			log("User does not exist " + db_user, "info")
			sys.exit(1)

		if self.verify_user(audit_db_root_user, audit_db_user, audit_db_root_password):
			log("User : " +audit_db_user + " already exists.", "info")
		else:
			log("User does not exist " + audit_db_user, "info")
			sys.exit(1)

		if self.verify_tablespace(audit_db_root_user, db_user, audit_db_root_password, audit_db_name):
			log("\nTablespace " + audit_db_name + " already exists.","info")
			status1 = True
		else:
			log("\nTablespace does not exist. Creating Tablespace: \n" + audit_db_name,"info")	
		        get_cmd = self.get_jisql_cmd(audit_db_root_user, audit_db_root_password)
			query = get_cmd + " -c \; -query \"create tablespace %s datafile '%s.dat' size 10M autoextend on;\"" %(audit_db_name, audit_db_name)
			ret = subprocess.check_call(shlex.split(query))
			if ret != 0:
			    log("\nTablespace creation failed!!\n","info")
			    sys.exit(1)
        		else:
           			log("Creating Tablespace "+ audit_db_name + " succeeded", "info")    		
				status1 = True

		if self.verify_tablespace(audit_db_root_user, db_user, audit_db_root_password, db_name):
			log("Tablespace " + db_name + " already exists.","info")
			status2 = True
		else:
			log("Tablespace does not exist. Creating Tablespace: " + db_name,"info")	
		        get_cmd = self.get_jisql_cmd(audit_db_root_user, audit_db_root_password)
			query = get_cmd + " -c \; -query \"create tablespace %s datafile '%s.dat' size 10M autoextend on;\"" %(db_name, db_name)
			ret = subprocess.check_call(shlex.split(query))
			if ret != 0:
			    log("\nTablespace creation failed!!\n","info")
			    sys.exit(1)
        		else:
           			log("Creating Tablespace "+ db_name + " succeeded", "info")    		
				status2 = True

		if (status1 == True and status2 == True):
			log("ASSIGN DEFAULT Tablespace :" + db_name , "info")
			# ASSIGN DEFAULT Tablespace db_name	
			get_cmd = self.get_jisql_cmd(audit_db_root_user , audit_db_root_password)
			query = get_cmd +" -c \; -query 'alter user %s identified by \"%s\" DEFAULT Tablespace %s;'" %(audit_db_user, audit_db_password, db_name)
			ret1 = subprocess.check_call(shlex.split(query))
 
			log("ASSIGN DEFAULT Tablespace :" + audit_db_name , "info")
			# ASSIGN DEFAULT Tablespace db_name	
			get_cmd = self.get_jisql_cmd(audit_db_root_user , audit_db_root_password)
			query = get_cmd +" -c \; -query 'alter user %s identified by \"%s\" DEFAULT Tablespace %s;'" %(audit_db_user, audit_db_password, audit_db_name)
			ret2 = subprocess.check_call(shlex.split(query))

			if (ret1 == 0 and ret2 == 0):
                                log("Granting Permission to " + db_user, "info")
                                query = get_cmd + " -c \; -query 'GRANT CREATE SESSION,CREATE PROCEDURE,CREATE TABLE,CREATE VIEW,CREATE SEQUENCE,CREATE PUBLIC SYNONYM,CREATE TRIGGER,UNLIMITED Tablespace TO %s WITH ADMIN OPTION;'" % (db_user)
				ret = subprocess.check_call(shlex.split(query))
				if ret == 0:
					
					if self.check_table(db_name, audit_db_root_user, audit_db_root_password, TABLE_NAME):	
						log("Table exists " + TABLE_NAME +" in tablespace " + db_name ,"info")
					else:
						self.import_db_file(audit_db_name, audit_db_root_user ,db_user, db_password, audit_db_root_password ,file_name)
                                	query = get_cmd + " -c \; -query 'GRANT CREATE SESSION TO %s;'" % (audit_db_user)
					ret = subprocess.check_call(shlex.split(query))
					if ret != 0:
						sys.exit(1)
                                	query = get_cmd + " -c \; -query 'GRANT SELECT ON %s.XA_ACCESS_AUDIT_SEQ TO %s;'" % (db_user,audit_db_user)
					ret = subprocess.check_call(shlex.split(query))
					if ret != 0:
						sys.exit(1)
                                	query = get_cmd + " -c \; -query 'GRANT INSERT ON %s.XA_ACCESS_AUDIT TO %s;'" % (db_user,audit_db_user)
					ret = subprocess.check_call(shlex.split(query))
					if ret != 0:
						sys.exit(1)
					return True
				else:
					log("\nGranting Oracle user '" + db_user + "' FAILED\n", "info")
					sys.exit(1)
			else:
				return False
					
	
	def grant_xa_db_user(self, root_user, db_name, db_user, db_password, db_root_password, invoke):
		 log("Granting Permission to " + db_user, "info")
		 get_cmd = self.get_jisql_cmd(root_user ,db_root_password)	
                 query = get_cmd + " -c \; -query 'GRANT CREATE SESSION,CREATE PROCEDURE,CREATE TABLE,CREATE VIEW,CREATE SEQUENCE,CREATE PUBLIC SYNONYM,CREATE TRIGGER,UNLIMITED Tablespace TO %s WITH ADMIN OPTION;'" % (db_user)
                 ret = subprocess.check_call(shlex.split(query))
                 if ret == 0:
			return True
		 else:
			log("\nGranting Oracle user '" + db_user + "' FAILED\n", "info")
			sys.exit(1)


	def grant_audit_db_user(self, audit_db_root_user, audit_db_name ,db_user,audit_db_user,db_password,audit_db_password, audit_db_root_password):
		 log("Granting Permission to " + db_user, "info")
		 get_cmd = self.get_jisql_cmd(audit_db_root_user, audit_db_root_password)
                 query = get_cmd + " -c \; -query 'GRANT CREATE SESSION,CREATE PROCEDURE,CREATE TABLE,CREATE VIEW,CREATE SEQUENCE,CREATE PUBLIC SYNONYM,CREATE TRIGGER,UNLIMITED Tablespace TO %s WITH ADMIN OPTION;'" % (db_user)
                 ret = subprocess.check_call(shlex.split(query))
                 if ret == 0:
			return True
		 else:
			log("\nGranting Oracle user '" + db_user + "' FAILED\n", "info")
			sys.exit(1)
	
	def import_db_file(self, db_name, root_user ,db_user, db_password, db_root_password,file_name):
		name = basename(file_name)
                if os.path.isfile(file_name):
                        log("Importing script : " + db_name + " from file: " + name,"info")
                        get_cmd = self.get_jisql_cmd(db_user, db_password)
                        query = get_cmd + " -input %s -c \;" %file_name
                        ret = subprocess.check_call(shlex.split(query))
                        if ret == 0:
                                log(name + " Imported successfully\n","info")
                        else:
                                log(name + " Import failed!\n","info")
                                sys.exit(1)
                else:
                    log("\nImport " +name + " sql file not found\n","debug")
                    sys.exit(1)
			
	def import_db_patches(self, db_name, root_user, db_user, db_password, db_root_password,file_name):
                if os.path.isfile(file_name):
			name = basename(file_name)
			version = name.split('-')[0]
                        log("Executing patch on : " + db_name + " from file: " + name,"info")
                        get_cmd = self.get_jisql_cmd(db_user, db_password)
			query = get_cmd + " -c \; -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
			output = check_output(shlex.split(query))
			if output.strip(version +" |"):
				log("Patch "+ name  +" is already Applied" ,"info")	
			else:
				get_cmd = self.get_jisql_cmd(db_user, db_password)
				query = get_cmd + " -input %s -c /" %file_name
				ret = subprocess.check_call(shlex.split(query))
				if ret == 0:
					log(name + " Patch Applied\n","info")
					query = get_cmd + " -c \; -query \"insert into x_db_version_h (id,version, inst_at, inst_by, updated_at, updated_by) values ( X_DB_VERSION_H_SEQ.nextval,'%s', sysdate, '%s', sysdate, '%s');\"" %(version, db_user, db_user)
					ret = subprocess.check_call(shlex.split(query))
					if ret == 0:
						log("Patch version updated", "info")
					else:
						log("Updating Patch version failed", "info")		
						sys.exit(1)
				else:
					log(name + "\n Import failed!\n","info")
					sys.exit(1)
                else:
                    log("\nPatch file not found\n","debug")
                    sys.exit(1)
			
	
	def check_table(self, db_name, root_user, db_root_password, TABLE_NAME):
		log("Verifying table " + TABLE_NAME +" in tablespace " + db_name, "debug")
		get_cmd = self.get_jisql_cmd(root_user, db_root_password)
		query = get_cmd + " -c \; -query \"select UPPER(table_name) from all_tables where UPPER(tablespace_name)=UPPER('%s') and UPPER(table_name)=UPPER('%s');\"" %(db_name ,TABLE_NAME)
		output = check_output(shlex.split(query))
		if output.strip(TABLE_NAME.upper() + ' |'):
			log("Table " + TABLE_NAME +" already exists in Tablespace " + db_name + "\n","info")
			return True
		else:
			log("Table " + TABLE_NAME +" does not exist in Tablespace " + db_name + "\n","info")
			return False


	def create_auditdb_user(self, xa_db_host , audit_db_host , db_name ,audit_db_name, xa_db_root_user, audit_db_root_user, db_user, audit_db_user, xa_db_root_password, audit_db_root_password, db_password, audit_db_password, file_name , TABLE_NAME):
		self.create_rangerdb_user(audit_db_root_user, db_user, db_password, audit_db_root_password)

		get_cmd = self.get_jisql_cmd(audit_db_root_user, audit_db_root_password)
                query = get_cmd + " -c \; -query \"select username from all_users where UPPER(username)=UPPER('%s');\"" %(audit_db_user)
                output = check_output(shlex.split(query))
                if output.strip(audit_db_user+" |"):
                        log( "Oracle Audit User: " + audit_db_user + " already exists!", "debug")
                else:
                        log("Audit User does not exists, Creating Audit User : " + audit_db_user, "info")
                        query = get_cmd + " -c \; -query 'create user %s identified by \"%s\";'" %(audit_db_user, audit_db_password)
                        ret = subprocess.check_call(shlex.split(query))
                        if ret == 0:
				query = get_cmd + " -c \; -query \"GRANT CREATE SESSION TO %s;\"" %(audit_db_user)
                        	ret = subprocess.check_call(shlex.split(query))
				if ret == 0:
					log("Granting Permission to " + audit_db_user + " Done", "info")
				else:
					log("Granting Permission to " + audit_db_user + " Failed", "info")
					sys.exit(1)
				

		log("\n--------- Check audit table exists --------- \n","info")
		output = self.check_table(audit_db_name, audit_db_root_user, audit_db_root_password, TABLE_NAME)	
		if output == False:
			self.import_audit_file_to_db(audit_db_root_user, db_name, audit_db_name, db_user, audit_db_user, db_password, audit_db_password, audit_db_root_password, file_name,TABLE_NAME)			
		self.grant_xa_db_user(audit_db_root_user, audit_db_name, db_user, db_password, audit_db_root_password, True)
		self.grant_audit_db_user(audit_db_root_user, audit_db_name ,db_user, audit_db_user, db_password,audit_db_password, audit_db_root_password)



class PostgresConf(BaseDB):
        # Constructor
	def __init__(self, host,SQL_CONNECTOR_JAR,JAVA_BIN):
                self.host = host
                self.SQL_CONNECTOR_JAR = SQL_CONNECTOR_JAR
                self.JAVA_BIN = JAVA_BIN
                BaseDB.init_logfiles(self)

	def get_jisql_cmd(self, user, password, db_name):
                #TODO: User array for forming command
                jisql_cmd = "%s -cp %s:jisql/lib/* org.apache.util.sql.Jisql -driver postgresql -cstring jdbc:postgresql://%s/%s -u %s -p %s -noheader -trim -c \;" %(self.JAVA_BIN,self.SQL_CONNECTOR_JAR,self.host, db_name, user, password)
                return jisql_cmd


	def create_rangerdb_user(self, root_user, db_user, db_password, db_root_password):
                get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'postgres')
                query = get_cmd + " -query \"SELECT rolname FROM pg_roles WHERE rolname='%s';\"" %(db_user)
                output = check_output(shlex.split(query))
                if output.strip(db_user+" |"):
			log( "Postgres User: " + db_user + " already exists!", "debug")
                else:
			log("User does not exists, Creating User : " + db_user, "info")
                        query = get_cmd + " -query \"CREATE USER %s WITH LOGIN PASSWORD '%s';\"" %(db_user, db_password)
                        ret = subprocess.check_call(shlex.split(query))
                        if ret == 0:
                                log("Postgres user " + db_user + " created", "info")
                        else:
                                log("Postgres user " +db_user+" creation failed\n", "info")
                                sys.exit(1)


        def verify_db(self, root_user, db_root_password, db_name):
                log("\nVerifying Database: " + db_name + "\n", "debug")
                get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'postgres')
                query = get_cmd + " -query \"SELECT datname FROM pg_database where datname='%s';\"" %(db_name)
                output = check_output(shlex.split(query))
                if output.strip(db_name + " |"):
                        return True
                else:
                        return False

	def import_file_to_db(self, root_user, db_name, db_user, db_password, db_root_password, file_name):
                log ("\nImporting to Database: " + db_name,"debug");
                if self.verify_db(root_user, db_root_password, db_name):
                        log("\nDatabase: "+db_name + " already exists. Ignoring import_db\n","info")
                else:
                        log("\nDatabase does not exist. Creating database : " + db_name,"info")
                        get_cmd = self.get_jisql_cmd(root_user, db_root_password, 'postgres')
                        query = get_cmd + " -query \"create database %s with OWNER %s;\"" %(db_name, db_user)
                        ret = subprocess.check_call(shlex.split(query))
                        if ret != 0:
                                log("\nDatabase creation failed!!","info")
                                sys.exit(1)
                        else:
                                log("Creating database : " + db_name + " succeeded", "info")
				self.import_db_file(db_name, root_user, db_user, db_password, db_root_password, file_name)


        def import_db_file(self, db_name, root_user, db_user, db_password, db_root_password, file_name):
                name = basename(file_name)
                if os.path.isfile(file_name):
                        log("Importing db schema to database : " + db_name + " from file: " + name,"info")
                        get_cmd = self.get_jisql_cmd(root_user, db_root_password, db_name)
                        query = get_cmd + " -input %s" %file_name
                        ret = subprocess.check_call(shlex.split(query))
                        if ret == 0:
                                log(name + " DB schema imported successfully\n","info")
                        else:
                                log(name + " DB Schema import failed!\n","info")
                                sys.exit(1)
                else:
                    log("\nDB Schema file " + name+ " not found\n","info")
                    sys.exit(1)

	def grant_xa_db_user(self, root_user, db_name, db_user, db_password, db_root_password , True):
		log ("GRANTING PRIVILEGES TO user '"+db_user+"' on db '"+db_name+"'" , "info")
		get_cmd = self.get_jisql_cmd(root_user, db_root_password, db_name)
		query = get_cmd + " -query \"GRANT ALL PRIVILEGES ON DATABASE %s to %s;\"" %(db_name, db_user)
		ret = subprocess.check_call(shlex.split(query))
		if ret != 0:
			log ("Ganting privileges on tables in schema public failed", "info")
			sys.exit(1)

		query = get_cmd + " -query \"GRANT ALL PRIVILEGES ON SCHEMA public TO %s;\"" %(db_user)
		ret = subprocess.check_call(shlex.split(query))
		if ret != 0:
			log ("Ganting privileges on schema public failed", "info")
			sys.exit(1)

		query = get_cmd + " -query \"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO %s;\"" %(db_user)
		ret = subprocess.check_call(shlex.split(query))
		if ret != 0:
			log ("Ganting privileges on database "+db_name+ " failed", "info")
			sys.exit(1)

		query = get_cmd + " -query \"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO %s;\"" %(db_user)
		ret = subprocess.check_call(shlex.split(query))
		if ret != 0:
			log ("Ganting privileges on database "+db_name+ " failed", "info")
			sys.exit(1)
		log ("GRANTING PRIVILEGES TO user '"+db_user+"' on db '"+db_name+"' Done" , "info")


        def grant_audit_db_user(self, audit_db_root_user, audit_db_name , db_user, audit_db_user, db_password, audit_db_password, audit_db_root_password):
		log("Granting Permission to " + audit_db_user, "info")
                get_cmd = self.get_jisql_cmd(audit_db_root_user, audit_db_root_password, audit_db_name)
                log("\nGranting Select privileges to Postgres Audit user '" + audit_db_user + "'", "info")
                query = get_cmd + " -query 'GRANT ALL ON XA_ACCESS_AUDIT_SEQ TO %s;'" % (audit_db_user)
                ret = subprocess.check_call(shlex.split(query))
                if ret != 0:
			log("\nGranting Select privileges to Postgres user '" + audit_db_user + "' FAILED\n", "info")
                        sys.exit(1)

		log("\nGranting insert privileges to Postgres Audit user '" + audit_db_user + "'\n", "info")
		query = get_cmd + " -query 'GRANT INSERT ON XA_ACCESS_AUDIT TO %s;'" % (audit_db_user)
                ret = subprocess.check_call(shlex.split(query))
                if ret != 0:
			log("\nGranting insert privileges to Postgres user '" + audit_db_user + "' FAILED\n", "info")
                        sys.exit(1)


        def import_db_patches(self, db_name, root_user, db_user, db_password, db_root_password, file_name):
                name = basename(file_name)
                if os.path.isfile(file_name):
                        version = name.split('-')[0]
                        log("Executing patch on : " + db_name + " from file: " + name,"info")
                        get_cmd = self.get_jisql_cmd(root_user, db_root_password, db_name)
                        query = get_cmd + " -query \"select version from x_db_version_h where version = '%s' and active = 'Y';\"" %(version)
                        output = check_output(shlex.split(query))
                        if output.strip(version + " |"):
                                log("Patch "+ name  +" is already Applied" ,"info")
                        else:
                                get_cmd = self.get_jisql_cmd(root_user, db_root_password, db_name)
                                query = get_cmd + " -input %s" %file_name
                                ret = subprocess.check_call(shlex.split(query))
                                if ret == 0:
                                        log(name + " Patch Applied\n","info")
                                        query = get_cmd + " -query \"insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by) values ('%s', now(), user(), now(), user()) ;\"" %(version)
                                        ret = subprocess.check_call(shlex.split(query))
                                        if ret == 0:
                                                log("Patch version updated", "info")
                                        else:
                                                log("Updating Patch version failed", "info")
                                                sys.exit(1)
                                else:
                                        log(name + "\n Import failed!\n","info")
                                        sys.exit(1)
                else:
                    log("\nImport " +name + " file not found\n","info")
                    sys.exit(1)


        def check_table(self, db_name, root_user, db_root_password, TABLE_NAME):
                if self.verify_db(root_user, db_root_password, db_name):
			log("Database: " + db_name + " exists","info")
                        log("Verifying table " + TABLE_NAME +" in database " + db_name, "debug")
                        get_cmd = self.get_jisql_cmd(root_user, db_root_password, db_name)
                        query = get_cmd + " -query \"select * from (select table_name from information_schema.tables where table_catalog='%s' and table_name = '%s') as temp;\"" %(db_name , TABLE_NAME)
                        output = check_output(shlex.split(query))
                        if output.strip(TABLE_NAME +" |"):
                                log("\nTable " + TABLE_NAME +" already exists in database " + db_name + "\n","info")
                                return True
                        else:
                                log("\nTable " + TABLE_NAME +" does not exist in database " + db_name + "\n","info")
                                return False
                else:
                        log("Database does not exist \n","info")
                        return False


	def create_auditdb_user(self, xa_db_host, audit_db_host, db_name, audit_db_name, xa_db_root_user, audit_db_root_user, db_user, audit_db_user, xa_db_root_password, audit_db_root_password, db_password, audit_db_password, file_name, TABLE_NAME):
		self.create_rangerdb_user(audit_db_root_user, db_user, db_password, audit_db_root_password)
		self.create_rangerdb_user(audit_db_root_user, audit_db_user, audit_db_password, audit_db_root_password)
		output = self.check_table(audit_db_name, audit_db_root_user, audit_db_root_password, TABLE_NAME)
		if output == False:
			self.import_file_to_db(audit_db_root_user, audit_db_name ,db_user, db_password, audit_db_root_password, file_name)
		self.grant_xa_db_user(audit_db_root_user, audit_db_name, db_user, db_password, audit_db_root_password, True)
		self.grant_audit_db_user(audit_db_root_user, audit_db_name ,db_user, audit_db_user, db_password,audit_db_password, audit_db_root_password)


def main():
	populate_global_dict()
	JAVA_BIN=globalDict['JAVA_BIN']
	XA_DB_FLAVOR=globalDict['DB_FLAVOR']
	AUDIT_DB_FLAVOR=globalDict['DB_FLAVOR']
	XA_DB_FLAVOR.upper()
	AUDIT_DB_FLAVOR.upper()
	
	xa_db_host = globalDict['db_host']
	audit_db_host = globalDict['db_host']

	mysql_dbversion_catalog = 'db/mysql/create_dbversion_catalog.sql'	
	mysql_core_file = globalDict['mysql_core_file']
	mysql_audit_file = globalDict['mysql_audit_file']
	mysql_patches = 'db/mysql/patches'

	oracle_dbversion_catalog = 'db/oracle/create_dbversion_catalog.sql'
	oracle_core_file = globalDict['oracle_core_file'] 
	oracle_audit_file = globalDict['oracle_audit_file'] 
	oracle_patches = 'db/oracle/patches'
	
	postgres_dbversion_catalog = 'db/postgres/create_dbversion_catalog.sql'
	postgres_core_file = globalDict['postgres_core_file']
	postgres_audit_file = globalDict['postgres_audit_file']
	postgres_patches = 'db/postgres/patches'

	db_name = globalDict['db_name']
	db_user = globalDict['db_user']
	db_password = globalDict['db_password']
	xa_db_root_user = globalDict['db_root_user']
	xa_db_root_password = globalDict['db_root_password']

	x_db_version = 'x_db_version_h'
	xa_access_audit = 'xa_access_audit'

	audit_db_name = globalDict['audit_db_name']
	audit_db_user = globalDict['audit_db_user']
	audit_db_password = globalDict['audit_db_password']
	audit_db_root_user = globalDict['db_root_user'] 
	audit_db_root_password = globalDict['db_root_password']

		
	

	if XA_DB_FLAVOR == "MYSQL":
		MYSQL_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']		
		xa_sqlObj = MysqlConf(xa_db_host,MYSQL_CONNECTOR_JAR,JAVA_BIN)
		xa_db_version_file = os.path.join(os.getcwd(),mysql_dbversion_catalog)
		xa_db_core_file = os.path.join(os.getcwd(),mysql_core_file)
		xa_patch_file = os.path.join(os.getcwd(),mysql_patches)
		
	elif XA_DB_FLAVOR == "ORACLE":
		ORACLE_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
		xa_db_root_user = xa_db_root_user+" AS SYSDBA"
		xa_sqlObj = OracleConf(xa_db_host,ORACLE_CONNECTOR_JAR,JAVA_BIN)
		xa_db_version_file = os.path.join(os.getcwd(),oracle_dbversion_catalog)
		xa_db_core_file = os.path.join(os.getcwd(),oracle_core_file)
		xa_patch_file = os.path.join(os.getcwd(),oracle_patches)
	elif XA_DB_FLAVOR == "POSTGRES":
		POSTGRES_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
		xa_sqlObj = PostgresConf(xa_db_host,POSTGRES_CONNECTOR_JAR,JAVA_BIN)
		xa_db_version_file = os.path.join(os.getcwd(),postgres_dbversion_catalog)
		xa_db_core_file = os.path.join(os.getcwd(),postgres_core_file)
		xa_patch_file = os.path.join(os.getcwd(),postgres_patches)
	else:
		log ("--------- NO SUCH FLAVOUR ---------", "info")
		sys.exit(1)

	if AUDIT_DB_FLAVOR == "MYSQL":
		MYSQL_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
		audit_sqlObj = MysqlConf(audit_db_host,MYSQL_CONNECTOR_JAR,JAVA_BIN)
		audit_db_file = os.path.join(os.getcwd(),mysql_audit_file)
		
	elif AUDIT_DB_FLAVOR == "ORACLE":
		ORACLE_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
		audit_db_root_user = audit_db_root_user+" AS SYSDBA"
		audit_sqlObj = OracleConf(audit_db_host,ORACLE_CONNECTOR_JAR,JAVA_BIN)
		audit_db_file = os.path.join(os.getcwd(),oracle_audit_file)
	elif AUDIT_DB_FLAVOR == "POSTGRES":
		POSTGRES_CONNECTOR_JAR=globalDict['SQL_CONNECTOR_JAR']
		audit_sqlObj = PostgresConf(audit_db_host,POSTGRES_CONNECTOR_JAR,JAVA_BIN)
		audit_db_file = os.path.join(os.getcwd(),postgres_audit_file)
	else:
		log ("--------- NO SUCH FLAVOUR ---------", "info")
		sys.exit(1)


	# Methods Begin
	log("\n--------- Creating admin user --------- \n","info")
	xa_sqlObj.create_rangerdb_user(xa_db_root_user, db_user, db_password, xa_db_root_password)
	log("\n--------- Importing DB Core Database ---------\n","info")
	xa_sqlObj.import_file_to_db(xa_db_root_user, db_name, db_user, db_password, xa_db_root_password, xa_db_core_file)
	xa_sqlObj.grant_xa_db_user(xa_db_root_user, db_name, db_user, db_password, xa_db_root_password, True)
	log("\n--------- Check Table ---------\n","info")
	output = xa_sqlObj.check_table(db_name, xa_db_root_user, xa_db_root_password, x_db_version)
	if output == False:
		log("\n--------- Updating Database ---------\n","info")
		xa_sqlObj.upgrade_db(db_name, xa_db_root_user, db_user, db_password, xa_db_root_password, xa_db_version_file)
	log("\n--------- Applying Patches ---------\n","info")
	xa_sqlObj.apply_patches(db_name, xa_db_root_user, db_user, db_password, xa_db_root_password, xa_patch_file)
	# Ranger Admin DB Host AND Ranger Audit DB Host are Different OR Same
	log("\n--------- Creating audit user --------- \n","info")
	audit_sqlObj.create_auditdb_user(xa_db_host, audit_db_host, db_name, audit_db_name, xa_db_root_user, audit_db_root_user, db_user, audit_db_user, xa_db_root_password, audit_db_root_password, db_password, audit_db_password, audit_db_file, xa_access_audit)

main()
