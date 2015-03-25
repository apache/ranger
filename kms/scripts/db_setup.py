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
from datetime import date
from xml.etree import ElementTree as ET
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

def populate_global_dict():
    global globalDict
    os.chdir("..")
    read_config_file = open(os.path.join(os.getcwd(),'ews/webapp/config/dbks-site.xml'))
    dbks_site_properties = import_properties_from_xml(read_config_file, globalDict)

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

def import_properties_from_xml(xml_path, properties_from_xml=None):
	xml = ET.parse(xml_path)
	root = xml.getroot()
	if properties_from_xml is None:
		properties_from_xml = dict()
	for child in root.findall('property'):
		name = child.find("name").text.strip()
		value = child.find("value").text.strip() if child.find("value").text is not None else ""
		properties_from_xml[name] = value
	return properties_from_xml


def main():
	populate_global_dict()
	JAVA_BIN=globalDict['ranger.db.ks.java.bin']
	XA_DB_FLAVOR=globalDict['ranger.db.ks.database.flavor']
	XA_DB_FLAVOR.upper()
	
	xa_db_host = globalDict['ranger.db.host']

	mysql_core_file = globalDict['ranger.db.ks.core.file']

	db_name = globalDict['ranger.db.ks.name']
	db_user = globalDict['ranger.db.ks.javax.persistence.jdbc.user']
	db_password = globalDict['ranger.db.ks.javax.persistence.jdbc.password']
	xa_db_root_user = globalDict['ranger.db.root.user']
	xa_db_root_password = globalDict['ranger.db.root.password']

	if XA_DB_FLAVOR == "MYSQL":
		MYSQL_CONNECTOR_JAR=globalDict['ranger.db.ks.sql.connector.jar']		
		xa_sqlObj = MysqlConf(xa_db_host,MYSQL_CONNECTOR_JAR,JAVA_BIN)
		xa_db_core_file_script = os.path.join(os.getcwd(),'scripts')
                xa_db_core_file = os.path.join(xa_db_core_file_script,mysql_core_file)
	else:
		log ("--------- NO SUCH FLAVOUR ---------", "info")
		sys.exit(1)

	# Methods Begin
	log("\n--------- Creating kms user --------- \n","info")
	xa_sqlObj.create_rangerdb_user(xa_db_root_user, db_user, db_password, xa_db_root_password)
	log("\n--------- Importing DB Core Database ---------\n","info")	
	xa_sqlObj.import_file_to_db(xa_db_root_user, db_name, db_user, db_password, xa_db_root_password, xa_db_core_file)
	xa_sqlObj.grant_xa_db_user(xa_db_root_user, db_name, db_user, db_password, xa_db_root_password, True)

main()

