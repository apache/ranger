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
import platform
import fileinput
import getpass
import shutil
from os.path import basename
from subprocess import Popen,PIPE
from datetime import date
from datetime import datetime
globalDict = {}

os_name = platform.system()
os_name = os_name.upper()

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
	read_config_file = open(os.path.join(os.getcwd(),'install.properties'))
	for each_line in read_config_file.read().split('\n') :
		if len(each_line) == 0 : continue
		if re.search('=', each_line):
			key , value = each_line.strip().split("=",1)
			key = key.strip()
			value = value.strip()
			globalDict[key] = value

def ModConfig(File, Variable, Setting):
	"""
	Modify Config file variable with new setting
	"""
	VarFound = False
	AlreadySet = False
	V=str(Variable)
	S=str(Setting)
	# use quotes if setting has spaces #
	if ' ' in S:
		S = '"%s"' % S

	for line in fileinput.input(File, inplace = 1):
		# process lines that look like config settings #
		if not line.lstrip(' ').startswith('#') and '=' in line:
			_infile_var = str(line.split('=')[0].rstrip(' '))
			_infile_set = str(line.split('=')[1].lstrip(' ').rstrip())
			# only change the first matching occurrence #
			if VarFound == False and _infile_var.rstrip(' ') == V:
				VarFound = True
				# don't change it if it is already set #
				if _infile_set.lstrip(' ') == S:
					AlreadySet = True
				else:
					line = "%s = %s\n" % (V, S)

		sys.stdout.write(line)

	# Append the variable if it wasn't found #
	if not VarFound:
		print "property '%s' not found.  Adding it to %s" % (V, File)
		with open(File, "a") as f:
			f.write("%s = %s\n" % (V, S))
	elif AlreadySet == True:
		print "property '%s' unchanged" % (V)
	else:
		print "property '%s' modified to '%s'" % (V, S)

	return

def main():

	FORMAT = '%(asctime)-15s %(message)s'
	logging.basicConfig(format=FORMAT, level=logging.DEBUG)
	populate_global_dict()

	SYNC_LDAP_BIND_KEYSTOREPATH=globalDict['CRED_KEYSTORE_FILENAME']
	SYNC_POLICY_MGR_ALIAS="policymgr.user.password"
	SYNC_POLICY_MGR_PASSWORD = ''
	SYNC_POLICY_MGR_USERNAME = ''
	JAVA_BIN = ''
	unix_user = "ranger"
	unix_group = "ranger"

	if os.environ['JAVA_HOME'] == "":
		log("[E] ---------- JAVA_HOME environment property not defined, aborting installation. ----------", "error")
		sys.exit(1)

	JAVA_BIN=os.path.join(os.environ['JAVA_HOME'],'bin','java')
	if os_name == "WINDOWS" :
		JAVA_BIN = JAVA_BIN+'.exe'
	if os.path.isfile(JAVA_BIN):
		pass
	else:
		while os.path.isfile(JAVA_BIN) == False:
			log("Enter java executable path: :","info")
			JAVA_BIN=raw_input()

	log("[I] Using Java:" + str(JAVA_BIN),"info")

	while SYNC_POLICY_MGR_USERNAME == "":
		print "Enter policymgr user name:"
		SYNC_POLICY_MGR_USERNAME=raw_input()

	while SYNC_POLICY_MGR_PASSWORD == "":
		SYNC_POLICY_MGR_PASSWORD=getpass.getpass("Enter policymgr user password:")

	if SYNC_LDAP_BIND_KEYSTOREPATH != "" or SYNC_POLICY_MGR_ALIAS != "" or SYNC_POLICY_MGR_USERNAME != "" or SYNC_POLICY_MGR_PASSWORD != "":
		log("[I] Storing policymgr usersync password in credential store:","info")
		cmd="%s -cp lib/* org.apache.ranger.credentialapi.buildks create %s -value %s  -provider jceks://file%s" %(JAVA_BIN,SYNC_POLICY_MGR_ALIAS,SYNC_POLICY_MGR_PASSWORD,SYNC_LDAP_BIND_KEYSTOREPATH)
		ret=subprocess.call(shlex.split(cmd))
		if ret == 0:
			cmd="chown %s:%s %s" %(unix_user,unix_group,SYNC_LDAP_BIND_KEYSTOREPATH)
			ret=subprocess.call(shlex.split(cmd))
			if ret == 0:
				CFG_FILE=os.path.join(os.getcwd(),'conf','unixauthservice.properties')
				NEW_CFG_FILE=os.path.join(os.getcwd(),'conf','unixauthservice.properties.tmp')
				if os.path.isfile(CFG_FILE):
					shutil.copyfile(CFG_FILE, NEW_CFG_FILE)
					ModConfig(NEW_CFG_FILE, "userSync.policyMgrUserName", SYNC_POLICY_MGR_USERNAME)
					ModConfig(NEW_CFG_FILE, "userSync.policyMgrKeystore", SYNC_LDAP_BIND_KEYSTOREPATH)
					ModConfig(NEW_CFG_FILE, "userSync.policyMgrAlias", SYNC_POLICY_MGR_ALIAS)
					now = datetime.now()
					shutil.copyfile(CFG_FILE, CFG_FILE+"."+now.strftime('%Y%m%d%H%M%S'))
					shutil.copyfile(NEW_CFG_FILE,CFG_FILE)
				else:
					log("[E] Required file not found: ["+CFG_FILE+"]","error")				
			else:
				log("[E] unable to execute command ["+cmd+"]","error")
		else:
			log("[E] unable to execute command ["+cmd+"]","error")
	else:
		log("[E] Input Error","error")


main()
