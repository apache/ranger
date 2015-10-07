#!/usr/bin/python
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
from xml.etree import ElementTree as ET
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

def import_properties_from_xml(xml_path, properties_from_xml=None):
	print('getting values from file : ' + str(xml_path))
	if os.path.isfile(xml_path):
		xml = ET.parse(xml_path)
		root = xml.getroot()
		if properties_from_xml is None:
			properties_from_xml = dict()
		for child in root.findall('property'):
			name = child.find("name").text.strip()
			value = child.find("value").text.strip() if child.find("value").text is not None  else ""
			properties_from_xml[name] = value
	else:
		print('XML file not found at path : ' + str(xml_path))
	return properties_from_xml

def write_properties_to_xml(xml_path, property_name='', property_value=''):
	if(os.path.isfile(xml_path)):
		xml = ET.parse(xml_path)
		root = xml.getroot()
		for child in root.findall('property'):
			name = child.find("name").text.strip()
			if name == property_name:
				child.find("value").text = property_value
		xml.write(xml_path)
		return 0
	else:
		return -1

def main():
	global globalDict
	FORMAT = '%(asctime)-15s %(message)s'
	logging.basicConfig(format=FORMAT, level=logging.DEBUG)

	CFG_FILE=os.path.join(os.getcwd(),'conf','ranger-tagsync-site.xml')
	if os.path.isfile(CFG_FILE):
		pass
	else:
		log("[E] Required file not found: ["+CFG_FILE+"]","error")
		sys.exit(1)

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

	globalDict=import_properties_from_xml(CFG_FILE,globalDict)
	TAGSYNC_KEYSTORE_FILENAME=globalDict['ranger.tagsync.tagadmin.keystore']
	log("[I] TAGSYNC_KEYSTORE_FILENAME:" + str(TAGSYNC_KEYSTORE_FILENAME),"info")
	TAGSYNC_TAGADMIN_ALIAS="tagadmin.user.password"
	TAGSYNC_TAGADMIN_PASSWORD = ''
	TAGSYNC_TAGADMIN_USERNAME = 'rangertagsync'
	unix_user = "ranger"
	unix_group = "ranger"

	while TAGSYNC_TAGADMIN_PASSWORD == "":
		TAGSYNC_TAGADMIN_PASSWORD=getpass.getpass("Enter tagadmin user password:")

	if TAGSYNC_KEYSTORE_FILENAME != "" or TAGSYNC_TAGADMIN_ALIAS != "" or TAGSYNC_TAGADMIN_USERNAME != "" or TAGSYNC_TAGADMIN_PASSWORD != "":
		log("[I] Storing tagadmin tagsync password in credential store:","info")
		cmd="%s -cp lib/* org.apache.ranger.credentialapi.buildks create %s -value %s  -provider jceks://file%s" %(JAVA_BIN,TAGSYNC_TAGADMIN_ALIAS,TAGSYNC_TAGADMIN_PASSWORD,TAGSYNC_KEYSTORE_FILENAME)
		ret=subprocess.call(shlex.split(cmd))
		if ret == 0:
			cmd="chown %s:%s %s" %(unix_user,unix_group,TAGSYNC_KEYSTORE_FILENAME)
			ret=subprocess.call(shlex.split(cmd))
			if ret == 0:
				if os.path.isfile(CFG_FILE):
					write_properties_to_xml(CFG_FILE,"ranger.tagsync.tagadmin.keystore",TAGSYNC_KEYSTORE_FILENAME)
					write_properties_to_xml(CFG_FILE,"ranger.tagsync.tagadmin.alias", TAGSYNC_TAGADMIN_ALIAS)
				else:
					log("[E] Required file not found: ["+CFG_FILE+"]","error")
			else:
				log("[E] unable to execute command ["+cmd+"]","error")
		else:
			log("[E] unable to execute command ["+cmd+"]","error")
	else:
		log("[E] Input Error","error")

main()
