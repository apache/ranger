#!/usr/bin/python
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
import StringIO
import xml.etree.ElementTree as ET
import ConfigParser
import os,errno,sys,getopt
from os import listdir
from os.path import isfile, join, dirname, basename
from urlparse import urlparse
from time import gmtime, strftime, localtime
from xml import etree
import shutil
import pwd, grp

if (not 'JAVA_HOME' in os.environ):
	print "ERROR: JAVA_HOME environment variable is not defined. Please define JAVA_HOME before running this script"
	sys.exit(1)

debugLevel = 1
generateXML = 0
installPropDirName = '.'
pidFolderName = '/var/run/ranger'
logFolderName = '/var/log/ranger'
initdDirName = '/etc/init.d'

rangerBaseDirName = '/etc/ranger'
usersyncBaseDirName = 'usersync'
confBaseDirName = 'conf'
confDistBaseDirName = 'conf.dist'
certBaseDirName = 'cert'
defaultCertFileName = 'unixauthservice.jks'

outputFileName = 'ranger-ugsync-site.xml'
installPropFileName = 'install.properties'
defaultSiteXMLFileName = 'ranger-ugsync-default.xml'
log4jFileName          = 'log4j.xml'
install2xmlMapFileName = 'installprop2xml.properties'
templateFileName = 'ranger-ugsync-template.xml'
initdProgramName = 'ranger-usersync'
PROP2ALIASMAP = { 'ranger.usersync.ldap.ldapbindpassword':'ranger.usersync.ldap.bindalias', 
				   'ranger.usersync.keystore.password':'usersync.ssl.key.password',
				   'ranger.usersync.truststore.password':'usersync.ssl.truststore.password'}

installTemplateDirName = join(installPropDirName,'templates')
confDistDirName = join(installPropDirName, confDistBaseDirName)
ugsyncLogFolderName = join(logFolderName, 'ugsync')
nativeAuthFolderName = join(installPropDirName, 'native')
nativeAuthProgramName = join(nativeAuthFolderName, 'credValidator.uexe')
usersyncBaseDirFullName = join(rangerBaseDirName, usersyncBaseDirName)
confFolderName = join(usersyncBaseDirFullName, confBaseDirName)
localConfFolderName = join(installPropDirName, confBaseDirName)
certFolderName = join(confFolderName, certBaseDirName)
defaultKSFileName = join(certFolderName, defaultCertFileName)
defaultKSPassword = 'UnIx529p'
defaultDNAME = 'cn=unixauthservice,ou=authenticator,o=mycompany,c=US'

unixUserProp = 'unix_user'
unixGroupProp = 'unix_group'

logFolderPermMode = 0770
rootOwnerId = 0
initPrefixList = ['S99', 'K00']

SYNC_SOURCE_KEY  = 'SYNC_SOURCE'
SYNC_INTERVAL_NEW_KEY = 'ranger.usersync.sleeptimeinmillisbetweensynccycle'
SYNC_SOURCE_UNIX = 'unix'
SYNC_SOURCE_LDAP = 'ldap'
SYNC_SOURCE_LIST = [ SYNC_SOURCE_UNIX, SYNC_SOURCE_LDAP ]

credUpdateClassName =  'org.apache.ranger.credentialapi.buildks'
#credUpdateClassName =  'com.hortonworks.credentialapi.buildks'

def archiveFile(originalFileName):
    archiveDir = dirname(originalFileName)
    archiveFileName = "." + basename(originalFileName) + "." + (strftime("%d%m%Y%H%M%S", localtime()))
    movedFileName = join(archiveDir,archiveFileName)
    print "INFO: moving [%s] to [%s] ......." % (originalFileName,movedFileName)
    os.rename(originalFileName, movedFileName)

def getXMLConfigKeys(xmlFileName):
    ret = []
    tree = ET.parse(xmlFileName)
    root = tree.getroot()
    for config in root.iter('property'):
        name = config.find('name').text
        ret.append(name)
    return ret

def getXMLConfigMap(xmlFileName):
    ret = {}
    tree = ET.parse(xmlFileName)
    root = tree.getroot()
    for config in root.findall('property'):
        name = config.find('name').text
        val = config.find('value').text
        ret[name] = val
    return ret


def getPropertiesConfigMap(configFileName):
    ret = {}
    config = StringIO.StringIO()
    config.write('[dummysection]\n')
    config.write(open(configFileName).read())
    config.seek(0,os.SEEK_SET)
    fcp = ConfigParser.ConfigParser()
    fcp.optionxform = str
    fcp.readfp(config)
    for k,v in fcp.items('dummysection'):
        ret[k] = v
    return ret

def getPropertiesKeyList(configFileName):
    ret = []
    config = StringIO.StringIO()
    config.write('[dummysection]\n')
    config.write(open(configFileName).read())
    config.seek(0,os.SEEK_SET)
    fcp = ConfigParser.ConfigParser()
    fcp.optionxform = str
    fcp.readfp(config)
    for k,v in fcp.items('dummysection'):
        ret.append(k)
    return ret

def writeXMLUsingProperties(xmlTemplateFileName,prop,xmlOutputFileName):
    tree = ET.parse(xmlTemplateFileName)
    root = tree.getroot()
    for config in root.findall('property'):
        name = config.find('name').text
        if (name in prop.keys()):
            config.find('value').text = prop[name]
        #else:
        #    print "ERROR: key not found: %s" % (name)
    if isfile(xmlOutputFileName):
        archiveFile(xmlOutputFileName)
    tree.write(xmlOutputFileName)

def updateProppertyInJCKSFile(jcksFileName,propName,value):
    fn = jcksFileName
    if (value == ''):
        value = ' '
    cmd = "java -cp './lib/*' %s create '%s' -value '%s' -provider jceks://file%s 2>&1" % (credUpdateClassName,propName,value,fn)
    ret = os.system(cmd)
    if (ret != 0):
        print "ERROR: Unable update the JCKSFile(%s) for aliasName (%s)" % (fn,propName)
        sys.exit(1)
    return ret


def convertInstallPropsToXML(props):
	directKeyMap = getPropertiesConfigMap(join(installTemplateDirName,install2xmlMapFileName))
	ret = {}
	for k,v in props.iteritems():
		if (k in directKeyMap.keys()):
			newKey = directKeyMap[k]
			ret[newKey] = v
		else:
			print "Direct Key not found:%s" % (k)

	ret['ranger.usersync.sink.impl.class'] = 'org.apache.ranger.unixusersync.process.PolicyMgrUserGroupBuilder'
	if (SYNC_SOURCE_KEY in ret):
		syncSource = ret[SYNC_SOURCE_KEY]
		if (syncSource == SYNC_SOURCE_UNIX):
			ret['ranger.usersync.source.impl.class'] = 'org.apache.ranger.unixusersync.process.UnixUserGroupBuilder'
			if (SYNC_INTERVAL_NEW_KEY not in ret or len(str(ret[SYNC_INTERVAL_NEW_KEY])) == 0):
				ret[SYNC_INTERVAL_NEW_KEY] = "300000"
			else:
				ret[SYNC_INTERVAL_NEW_KEY] = int(ret[SYNC_INTERVAL_NEW_KEY]) * 60000
			#for key in ret.keys():
			#	if (key.startswith("ranger.usersync.ldap") or key.startswith("ranger.usersync.group") or key.startswith("ranger.usersync.paged")):
			#		del ret[key]
		elif (syncSource == SYNC_SOURCE_LDAP):
			ret['ranger.usersync.source.impl.class'] = 'org.apache.ranger.ldapusersync.process.LdapUserGroupBuilder'
			if (SYNC_INTERVAL_NEW_KEY not in ret or len(str(ret[SYNC_INTERVAL_NEW_KEY])) == 0):
				ret[SYNC_INTERVAL_NEW_KEY] = "3600000"
			else:
				ret[SYNC_INTERVAL_NEW_KEY] = int(ret[SYNC_INTERVAL_NEW_KEY]) * 60000
		else:
			print "ERROR: Invalid value (%s) defined for %s in install.properties. Only valid values are %s" % (syncSource, SYNC_SOURCE_KEY,SYNC_SOURCE_LIST)
			sys.exit(1)
		del ret['SYNC_SOURCE']
	else:
		print "ERROR: No value defined for SYNC_SOURCE in install.properties. valid values are %s" % (SYNC_SOURCE_KEY, SYNC_SOURCE_LIST)
		sys.exit(1)

	return ret

def createUser(username,groupname):
	cmd = "useradd -g %s %s -m" % (groupname,username)
	ret = os.system(cmd)
	if (ret != 0):
		print "ERROR: os command execution (%s) failed. error code = %d " % (cmd, ret)
		sys.exit(1)
	try:
		ret = pwd.getpwnam(username).pw_uid
		return ret
	except KeyError, e:
		print "ERROR: Unable to create a new user account: %s with group %s - error [%s]" % (username,groupname,e)
		sys.exit(1)

def createGroup(groupname):
	cmd = "groupadd %s" % (groupname)
	ret = os.system(cmd)
	if (ret != 0):
		print "ERROR: os command execution (%s) failed. error code = %d " % (cmd, ret)
		sys.exit(1)
	try:
		ret = grp.getgrnam(groupname).gr_gid
		return ret
	except KeyError, e:
		print "ERROR: Unable to create a new group: %s" % (groupname,e)
		sys.exit(1)

def initializeInitD():
	if (os.path.isdir(initdDirName)):
		fn = join(installPropDirName,initdProgramName)
		initdFn = join(initdDirName,initdProgramName)
		shutil.copy(fn, initdFn)
		os.chmod(initdFn,0550)
		rcDirList = [ "/etc/rc2.d", "/etc/rc3.d", "/etc/rc.d/rc2.d", "/etc/rc.d/rc3.d" ]
		for rcDir in rcDirList:
			if (os.path.isdir(rcDir)):
				for  prefix in initPrefixList:
					scriptFn = prefix + initdProgramName
					scriptName = join(rcDir, scriptFn)
					if isfile(scriptName):
						os.remove(scriptName)
					#print "+ ln -sf %s %s" % (initdFn, scriptName)
					os.symlink(initdFn,scriptName)
		userSyncScriptName = "ranger-usersync-services.sh"
		localScriptName = os.path.abspath(join(installPropDirName,userSyncScriptName))
		ubinScriptName = join("/usr/bin",initdProgramName)
		if isfile(ubinScriptName):
			os.remove(ubinScriptName)
		os.symlink(localScriptName,ubinScriptName)


def createJavaKeystoreForSSL(fn,passwd):
	cmd = "keytool -genkeypair -keyalg RSA -alias selfsigned -keystore '%s' -keypass '%s' -storepass '%s' -validity 3600 -keysize 2048 -dname '%s'" % (fn, passwd, passwd, defaultDNAME)
	ret = os.system(cmd)
	if (ret != 0):
		print "ERROR: unable to create JavaKeystore for SSL: file (%s)" % (fn)
		sys.exit(1)
	return ret


def main():

	dirList = [ rangerBaseDirName, usersyncBaseDirName, confFolderName, certFolderName ]
	for dir in dirList:
		if (not os.path.isdir(dir)):
			os.makedirs(dir,0750)

	defFileList = [ defaultSiteXMLFileName, log4jFileName ]
	for defFile in defFileList:
		fn = join(confDistDirName, defFile)
		if ( isfile(fn) ):
			shutil.copy(fn,join(confFolderName,defFile))

	#
	# Create JAVA_HOME setting in confFolderName
	#
	java_home_setter_fn = join(confFolderName, 'java_home.sh')
	if isfile(java_home_setter_fn):
		archiveFile(java_home_setter_fn)
	jhf = open(java_home_setter_fn, 'w')
	str = "export JAVA_HOME=%s\n" % os.environ['JAVA_HOME']
	jhf.write(str)
	jhf.close()
	os.chmod(java_home_setter_fn,0750)


	if (not os.path.isdir(localConfFolderName)):
		os.symlink(confFolderName, localConfFolderName)

	defaultProps = getXMLConfigMap(join(confFolderName,defaultSiteXMLFileName))
	installProps = getPropertiesConfigMap(join(installPropDirName,installPropFileName))
	modifiedInstallProps = convertInstallPropsToXML(installProps)

	mergeProps = {}
	mergeProps.update(defaultProps)
	mergeProps.update(modifiedInstallProps)

	localLogFolderName = mergeProps['ranger.usersync.logdir']
	if (not os.path.isdir(localLogFolderName)):
		if (localLogFolderName != ugsyncLogFolderName):
			os.symlink(ugsyncLogFolderName, localLogFolderName)

	if (not 'ranger.usersync.keystore.file' in mergeProps):
		mergeProps['ranger.usersync.keystore.file'] = defaultKSFileName

	ksFileName = mergeProps['ranger.usersync.keystore.file']

	if (not isfile(ksFileName)):
		mergeProps['ranger.usersync.keystore.password'] = defaultKSPassword
		createJavaKeystoreForSSL(ksFileName, defaultKSPassword)




	fn = join(installTemplateDirName,templateFileName)
	outfn = join(confFolderName, outputFileName)
	writeXMLUsingProperties(fn, mergeProps, outfn)

	if ( os.path.isdir(logFolderName) ):
		logStat = os.stat(logFolderName)
		logStat.st_uid
		logStat.st_gid
		ownerName = pwd.getpwuid(logStat.st_uid).pw_name
		groupName = pwd.getpwuid(logStat.st_uid).pw_name
	else:
		os.makedirs(logFolderName,logFolderPermMode)

	if (not os.path.isdir(pidFolderName)):
		os.makedirs(pidFolderName,logFolderPermMode)

	if (not os.path.isdir(ugsyncLogFolderName)):
		os.makedirs(ugsyncLogFolderName,logFolderPermMode)

	if (unixUserProp in mergeProps):
		ownerName = mergeProps[unixUserProp]
	else:
		print "ERROR: Property [%s] not defined." % (unixUserProp)
		sys.exit(1)

	if (unixGroupProp in mergeProps):
		groupName = mergeProps[unixGroupProp]
	else:
		print "ERROR: Property [%s] not defined." % (unixGroupProp)
		sys.exit(1)

	try:
		ownerId = pwd.getpwnam(ownerName).pw_uid
	except KeyError, e:
		ownerId = createUser(ownerName, groupName)

	try:
		groupId = grp.getgrnam(groupName).gr_gid
	except KeyError, e:
		groupId = createGroup(groupId)

	os.chown(logFolderName,ownerId,groupId)
	os.chown(ugsyncLogFolderName,ownerId,groupId)
	os.chown(pidFolderName,ownerId,groupId)

	initializeInitD()

	#
	# Add password to crypt path
	#

	cryptPath = mergeProps['ranger.usersync.credstore.filename']

	for keyName,aliasName in PROP2ALIASMAP.iteritems() :
		if (keyName in mergeProps):
			keyPassword = mergeProps[keyName]
			updateProppertyInJCKSFile(cryptPath,aliasName,keyPassword)
		else:
			updateProppertyInJCKSFile(cryptPath,aliasName," ")


	fixPermList = [ "." ]
	for d in dirList:
		fixPermList.append(d)

	for dir in fixPermList:
		for root, dirs, files in os.walk(dir):
			os.chown(root, ownerId, groupId)
			os.chmod(root,0755)
			for obj in dirs:
				dn = join(root,obj)
				os.chown(dn, ownerId, groupId)
				os.chmod(dn, 0755)
			for obj in files:
				fn = join(root,obj)
				os.chown(fn, ownerId, groupId)
				os.chmod(fn, 0750)

	if isfile(nativeAuthProgramName):
		os.chown(nativeAuthProgramName, rootOwnerId, groupId)
		os.chmod(nativeAuthProgramName, 04550)
	else:
		print "WARNING: Unix Authentication Program (%s) is not available for setting chmod(4550), chown(%s:%s) " % (nativeAuthProgramName, "root", groupName)

main()
