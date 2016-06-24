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
globalDict = {}

if (not 'JAVA_HOME' in os.environ):
	print "ERROR: JAVA_HOME environment variable is not defined. Please define JAVA_HOME before running this script"
	sys.exit(1)

debugLevel = 1
generateXML = 0
installPropDirName = '.'
pidFolderName = '/var/run/ranger'
#logFolderName = '/var/log/ranger'
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
log4jFileName          = 'log4j.properties'
install2xmlMapFileName = 'installprop2xml.properties'
templateFileName = 'ranger-ugsync-template.xml'
initdProgramName = 'ranger-usersync'
PROP2ALIASMAP = { 'ranger.usersync.ldap.ldapbindpassword':'ranger.usersync.ldap.bindalias', 
				   'ranger.usersync.keystore.password':'usersync.ssl.key.password',
				   'ranger.usersync.truststore.password':'usersync.ssl.truststore.password'}

installTemplateDirName = join(installPropDirName,'templates')
confDistDirName = join(installPropDirName, confDistBaseDirName)
#ugsyncLogFolderName = join(logFolderName, 'usersync')
nativeAuthFolderName = join(installPropDirName, 'native')
nativeAuthProgramName = join(nativeAuthFolderName, 'credValidator.uexe')
pamAuthProgramName = join(nativeAuthFolderName, 'pamCredValidator.uexe')
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
SYNC_LDAP_BIND_PASSWORD_KEY  = 'ranger.usersync.ldap.ldapbindpassword'
credUpdateClassName =  'org.apache.ranger.credentialapi.buildks'
#credUpdateClassName =  'com.hortonworks.credentialapi.buildks'
ENV_LOGDIR_FILE = 'ranger-usersync-env-logdir.sh'
hadoopConfFileName = 'core-site.xml'
ENV_HADOOP_CONF_FILE = "ranger-usersync-env-hadoopconfdir.sh"


RANGER_USERSYNC_HOME = os.getenv("RANGER_USERSYNC_HOME")
if RANGER_USERSYNC_HOME is None:
    RANGER_USERSYNC_HOME = os.getcwd()

def populate_global_dict():
    global globalDict
    read_config_file = open(os.path.join(RANGER_USERSYNC_HOME,'install.properties'))
    for each_line in read_config_file.read().split('\n') :
        if len(each_line) == 0 : continue
        if re.search('=', each_line):
            key , value = each_line.strip().split("=",1)
            key = key.strip()
            if 'PASSWORD' in key:
                jceks_file_path = os.path.join(RANGER_USERSYNC_HOME, 'jceks','ranger_db.jceks')
                value = ''
            value = value.strip()
            globalDict[key] = value

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
    prop_arr =["ranger.usersync.ldap.ldapbindpassword", "ranger.usersync.keystore.password","ranger.usersync.truststore.password","ranger.usersync.policymgr"]
    for config in root.findall('property'):
        name = config.find('name').text
        if name in prop_arr:
            config.find('value').text = "_"
            continue
        if (name in prop.keys()):
            config.find('value').text = str(prop[name])
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

def password_validation(password, userType):
    if password:
        if re.search("[\\\`'\"]",password):
            print "[E] "+userType+" proprty contains one of the unsupported special characters like \" ' \ `"
            sys.exit(1)
        else:
            print "[I] "+userType+" proprty is verified."
    else:
        print "[E] Blank password is not allowed for proprty " +userType+ ",please enter valid password."
        sys.exit(1)


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
			ldapPass=ret[SYNC_LDAP_BIND_PASSWORD_KEY]
			password_validation(ldapPass, SYNC_LDAP_BIND_PASSWORD_KEY)
			ret['ranger.usersync.source.impl.class'] = 'org.apache.ranger.ldapusersync.process.LdapUserGroupBuilder'
			if (SYNC_INTERVAL_NEW_KEY not in ret or len(str(ret[SYNC_INTERVAL_NEW_KEY])) == 0):
				ret[SYNC_INTERVAL_NEW_KEY] = "3600000"
			else:
				ret[SYNC_INTERVAL_NEW_KEY] = int(ret[SYNC_INTERVAL_NEW_KEY]) * 60000
		else:
			print "ERROR: Invalid value (%s) defined for %s in install.properties. Only valid values are %s" % (syncSource, SYNC_SOURCE_KEY,SYNC_SOURCE_LIST)
			sys.exit(1)
		ret['ranger.usersync.sync.source'] = syncSource
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

def initializeInitD(ownerName):
	if (os.path.isdir(initdDirName)):
		fn = join(installPropDirName,initdProgramName)
		initdFn = join(initdDirName,initdProgramName)
		shutil.copy(fn, initdFn)
		if (ownerName != 'ranger'):
			f = open(initdFn,'r')
			filedata = f.read()
			f.close()
			find_str = "LINUX_USER=ranger"
			replace_str = "LINUX_USER="+ ownerName
			newdata = filedata.replace(find_str,replace_str)
			f = open(initdFn,'w')
			f.write(newdata)
			f.close()
		os.chmod(initdFn,0550)
		rcDirList = [ "/etc/rc2.d", "/etc/rc3.d", "/etc/rc.d/rc2.d", "/etc/rc.d/rc3.d" ]
		for rcDir in rcDirList:
			if (os.path.isdir(rcDir)):
				for  prefix in initPrefixList:
					scriptFn = prefix + initdProgramName
					scriptName = join(rcDir, scriptFn)
					if isfile(scriptName) or os.path.islink(scriptName):
						os.remove(scriptName)
					os.symlink(initdFn,scriptName)
			userSyncScriptName = "ranger-usersync-services.sh"
			localScriptName = os.path.abspath(join(installPropDirName,userSyncScriptName))
			ubinScriptName = join("/usr/bin",initdProgramName)
			if isfile(ubinScriptName) or os.path.islink(ubinScriptName):
				os.remove(ubinScriptName)
			os.symlink(localScriptName,ubinScriptName)


def createJavaKeystoreForSSL(fn,passwd):
	cmd = "keytool -genkeypair -keyalg RSA -alias selfsigned -keystore '%s' -keypass '%s' -storepass '%s' -validity 3600 -keysize 2048 -dname '%s'" % (fn, passwd, passwd, defaultDNAME)
	ret = os.system(cmd)
	if (ret != 0):
		print "ERROR: unable to create JavaKeystore for SSL: file (%s)" % (fn)
		sys.exit(1)
	return ret

def write_env_files(exp_var_name, log_path, file_name):
        final_path = "{0}/{1}".format(confBaseDirName,file_name)
        if not os.path.isfile(final_path):
                print "Creating %s file" % file_name
        f = open(final_path, "w")
        f.write("export {0}={1}".format(exp_var_name,log_path))
        f.close()

def main():

	populate_global_dict()
	logFolderName = globalDict['logdir']
	hadoop_conf = globalDict['hadoop_conf']

	if logFolderName.lower() == "$pwd" or logFolderName == "" :
                logFolderName = os.path.join(os.getcwd(),"logs")
	ugsyncLogFolderName = logFolderName

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
	if localLogFolderName.lower() == "$pwd" or localLogFolderName == "" :
		localLogFolderName = logFolderName
	if (not os.path.isdir(localLogFolderName)):
		if (localLogFolderName != ugsyncLogFolderName):
			os.symlink(ugsyncLogFolderName, localLogFolderName)

	if (not 'ranger.usersync.keystore.file' in mergeProps):
		mergeProps['ranger.usersync.keystore.file'] = defaultKSFileName

	ksFileName = mergeProps['ranger.usersync.keystore.file']

	if (not isfile(ksFileName)):
		mergeProps['ranger.usersync.keystore.password'] = defaultKSPassword
		createJavaKeystoreForSSL(ksFileName, defaultKSPassword)

	if ('ranger.usersync.keystore.password' not in mergeProps):
		mergeProps['ranger.usersync.keystore.password'] = defaultKSPassword


	fn = join(installTemplateDirName,templateFileName)
	outfn = join(confFolderName, outputFileName)

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
		mergeProps[unixUserProp] = "ranger"
		ownerName = mergeProps[unixUserProp]

	if (unixGroupProp in mergeProps):
		groupName = mergeProps[unixGroupProp]
	else:
		mergeProps[unixGroupProp] = "ranger"
		groupName = mergeProps[unixGroupProp]

	try:
		groupId = grp.getgrnam(groupName).gr_gid
	except KeyError, e:
		groupId = createGroup(groupName)

	try:
		ownerId = pwd.getpwnam(ownerName).pw_uid
	except KeyError, e:
		ownerId = createUser(ownerName, groupName)

	os.chown(logFolderName,ownerId,groupId)
	os.chown(ugsyncLogFolderName,ownerId,groupId)
	os.chown(pidFolderName,ownerId,groupId)
	os.chown(rangerBaseDirName,ownerId,groupId)
	os.chown(usersyncBaseDirFullName,ownerId,groupId)

	initializeInitD(ownerName)

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
	
	os.chown(cryptPath,ownerId,groupId)

	if ('ranger.usersync.policymgr.keystore' not in mergeProps):
		mergeProps['ranger.usersync.policymgr.keystore'] = cryptPath

	ugsyncCryptPath = mergeProps['ranger.usersync.policymgr.keystore']

	if ('ranger.usersync.policymgr.username' not in mergeProps):
		mergeProps['ranger.usersync.policymgr.username'] = 'rangerusersync'
	
	if ('ranger.usersync.policymgr.alias' not in mergeProps):
		mergeProps['ranger.usersync.policymgr.alias'] = 'ranger.usersync.policymgr.password'
	
	if ('ranger.usersync.policymgr.password' not in mergeProps):
		mergeProps['ranger.usersync.policymgr.password'] = 'rangerusersync'

	usersyncKSPath = mergeProps['ranger.usersync.policymgr.keystore']
	pmgrAlias = mergeProps['ranger.usersync.policymgr.alias']
	pmgrPasswd = mergeProps['ranger.usersync.policymgr.password']

	updateProppertyInJCKSFile(usersyncKSPath,pmgrAlias,pmgrPasswd)
	os.chown(ugsyncCryptPath,ownerId,groupId)

	writeXMLUsingProperties(fn, mergeProps, outfn)

	fixPermList = [ ".", usersyncBaseDirName, confFolderName, certFolderName ]

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
		os.chmod(nativeAuthProgramName, 04555)
	else:
		print "WARNING: Unix Authentication Program (%s) is not available for setting chmod(4550), chown(%s:%s) " % (nativeAuthProgramName, "root", groupName)

	if isfile(pamAuthProgramName):
		os.chown(pamAuthProgramName, rootOwnerId, groupId)
		os.chmod(pamAuthProgramName, 04555)
	else:
		print "WARNING: Unix Authentication Program (%s) is not available for setting chmod(4550), chown(%s:%s) " % (pamAuthProgramName, "root", groupName)

        write_env_files("logdir", logFolderName, ENV_LOGDIR_FILE);
        write_env_files("RANGER_USERSYNC_HADOOP_CONF_DIR", hadoop_conf, ENV_HADOOP_CONF_FILE);
        os.chown(os.path.join(confBaseDirName, ENV_LOGDIR_FILE),ownerId,groupId)
        os.chmod(os.path.join(confBaseDirName, ENV_LOGDIR_FILE),0755)
        os.chown(os.path.join(confBaseDirName, ENV_HADOOP_CONF_FILE),ownerId,groupId)
        os.chmod(os.path.join(confBaseDirName, ENV_HADOOP_CONF_FILE),0755)

        hadoop_conf_full_path = os.path.join(hadoop_conf, hadoopConfFileName)
	usersync_conf_full_path = os.path.join(usersyncBaseDirFullName,confBaseDirName,hadoopConfFileName)
        if not isfile(hadoop_conf_full_path):
                print "WARN: core-site.xml file not found in provided hadoop conf path..."
                f = open(usersync_conf_full_path, "w")
                f.write("<configuration></configuration>")
                f.close()
		os.chown(usersync_conf_full_path,ownerId,groupId)
		os.chmod(usersync_conf_full_path,0750)
        else:
                if os.path.islink(usersync_conf_full_path):
                        os.remove(usersync_conf_full_path)

	if isfile(hadoop_conf_full_path):
        	os.symlink(hadoop_conf_full_path, usersync_conf_full_path)

main()
