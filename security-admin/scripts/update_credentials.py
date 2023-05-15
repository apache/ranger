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
import logging
import getpass
import platform
import argparse
import subprocess
def validate_user_input(inp):
	if len(inp) == 0:
		logging.error("Input Error. Exiting!")
		return False
	if re.search("[\\\`'\"]", inp):
		logging.error("Input contains one of the unsupported special characters like \" ' \ `. Exiting!")
		return False
	return True

class CredManager:
	timeout = 60
	def __init__(self):
		self.os_name = platform.system().upper()

		self.ranger_admin_home = os.getenv("RANGER_ADMIN_HOME")
		if self.ranger_admin_home is None:
			self.ranger_admin_home = os.getcwd()

		self.ews_lib = os.path.join(self.ranger_admin_home,"ews","lib")
		self.app_home = os.path.join(self.ranger_admin_home,"ews","webapp")
		self.ranger_log = os.path.join(self.ranger_admin_home,"ews","logs")

	def get_java_bin(self):
		if os.environ['JAVA_HOME'] == "":
			logging.error("---------- JAVA_HOME environment property not defined, exiting. ----------")
			sys.exit(1)

		java_bin = os.path.join(os.environ['JAVA_HOME'],'bin','java')
		if self.os_name == "WINDOWS" :
			java_bin = java_bin +'.exe'

		if not os.path.isfile(java_bin):
			logging.error("Unable to locate java, enter java executable path")
			java_bin = input()
		logging.info("Using Java: {}".format(java_bin))
		return java_bin

	def get_path(self):
		home = self.app_home
		ews_lib = self.ews_lib
		if self.os_name == "LINUX":
			return os.path.join("%s","WEB-INF","classes","conf:%s",
								"WEB-INF","classes","lib","*:%s",
								"WEB-INF",":%s",
								"META-INF",":%s",
								"WEB-INF","lib","*:%s",
								"WEB-INF","classes",":%s",
								"WEB-INF","classes",
								"META-INF:%s/*") \
				   %(home, home, home, home, home, home, home, ews_lib)
		elif self.os_name == "WINDOWS":
			return os.path.join("%s", "WEB-INF","classes","conf;%s",
								"WEB-INF","classes","lib","*;%s",
								"WEB-INF",";%s",
								"META-INF",";%s",
								"WEB-INF","lib","*;%s",
								"WEB-INF","classes",";%s",
								"WEB-INF","classes",
								"META-INF" ) \
				   %(home, home, home, home, home, home, home)
		else: return None
	def __invoke_cmd(self, command, header):
		ret = None
		try:
			if self.os_name == "LINUX":
				ret = subprocess.call(shlex.split(command), timeout=CredManager.timeout)
			elif self.os_name == "WINDOWS":
				ret = subprocess.call(command, timeout=CredManager.timeout)
			else:
				logging.error("Platform not supported!")
		except subprocess.TimeoutExpired:
			logging.error("The command timed out after {} seconds".format(CredManager.timeout))
		if ret == 0:
			logging.info("{} updated successfully.".format(header))
		else:
			logging.error("Unable to update {} for user.".format(header))
	def update_username(self, username, current_password, new_username):
		java_cmd = "%s " \
					   "-Dlogdir=%s " \
					   "-Dlogback.configurationFile=db_patch.logback.xml " \
					   "-cp %s org.apache.ranger.patch.cliutil.ChangeUserNameUtil %s %s %s"\
					   %(self.get_java_bin(), self.ranger_log, self.get_path(),
						 '"'+ username +'"','"' + current_password + '"','"' + new_username + '"')
		self.__invoke_cmd(java_cmd, "username")

	def update_password(self, username, current_password, new_password):
		java_cmd = "%s " \
				   "-Dlogdir=%s " \
				   "-Dlogback.configurationFile=db_patch.logback.xml " \
				   "-cp %s org.apache.ranger.patch.cliutil.ChangePasswordUtil %s %s %s" \
				   %(self.get_java_bin(), self.ranger_log, self.get_path(),
					 '"'+ username +'"','"' + current_password + '"','"'+ new_password + '"')
		self.__invoke_cmd(java_cmd, "password")


def main():
	logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
	parser = argparse.ArgumentParser(description='Usage : python update_credentials.py login_id')
	parser.add_argument('-n', '--username', help='user name', required=True)
	args = parser.parse_args()
	assert validate_user_input(args.username)

	choice = input("Please choose an option: \n"
				   "1. Change username \n"
				   "2. Change password \n"
				   "Enter your choice [1-2]: ")
	if choice in ["1", "2"]:
		choice = int(choice)
		current_password = getpass.getpass("Enter current password: ")
		if len(current_password) == 0:
			logging.error("Input Error. Exiting!")
		else:
			cred_manager = CredManager()
			# update username
			if choice == 1:
				new_username = input("Enter new username: ")
				assert validate_user_input(new_username)
				cred_manager.update_username(username=args.username,
											 current_password=current_password, new_username=new_username)
			# update password
			elif choice == 2:
				new_password = getpass.getpass("Enter new password: ")
				assert validate_user_input(new_password)
				if current_password == new_password:
					logging.error("Current Password and New Password are same. Exiting! ")
				else:
					cred_manager.update_password(username=args.username,
												 current_password=current_password, new_password=new_password)
	else:
		logging.error("Invalid option. Exiting! ")
main()