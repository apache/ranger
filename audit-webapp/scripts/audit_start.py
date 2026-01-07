#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import traceback
import audit_config as conf

AUDIT_SERVER_LOG_OPTS = "-Daudit.server.log.dir=%s -Daudit.server.log.file=%s.log"
AUDIT_SERVER_COMMAND_OPTS = "-Daudit.server.home=%s -Daudit.server.webapp.dir=%s"
AUDIT_SERVER_CONFIG_OPTS = "-Daudit.server.conf=%s"
DEFAULT_JVM_HEAP_OPTS = "-Xmx1024m"
DEFAULT_JVM_OPTS = "-Dlog4j2.configurationFile=%s/audit-log4j2.properties -Djava.net.preferIPv4Stack=true -server -Dlog4j2.debug=true"

def main():
    audit_home = conf.home()
    conf_dir = conf.dir_must_exist(conf.conf_dir(audit_home))

    conf.execute_env_sh(conf_dir)

    log_dir = conf.dir_must_exist(conf.log_dir(audit_home))
    webapp_dir = conf.webapp_dir(audit_home)
    webapp_path = os.path.join(webapp_dir, "audit")
    ews_root = os.path.join(audit_home, "ews")

    # expand web app dir
    conf.expand_webapp(audit_home)

    # copies conf folder content into webapp conf folder
    conf.copy_config(audit_home)

    if conf.is_cygwin():
        # Path names that are passed to JVM must be converted to Windows format.
        jvm_audit_home = conf.convert_cygwin_path(audit_home)
        jvm_conf_dir = conf.convert_cygwin_path(conf_dir)
        jvm_log_dir = conf.convert_cygwin_path(log_dir)
    else:
        jvm_audit_home = audit_home
        jvm_conf_dir = conf_dir
        jvm_log_dir = log_dir

    cmd_opts = (AUDIT_SERVER_COMMAND_OPTS % (jvm_audit_home, webapp_path))
    cfg_opts = (AUDIT_SERVER_CONFIG_OPTS % jvm_conf_dir)
    audit_heap_opts = os.environ.get(conf.ENV_AUDIT_SERVER_HEAP, DEFAULT_JVM_HEAP_OPTS)
    audit_server_jvm_opts = os.environ.get(conf.ENV_AUDIT_SERVER_OPTS)
    audit_opts = (DEFAULT_JVM_OPTS % jvm_conf_dir)
    audit_jvm_opts = os.environ.get(conf.ENV_AUDIT_SERVER_OPTS, audit_opts)

    # Set HADOOP_CONF_DIR to use audit-webapp's configuration directory
    # This ensures Hadoop client libraries use our core-site.xml with proxy user config
    os.environ['HADOOP_CONF_DIR'] = jvm_conf_dir

    jvm_opts_list = (AUDIT_SERVER_LOG_OPTS % (jvm_log_dir, "audit-server")).split()

    jvm_opts_list.extend(cmd_opts.split())
    jvm_opts_list.extend(cfg_opts.split())
    jvm_opts_list.extend(audit_heap_opts.split())

    if audit_server_jvm_opts:
        jvm_opts_list.extend(audit_server_jvm_opts.split())

    jvm_opts_list.extend(audit_jvm_opts.split())

    p = os.pathsep
    audit_classpath = conf_dir + p \
                      + os.path.join(webapp_dir, "audit", "conf", "*") + p \
                      + os.path.join(webapp_dir, "audit", "WEB-INF", "classes") + p \
                      + os.path.join(webapp_dir, "audit", "WEB-INF", "lib", "*")

    if conf.is_cygwin():
        audit_classpath = conf.convert_cygwin_path(audit_classpath, True)

    audit_pid_file = conf.pid_file(audit_home)

    if os.path.isfile(audit_pid_file):
        # Check if process listed in audit-server.pid file is still running
        pf = open(audit_pid_file, 'r')
        pid = pf.read().strip()

        pf.close()

        if pid != "":
            if conf.exist_pid(int(pid)):
                print("ERROR: Audit Server is already running with PID: " + pid)
                print("If you believe this is an error, remove the PID file at: " + audit_pid_file)
                sys.exit(1)
            else:
                # Process not running, remove stale PID file
                print("Removing stale PID file: " + audit_pid_file)
                os.remove(audit_pid_file)

    if conf.is_cygwin():
        webapp_path = conf.convert_cygwin_path(webapp_path)

    start_audit_server(audit_classpath, audit_pid_file, jvm_log_dir, jvm_opts_list, webapp_path)

    conf.wait_for_startup(conf_dir, 300)

    print("Audit Server started!!!")


def start_audit_server(audit_classpath, audit_pid_file, jvm_log_dir, jvm_opts_list, webapp_path):
    args = ["-app", webapp_path] + sys.argv[1:]
    process = conf.java("org.apache.ranger.audit.server.EmbeddedServer",
                        args, audit_classpath, jvm_opts_list, jvm_log_dir)

    conf.write_pid(audit_pid_file, process)


if __name__ == '__main__':
    try:
        return_code = main()
    except Exception as e:
        print("Exception: " + str(e))
        print(traceback.format_exc())

        return_code = -1

    sys.exit(return_code)
