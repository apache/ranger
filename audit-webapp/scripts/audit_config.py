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

import getpass
import os
import platform
import subprocess
import sys
import time
import errno
import shutil
import socket
from re import split
from time import sleep
from xml.dom import minidom

DIR_BIN = "bin"
DIR_LIB = "lib"
DIR_LIB_EXT = "libext"
DIR_CONF = "conf"
DIR_LOG = "/var/log/ranger/audit-server"
ENV_JAVA_HOME = "JAVA_HOME"
ENV_AUDIT_SERVER_CONF = "AUDIT_SERVER_CONF_DIR"
ENV_AUDIT_SERVER_HOME = "AUDIT_SERVER_HOME_DIR"
ENV_AUDIT_SERVER_LOG = "AUDIT_SERVER_LOG_DIR"
ENV_AUDIT_SERVER_PID = "AUDIT_SERVER_PID_DIR"
ENV_AUDIT_SERVER_WEBAPP = "AUDIT_SERVER_EXPANDED_WEBAPP_DIR"
ENV_AUDIT_SERVER_OPTS = "AUDIT_SERVER_OPTS"
ENV_AUDIT_SERVER_HEAP = "AUDIT_SERVER_HEAP"

ENV_KEYS = [ENV_JAVA_HOME, ENV_AUDIT_SERVER_CONF, ENV_AUDIT_SERVER_LOG, ENV_AUDIT_SERVER_PID, ENV_AUDIT_SERVER_WEBAPP,
            ENV_AUDIT_SERVER_OPTS, ENV_AUDIT_SERVER_OPTS, ENV_AUDIT_SERVER_HEAP, ENV_AUDIT_SERVER_HOME]

IS_WINDOWS = platform.system() == "Windows"
ON_POSIX = 'posix' in sys.builtin_module_names
CONF_FILE = "audit-server-site.xml"
CONF_AUDIT_SERVER_HTTP_PORT = "ranger.audit.service.http.port"
CONF_AUDIT_SERVER_HTTPS_PORT = "ranger.audit.service.https.port"
CONF_AUDIT_SERVER_ENABLE_TLS = "ranger.audit.service.enableTLS"
CONF_AUDIT_SERVER_BIND_ADDRESS = "ranger.audit.server.bind.address"
DEFAULT_AUDIT_SERVER_HTTP_PORT = "6081"
DEFAULT_AUDIT_SERVER_HTTPS_PORT = "-1"
DEFAULT_AUDIT_SERVER_HOST = "localhost"

DEBUG = True


def script_dir():
    return os.path.dirname(os.path.realpath(__file__))


def home():
    return os.environ.get(ENV_AUDIT_SERVER_HOME, os.path.dirname(script_dir()))


def lib_dir(audit_home):
    return os.path.join(audit_home, DIR_LIB)


def lib_ext_dir(audit_home):
    return os.path.join(audit_home, DIR_LIB_EXT)


def conf_dir(audit_home):
    return os.environ.get(ENV_AUDIT_SERVER_CONF, os.path.join(audit_home, DIR_CONF))


def log_dir(audit_home):
    return os.environ.get(ENV_AUDIT_SERVER_LOG, os.path.join(audit_home, DIR_LOG))


def pid_file(dir):
    return os.path.join(os.environ.get(ENV_AUDIT_SERVER_PID, os.path.join(dir, DIR_LOG)), 'audit-server.pid')


def webapp_dir(audit_home):
    return os.environ.get(ENV_AUDIT_SERVER_WEBAPP, os.path.join(audit_home, "webapp"))


def expand_webapp(audit_home):
    _webapp_dir = webapp_dir(audit_home)
    expanded_dir = os.path.join(_webapp_dir, "audit")
    d = os.sep

    if not os.path.exists(os.path.join(expanded_dir, "WEB-INF")):
        try:
            os.makedirs(expanded_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise e
            pass

        audit_war_path = os.path.join(_webapp_dir, "audit-webapp.war")

        if is_cygwin():
            audit_war_path = convert_cygwin_path(audit_war_path)

        os.chdir(expanded_dir)

        jar(audit_war_path)
    else:
        os.chdir(expanded_dir)

def copy_config(audit_home):
    _webapp_dir = webapp_dir(audit_home)
    _webapp_cls_dir = os.path.join(_webapp_dir, "audit", "WEB-INF", "classes")
    _web_conf_dir = os.path.join(_webapp_cls_dir, DIR_CONF)
    _new_web_conf_dir = os.path.join(_webapp_cls_dir, "conf")

    _conf_copy_from_dir = conf_dir(audit_home)
    _conf_copy_to_dir = _new_web_conf_dir

    if os.path.exists(os.path.join(_web_conf_dir)):
        try:
            os.mkdir(_new_web_conf_dir)
            shutil.rmtree(_web_conf_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise e
            pass

    if os.path.exists(os.path.join(_conf_copy_from_dir)):
        try:
            src_files = os.listdir(_conf_copy_from_dir)
            for file_name in src_files:
                full_file_name = os.path.join(_conf_copy_from_dir, file_name)
                if os.path.isfile(full_file_name):
                    shutil.copy(full_file_name, _conf_copy_to_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise e
        pass

def dir_must_exist(dir_name):
    if not os.path.exists(dir_name):
        try:
            os.makedirs(dir_name, mode=0o755)
            print("Created directory: %s" % dir_name)
        except OSError as e:
            if e.errno == errno.EACCES:
                print("WARNING: Cannot create directory %s (permission denied)" % dir_name)
                print("Please create it manually with: sudo mkdir -p %s && sudo chown ranger:ranger %s" % (dir_name, dir_name))
                raise
            elif e.errno != errno.EEXIST:
                raise
    return dir_name


def execute_env_sh(cnf_dir):
    env_script = '%s/audit-server-env.sh' % cnf_dir

    if not IS_WINDOWS and os.path.exists(env_script):
        env_cmd = 'source %s && env' % env_script
        command = ['bash', '-c', env_cmd]

        proc = subprocess.Popen(command, stdout=subprocess.PIPE)

        for line in proc.stdout:
            (key, _, value) = line.strip().partition("=")

            if key in ENV_KEYS:
                os.environ[key] = value

        proc.communicate()


def java(classname, args, classpath, jvm_opts_list, log_dir_=None):
    java_home = os.environ.get(ENV_JAVA_HOME, None)

    if java_home:
        prg = os.path.join(java_home, "bin", "java")
    else:
        prg = which("java")

    if prg is None:
        raise EnvironmentError('The java binary could not be found in your path or JAVA_HOME')

    commandline = [prg] + jvm_opts_list + ["-classpath", classpath, classname] + args
    return run_process(commandline, log_dir_)


def jar(path):
    java_home = os.environ.get(ENV_JAVA_HOME, None)

    if java_home:
        prg = os.path.join(java_home, "bin", "jar")
    else:
        prg = which("jar")

    if prg is None:
        raise EnvironmentError('The jar binary could not be found in your path or JAVA_HOME')

    process = run_process([prg, "-xf", path])
    process.wait()


def is_exe(f_path):
    return os.path.isfile(f_path) and os.access(f_path, os.X_OK)


def which(program):
    f_path, _ = os.path.split(program)

    if f_path:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)

            if is_exe(exe_file):
                return exe_file

    return None


def run_process(commandline, log_dir_=None, shell=False, wait=False):
    """
    Run a process
    :param wait:
    :param shell:
    :param log_dir_:
    :param commandline: command line
    :return:the return code
    """
    global finished
    debug("Executing : " + str(commandline))
    stdout_file, stderr_file = None, None
    if log_dir_:
        # Fixed: stdout goes to .out, stderr goes to .err (was reversed before)
        stdout_file = open(os.path.join(log_dir_, "catalina.out"), "w")
        stderr_file = open(os.path.join(log_dir_, "catalina.err"), "w")

    p = subprocess.Popen(commandline, stdout=stdout_file, stderr=stderr_file, shell=shell)

    if wait:
        p.communicate()

    return p


def print_output(name, src, to_std_err):
    """
    Relay the output stream to stdout line by line
    :param name:
    :param src: source stream
    :param to_std_err: flag set if stderr is to be the dest
    :return:
    """

    global need_password
    debug("starting printer for " + name)
    line = ""
    while not finished:
        (line, done) = read(src, line)
        if done:
            out(to_std_err, line + "\n")
            flush(to_std_err)
            if line.find("Enter password for") >= 0:
                need_password = True
            line = ""
    out(to_std_err, line)
    # close down: read remainder of stream
    c = src.read(1)
    while c != "":
        c = c.decode('utf-8')
        out(to_std_err, c)
        if c == "\n":
            flush(to_std_err)
        c = src.read(1)
    flush(to_std_err)
    src.close()


def read_input(name, exe):
    """
    Read input from stdin and send to process
    :param exe:
    :param name:
    :return:
    """
    global need_password
    debug("starting reader for %s" % name)
    while not finished:
        if need_password:
            need_password = False
            if sys.stdin.isatty():
                cred = getpass.getpass()
            else:
                cred = sys.stdin.readline().rstrip()
            exe.stdin.write(cred + "\n")


def debug(text):
    if DEBUG:
        print('[DEBUG] ' + text)


def error(text):
    print('[ERROR] ' + text)
    sys.stdout.flush()


def info(text):
    print('[INFO] ' + text)
    sys.stdout.flush()


def out(to_std_err, text):
    """
    Write to one of the system output channels.
    This action does not add newlines. If you want that: write them yourself
    :param to_std_err: flag set if stderr is to be the dest
    :param text: text to write.
    :return:
    """
    if to_std_err:
        sys.stderr.write(text)
    else:
        sys.stdout.write(text)


def flush(to_std_err):
    """
    Flush the output stream
    :param to_std_err: flag set if stderr is to be the dest
    :return:
    """
    if to_std_err:
        sys.stderr.flush()
    else:
        sys.stdout.flush()


def read(pipe, line):
    """
    read a char, append to the listing if there is a char that is not \n
    :param pipe: pipe to read from
    :param line: line being built up
    :return: (the potentially updated line, flag indicating newline reached)
    """

    c = pipe.read(1)
    if c != "":
        o = c.decode('utf-8')
        if o != '\n':
            line += o
            return line, False
        else:
            return line, True
    else:
        return line, False


def write_pid(pid_file_, process):
    f = open(pid_file_, 'w')
    f.write(str(process.pid))
    f.close()


def exist_pid(pid):
    if ON_POSIX:
        # check if process id exist in the current process table
        # See man 2 kill - Linux man page for info about the kill(pid,0) system function
        try:
            os.kill(pid, 0)
        except OSError as e:
            return e.errno == errno.EPERM
        else:
            return True

    elif IS_WINDOWS:
        # The os.kill approach does not work on Windows with python 2.7
        # the output from task list command is searched for the process id
        pid_str = str(pid)
        command = 'tasklist /fi  "pid eq %s"' % pid_str
        sub_process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=False)
        sub_process.communicate()
        output = subprocess.check_output(command)
        output = split(" *", output)
        for line in output:
            if pid_str in line:
                return True
        return False
    # os other than nt or posix - not supported - need to delete the file to restart server if pid no longer exist
    return True


def wait_for_shutdown(pid, msg, wait):
    count = 0
    sys.stdout.write(msg)
    while exist_pid(pid):
        sys.stdout.write('.')
        sys.stdout.flush()
        sleep(1)
        if count > wait:
            break
        count = count + 1

    sys.stdout.write('\n')


def get_audit_server_port(cnf_dir):
    port = None

    if '-port' in sys.argv:
        port = sys.argv[sys.argv.index('-port') + 1]

    if port is None:
        cnf_dir = os.path.join(cnf_dir, CONF_FILE)
        enable_tls = get_config(cnf_dir, CONF_AUDIT_SERVER_ENABLE_TLS)

        if enable_tls is not None and enable_tls.lower() == 'true':
            port = get_config_with_default(cnf_dir, CONF_AUDIT_SERVER_HTTPS_PORT, DEFAULT_AUDIT_SERVER_HTTPS_PORT)
        else:
            port = get_config_with_default(cnf_dir, CONF_AUDIT_SERVER_HTTP_PORT, DEFAULT_AUDIT_SERVER_HTTP_PORT)

    return port


def get_audit_server_host(cnf_dir):
    cnf_dir = os.path.join(cnf_dir, CONF_FILE)
    host = get_config_with_default(cnf_dir, CONF_AUDIT_SERVER_BIND_ADDRESS, DEFAULT_AUDIT_SERVER_HOST)

    if host == '0.0.0.0':
        host = DEFAULT_AUDIT_SERVER_HOST

    return host


def wait_for_startup(cnf_dir, wait):
    global s
    count = 0
    host = get_audit_server_host(cnf_dir)
    port = get_audit_server_port(cnf_dir)

    print("Waiting for Audit Server to listen on host " + host + ":" + port)

    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            s.settimeout(1)
            s.connect((host, int(port)))
            s.close()

            break
        except Exception as e:
            # Wait for 1 sec before next ping
            sys.stdout.write('.')
            sys.stdout.flush()

            sleep(1)

        if count > wait:
            s.close()

            break

        count = count + 1

    sys.stdout.write('\n')


def server_already_running(pid):
    print("Audit server is already running under process " + pid)
    sys.exit()


def server_pid_not_running(pid):
    print("Audit Server is no longer running with pid " + pid)


def get_config(file, key):
    ret = None
    conf = minidom.parse(file)

    for property in conf.getElementsByTagName('property'):
        name_node = property.getElementsByTagName('name').item(0).firstChild

        if name_node is None:
            continue

        name = name_node.data.strip()

        if name == key:
            value_node = property.getElementsByTagName('value').item(0).firstChild
            if value_node is not None:
                ret = value_node.data.strip()
                break

    if ret is not None:
        print(key + " =" + ret)
    else:
        print(key + " = None")
    return ret


def get_config_with_default(file, key, default_value):
    value = get_config(file, key)
    if value is None:
        value = default_value
    return value


def is_cygwin():
    return platform.system().startswith("CYGWIN")


# Convert the specified cygwin-style pathname to Windows format,
# using the cygpath utility.  By default, path is assumed
# to be a file system pathname.  If isClasspath is True,
# then path is treated as a Java classpath string.
def convert_cygwin_path(path, is_classpath=False):
    if is_classpath:
        cygpath_args = ["cygpath", "-w", "-p", path]
    else:
        cygpath_args = ["cygpath", "-w", path]

    windows_path = subprocess.Popen(cygpath_args, stdout=subprocess.PIPE).communicate()[0]
    windows_path = windows_path.strip()

    return windows_path
