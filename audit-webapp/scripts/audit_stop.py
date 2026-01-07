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
import signal
import time
import traceback
import audit_config as conf

def main():
    """
    Gracefully stop the Ranger Audit Server by sending SIGTERM signal
    to trigger the enhanced shutdown mechanism with Kafka resource cleanup.
    """
    audit_home = conf.home()
    audit_pid_file = conf.pid_file(audit_home)

    print("=== Ranger Audit Server Graceful Shutdown ===")
    print("Audit Home: %s" % audit_home)
    print("PID File: %s" % audit_pid_file)

    # Check if PID file exists
    if not os.path.isfile(audit_pid_file):
        print("ERROR: PID file not found. Audit server may not be running.")
        print("Expected PID file location: %s" % audit_pid_file)
        return 1

    # Read PID from file
    try:
        with open(audit_pid_file, 'r') as pf:
            pid_str = pf.read().strip()

        if not pid_str:
            print("ERROR: PID file is empty")
            return 1

        pid = int(pid_str)
        print("Found Audit Server PID: %d" % pid)

    except (IOError, ValueError) as e:
        print("ERROR: Could not read PID from file: %s" % str(e))
        return 1

    # Check if process is actually running
    if not conf.exist_pid(pid):
        print("WARNING: Process with PID %d is not running" % pid)
        print("Cleaning up stale PID file...")
        try:
            os.remove(audit_pid_file)
            print("Stale PID file removed")
        except OSError as e:
            print("Could not remove PID file: %s" % str(e))
        return 0

    # Attempt graceful shutdown
    print("Initiating graceful shutdown...")
    print("Sending SIGTERM to process %d to trigger enhanced shutdown mechanism" % pid)

    try:
        # Send SIGTERM to trigger our enhanced shutdown hook
        os.kill(pid, signal.SIGTERM)
        print("SIGTERM sent successfully")

        # Wait for graceful shutdown with timeout
        shutdown_timeout = 60  # 60 seconds timeout
        print("Waiting for graceful shutdown (timeout: %d seconds)..." % shutdown_timeout)

        if wait_for_shutdown_with_progress(pid, shutdown_timeout):
            print("\n✓ Audit Server shutdown completed gracefully")

            # Clean up PID file
            try:
                if os.path.isfile(audit_pid_file):
                    os.remove(audit_pid_file)
                    print("PID file cleaned up")
            except OSError as e:
                print("Warning: Could not remove PID file: %s" % str(e))

            return 0
        else:
            print("\n⚠ Graceful shutdown timed out after %d seconds" % shutdown_timeout)

            # Check if process is still running
            if conf.exist_pid(pid):
                print("Process is still running. Attempting force shutdown...")
                return force_shutdown(pid, audit_pid_file)
            else:
                print("Process has terminated")
                cleanup_pid_file(audit_pid_file)
                return 0

    except OSError as e:
        if e.errno == 3:  # No such process
            print("Process %d no longer exists" % pid)
            cleanup_pid_file(audit_pid_file)
            return 0
        elif e.errno == 1:  # Operation not permitted
            print("ERROR: Permission denied. Cannot send signal to process %d" % pid)
            print("Try running as the same user that started the audit server")
            return 1
        else:
            print("ERROR: Could not send signal to process %d: %s" % (pid, str(e)))
            return 1

def wait_for_shutdown_with_progress(pid, timeout):
    """
    Wait for process to shutdown with progress indication
    Returns True if process terminated within timeout, False otherwise
    """
    start_time = time.time()
    last_progress_time = start_time
    progress_interval = 5  # Show progress every 5 seconds

    sys.stdout.write("Progress: ")
    sys.stdout.flush()

    while conf.exist_pid(pid):
        current_time = time.time()
        elapsed = current_time - start_time

        if elapsed >= timeout:
            return False

        # Show progress every 5 seconds
        if current_time - last_progress_time >= progress_interval:
            sys.stdout.write("%.0fs " % elapsed)
            sys.stdout.flush()
            last_progress_time = current_time

        time.sleep(1)

    elapsed = time.time() - start_time
    sys.stdout.write("%.1fs" % elapsed)
    sys.stdout.flush()

    return True

def force_shutdown(pid, audit_pid_file):
    """
    Force shutdown using SIGKILL as last resort
    """
    print("Attempting force shutdown with SIGKILL...")

    try:
        os.kill(pid, signal.SIGKILL)
        print("SIGKILL sent")

        # Wait a bit for force kill to take effect
        force_timeout = 10
        print("Waiting for force shutdown (timeout: %d seconds)..." % force_timeout)

        if wait_for_shutdown_with_progress(pid, force_timeout):
            print("\n✓ Process terminated forcefully")
            cleanup_pid_file(audit_pid_file)
            print("⚠ WARNING: Force shutdown may have left Kafka resources in inconsistent state")
            return 0
        else:
            print("\n✗ ERROR: Could not terminate process even with SIGKILL")
            return 1

    except OSError as e:
        print("ERROR: Could not force kill process %d: %s" % (pid, str(e)))
        return 1

def cleanup_pid_file(audit_pid_file):
    """
    Clean up the PID file
    """
    try:
        if os.path.isfile(audit_pid_file):
            os.remove(audit_pid_file)
            print("PID file cleaned up")
    except OSError as e:
        print("Warning: Could not remove PID file: %s" % str(e))

def show_usage():
    """
    Show usage information
    """
    print("Usage: %s [options]" % sys.argv[0])
    print("")
    print("Gracefully stop the Ranger Audit Server")
    print("")
    print("Options:")
    print("  -h, --help     Show this help message")
    print("  -f, --force    Force shutdown immediately (skip graceful shutdown)")
    print("")
    print("Environment Variables:")
    print("  AUDIT_SERVER_HOME_DIR    Audit server home directory")
    print("  AUDIT_SERVER_PID_DIR     Directory containing PID file")
    print("")
    print("The script will:")
    print("1. Send SIGTERM to trigger graceful shutdown with Kafka resource cleanup")
    print("2. Wait up to 60 seconds for graceful termination")
    print("3. If needed, force shutdown with SIGKILL as last resort")

def force_shutdown_mode():
    """
    Force shutdown mode - immediately send SIGKILL
    """
    audit_home = conf.home()
    audit_pid_file = conf.pid_file(audit_home)

    print("=== Ranger Audit Server Force Shutdown ===")

    if not os.path.isfile(audit_pid_file):
        print("ERROR: PID file not found")
        return 1

    try:
        with open(audit_pid_file, 'r') as pf:
            pid = int(pf.read().strip())

        if not conf.exist_pid(pid):
            print("Process not running")
            cleanup_pid_file(audit_pid_file)
            return 0

        print("Force killing process %d..." % pid)
        os.kill(pid, signal.SIGKILL)

        if wait_for_shutdown_with_progress(pid, 10):
            print("\n✓ Process terminated forcefully")
            cleanup_pid_file(audit_pid_file)
            print("⚠ WARNING: Force shutdown may have left resources in inconsistent state")
            return 0
        else:
            print("\n✗ ERROR: Could not terminate process")
            return 1

    except (IOError, ValueError, OSError) as e:
        print("ERROR: %s" % str(e))
        return 1

if __name__ == '__main__':
    try:
        # Parse command line arguments
        if len(sys.argv) > 1:
            if sys.argv[1] in ['-h', '--help']:
                show_usage()
                sys.exit(0)
            elif sys.argv[1] in ['-f', '--force']:
                return_code = force_shutdown_mode()
            else:
                print("ERROR: Unknown option: %s" % sys.argv[1])
                show_usage()
                sys.exit(1)
        else:
            return_code = main()

    except KeyboardInterrupt:
        print("\n\nShutdown interrupted by user")
        return_code = 1
    except Exception as e:
        print("Exception: " + str(e))
        print(traceback.format_exc())
        return_code = -1

    sys.exit(return_code)
