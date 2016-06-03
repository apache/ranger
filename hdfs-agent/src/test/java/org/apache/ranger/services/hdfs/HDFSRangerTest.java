/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.services.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.hadoop.RangerHdfsAuthorizer;
import org.apache.ranger.authorization.hadoop.exceptions.RangerAccessControlException;
import org.junit.Assert;

/**
 * Here we plug the Ranger AccessControlEnforcer into HDFS. 
 * 
 * A custom RangerAdminClient is plugged into Ranger in turn, which loads security policies from a local file. These policies were 
 * generated in the Ranger Admin UI for a service called "HDFSTest". It contains three policies, each of which grants read, write and
 * execute permissions in turn to "/tmp/tmpdir", "/tmp/tmpdir2" and "/tmp/tmpdir3" to a user called "bob" and to a group called "IT".
 */
public class HDFSRangerTest {
    
    private static final File baseDir = new File("./target/hdfs/").getAbsoluteFile();
    private static MiniDFSCluster hdfsCluster;
    private static String defaultFs;
    
    @org.junit.BeforeClass
    public static void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        conf.set("dfs.namenode.inode.attributes.provider.class", RangerHdfsAuthorizer.class.getName());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();
        defaultFs = conf.get("fs.defaultFS");
    }
    
    @org.junit.AfterClass
    public static void cleanup() throws Exception {
        FileUtil.fullyDelete(baseDir);
        hdfsCluster.shutdown();
    }
    
    @org.junit.Test
    public void readTest() throws Exception {
        FileSystem fileSystem = hdfsCluster.getFileSystem();
        
        // Write a file - the AccessControlEnforcer won't be invoked as we are the "superuser"
        final Path file = new Path("/tmp/tmpdir/data-file2");
        FSDataOutputStream out = fileSystem.create(file);
        for (int i = 0; i < 1024; ++i) {
            out.write(("data" + i + "\n").getBytes("UTF-8"));
            out.flush();
        }
        out.close();
        
        // Change permissions to read-only
        fileSystem.setPermission(file, new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE));
        
        // Now try to read the file as "bob" - this should be allowed (by the policy - user)
        UserGroupInformation ugi = UserGroupInformation.createUserForTesting("bob", new String[] {});
        ugi.doAs(new PrivilegedExceptionAction<Void>() {

            public Void run() throws Exception {
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS", defaultFs);
                
                FileSystem fs = FileSystem.get(conf);
                
                // Read the file
                FSDataInputStream in = fs.open(file);
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                IOUtils.copy(in, output);
                String content = new String(output.toByteArray());
                Assert.assertTrue(content.startsWith("data0"));
                
                fs.close();
                return null;
            }
        });
        
        // Now try to read the file as "alice" - this should be allowed (by the policy - group)
        ugi = UserGroupInformation.createUserForTesting("alice", new String[] {"IT"});
        ugi.doAs(new PrivilegedExceptionAction<Void>() {

            public Void run() throws Exception {
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS", defaultFs);
                
                FileSystem fs = FileSystem.get(conf);
                
                // Read the file
                FSDataInputStream in = fs.open(file);
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                IOUtils.copy(in, output);
                String content = new String(output.toByteArray());
                Assert.assertTrue(content.startsWith("data0"));
                
                fs.close();
                return null;
            }
        });
        
        // Now try to read the file as unknown user "eve" - this should not be allowed
        ugi = UserGroupInformation.createUserForTesting("eve", new String[] {});
        ugi.doAs(new PrivilegedExceptionAction<Void>() {

            public Void run() throws Exception {
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS", defaultFs);
                
                FileSystem fs = FileSystem.get(conf);
                
                // Read the file
                try {
                    fs.open(file);
                    Assert.fail("Failure expected on an incorrect permission");
                } catch (RemoteException ex) {
                    // expected
                    Assert.assertTrue(RangerAccessControlException.class.getName().equals(ex.getClassName()));
                }
                
                fs.close();
                return null;
            }
        });
    }
    
    @org.junit.Test
    public void writeTest() throws Exception {
        
        FileSystem fileSystem = hdfsCluster.getFileSystem();
        
        // Write a file - the AccessControlEnforcer won't be invoked as we are the "superuser"
        final Path file = new Path("/tmp/tmpdir2/data-file3");
        FSDataOutputStream out = fileSystem.create(file);
        for (int i = 0; i < 1024; ++i) {
            out.write(("data" + i + "\n").getBytes("UTF-8"));
            out.flush();
        }
        out.close();
        
        // Now try to write to the file as "bob" - this should be allowed (by the policy - user)
        UserGroupInformation ugi = UserGroupInformation.createUserForTesting("bob", new String[] {});
        ugi.doAs(new PrivilegedExceptionAction<Void>() {

            public Void run() throws Exception {
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS", defaultFs);
                
                FileSystem fs = FileSystem.get(conf);
                
                // Write to the file
                fs.append(file);
                
                fs.close();
                return null;
            }
        });
        
        // Now try to write to the file as "alice" - this should be allowed (by the policy - group)
        ugi = UserGroupInformation.createUserForTesting("alice", new String[] {"IT"});
        ugi.doAs(new PrivilegedExceptionAction<Void>() {

            public Void run() throws Exception {
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS", defaultFs);
                
                FileSystem fs = FileSystem.get(conf);
                
                // Write to the file
                fs.append(file);
                
                fs.close();
                return null;
            }
        });
        
        // Now try to read the file as unknown user "eve" - this should not be allowed
        ugi = UserGroupInformation.createUserForTesting("eve", new String[] {});
        ugi.doAs(new PrivilegedExceptionAction<Void>() {

            public Void run() throws Exception {
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS", defaultFs);
                
                FileSystem fs = FileSystem.get(conf);
                
                // Write to the file
                try {
                    fs.append(file);
                    Assert.fail("Failure expected on an incorrect permission");
                } catch (RemoteException ex) {
                    // expected
                    Assert.assertTrue(RangerAccessControlException.class.getName().equals(ex.getClassName()));
                }
                
                fs.close();
                return null;
            }
        });
    }
 
    @org.junit.Test
    public void executeTest() throws Exception {
        FileSystem fileSystem = hdfsCluster.getFileSystem();
        
        // Write a file - the AccessControlEnforcer won't be invoked as we are the "superuser"
        final Path file = new Path("/tmp/tmpdir3/data-file2");
        FSDataOutputStream out = fileSystem.create(file);
        for (int i = 0; i < 1024; ++i) {
            out.write(("data" + i + "\n").getBytes("UTF-8"));
            out.flush();
        }
        out.close();
        
        // Change permissions to read-only
        fileSystem.setPermission(file, new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE));
        
        // Change the parent directory permissions to be execute only for the owner
        Path parentDir = new Path("/tmp/tmpdir3");
        fileSystem.setPermission(parentDir, new FsPermission(FsAction.EXECUTE, FsAction.NONE, FsAction.NONE));
        
        // Try to read the directory as "bob" - this should be allowed (by the policy - user)
        UserGroupInformation ugi = UserGroupInformation.createUserForTesting("bob", new String[] {});
        ugi.doAs(new PrivilegedExceptionAction<Void>() {

            public Void run() throws Exception {
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS", defaultFs);
                
                FileSystem fs = FileSystem.get(conf);
                
                RemoteIterator<LocatedFileStatus> iter = fs.listFiles(file.getParent(), false);
                Assert.assertTrue(iter.hasNext());
                
                fs.close();
                return null;
            }
        });
        
        // Try to read the directory as "alice" - this should be allowed (by the policy - group)
        ugi = UserGroupInformation.createUserForTesting("alice", new String[] {"IT"});
        ugi.doAs(new PrivilegedExceptionAction<Void>() {

            public Void run() throws Exception {
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS", defaultFs);
                
                FileSystem fs = FileSystem.get(conf);
                
                RemoteIterator<LocatedFileStatus> iter = fs.listFiles(file.getParent(), false);
                Assert.assertTrue(iter.hasNext());
                
                fs.close();
                return null;
            }
        });
        
        // Now try to read the directory as unknown user "eve" - this should not be allowed
        ugi = UserGroupInformation.createUserForTesting("eve", new String[] {});
        ugi.doAs(new PrivilegedExceptionAction<Void>() {

            public Void run() throws Exception {
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS", defaultFs);
                
                FileSystem fs = FileSystem.get(conf);
                
                // Write to the file
                try {
                    RemoteIterator<LocatedFileStatus> iter = fs.listFiles(file.getParent(), false);
                    Assert.assertTrue(iter.hasNext());
                    Assert.fail("Failure expected on an incorrect permission");
                } catch (RemoteException ex) {
                    // expected
                    Assert.assertTrue(RangerAccessControlException.class.getName().equals(ex.getClassName()));
                }
                
                fs.close();
                return null;
            }
        });
        
    }
    
}
