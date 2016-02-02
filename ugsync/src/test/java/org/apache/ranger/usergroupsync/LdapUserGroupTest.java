/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.usergroupsync;

import static org.junit.Assert.assertEquals;

import org.apache.directory.server.annotations.CreateLdapConnectionPool;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;
import org.apache.ranger.ldapusersync.process.LdapUserGroupBuilder;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.directory.server.core.annotations.CreateIndex;
import org.apache.ranger.usergroupsync.PolicyMgrUserGroupBuilderTest;

@RunWith(FrameworkRunner.class)
@CreateDS(name = "classDS",
partitions =
{
    @CreatePartition(
        name = "AD",
        suffix = "DC=ranger,DC=qe,DC=hortonworks,DC=com",
        contextEntry = @ContextEntry(
            entryLdif =
            "dn: DC=ranger,DC=qe,DC=hortonworks,DC=com\n" +
                "objectClass: domain\n" +
                "objectClass: top\n" +
                "dc: example\n\n"
        ),
        indexes =
            {
                @CreateIndex(attribute = "objectClass"),
                @CreateIndex(attribute = "dc"),
                @CreateIndex(attribute = "ou")
        }
    )
}
)
@CreateLdapConnectionPool(
        maxActive = 1,
        maxWait = 5000 )
@ApplyLdifFiles( {
	"ADSchema.ldif"
	}
	)
public class LdapUserGroupTest extends AbstractLdapTestUnit{
	private UserGroupSyncConfig config;
    private LdapUserGroupBuilder ldapBuilder;
    
	@Before
	public void setup() throws Exception {
		LdapServer ldapServer = new LdapServer(); 
	    ldapServer.setSaslHost("127.0.0.1");
	    ldapServer.setSearchBaseDn("DC=ranger,DC=qe,DC=hortonworks,DC=com"); 
	    ldapServer.setTransports(new TcpTransport("127.0.0.1", 10389)); 
	    ldapServer.setDirectoryService(getService());
	    ldapServer.setMaxSizeLimit( LdapServer.NO_SIZE_LIMIT );
	    setLdapServer(ldapServer);
	    getService().startup();
	    getLdapServer().start();
		config = UserGroupSyncConfig.getInstance();	
		ldapBuilder = new LdapUserGroupBuilder();
        ldapBuilder.init();
	}
	
	@Test
    public void testUpdateSinkTotalUsers() throws Throwable {
		config.setUserSearchFilter("");
        config.setGroupSearchEnabled(false);
        config.setPagedResultsEnabled(true);
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(109, sink.getTotalUsers());
    }
	
	@Test
    public void testUpdateSinkWithoutPagedResults() throws Throwable {
		config.setUserSearchFilter("");
        config.setGroupSearchEnabled(false);
        config.setPagedResultsEnabled(false);
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(109, sink.getTotalUsers());
    }
	
	@Test
    public void testUpdateSinkUserFilter() throws Throwable {
            //config.setUserSearchFilter("(|(memberof=cn=usersGroup9,ou=Group,dc=openstacklocal)(memberof=cn=usersGroup4,ou=Group,dc=openstacklocal))");
            config.setUserSearchFilter("(|(memberof=CN=Group10,OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com)(memberof=CN=Group11,OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com))");
            config.setGroupSearchEnabled(false);
            PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
            sink.init();
            ldapBuilder.updateSink(sink);
            assertEquals(12, sink.getTotalUsers());
    }

    @Test
    public void testUpdateSinkTotalGroups() throws Throwable {
            config.setUserSearchFilter("");
            config.setGroupSearchFilter("");
            config.setGroupSearchEnabled(true);
            PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
            sink.init();
            ldapBuilder.updateSink(sink);
            assertEquals(10, sink.getTotalGroups());
    }

    @Test
    public void testUpdateSinkGroupFilter() throws Throwable {
            config.setUserSearchFilter("");
            config.setGroupSearchFilter("cn=Group19");
            config.setGroupSearchEnabled(true);
            PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
            sink.init();
            ldapBuilder.updateSink(sink);
            assertEquals(1, sink.getTotalGroups());
    }

    @Test
    public void testUpdateSinkGroupSearchDisable() throws Throwable {
            config.setUserSearchFilter("");
            config.setGroupSearchFilter("cn=Group19");
            config.setGroupSearchEnabled(false);
            PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
            sink.init();
            ldapBuilder.updateSink(sink);
            assertEquals(11, sink.getTotalGroups());
    }
    
    @After
    public void shutdown() throws Exception {
    	if (getService().isStarted()) {
    		getService().shutdown();
    	}
    	if (getLdapServer().isStarted()) {
    		getLdapServer().stop();
    	}
    }
}
