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
import static org.junit.Assert.assertFalse;

import org.apache.directory.server.annotations.CreateLdapConnectionPool;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.directory.server.core.annotations.CreateIndex;

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
public class TestLdapUserGroup extends AbstractLdapTestUnit{
	private UserGroupSyncConfig config;
	private UserGroupSource ldapBuilder;

	@Before
	public void setup() throws Exception {
		LdapServer ldapServer = new LdapServer();
		ldapServer.setSaslHost("127.0.0.1");
		ldapServer.setSearchBaseDn("DC=ranger,DC=qe,DC=hortonworks,DC=com");
		String ldapPort = System.getProperty("ldap.port");
		Assert.assertNotNull("Property 'ldap.port' null", ldapPort);
		ldapServer.setTransports(new TcpTransport("127.0.0.1", Integer.parseInt(ldapPort)));
		ldapServer.setDirectoryService(getService());
		ldapServer.setMaxSizeLimit( LdapServer.NO_SIZE_LIMIT );
		setLdapServer(ldapServer);
		getService().startup();
		getLdapServer().start();
		config = UserGroupSyncConfig.getInstance();	
		ldapBuilder = new LdapUserGroupBuilder();
	}
	
	@Test
	public void testUpdateSinkTotalUsers() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("cn=users,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserSearchFilter("");
		config.setGroupSearchBase("OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(false);
		config.setPagedResultsEnabled(true);
		config.setGroupSearchFirstEnabled(false);
		//config.setGroupHierarchyLevel(0);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(109, sink.getTotalUsers());
	}

	@Test
	public void testUpdateSinkWithoutPagedResults() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("cn=users,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserSearchFilter("");
		config.setGroupSearchBase("OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(false);
		config.setPagedResultsEnabled(false);
		config.setGroupSearchFirstEnabled(false);
		//config.setGroupHierarchyLevel(0);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(109, sink.getTotalUsers());
	}

	@Test
	public void testUpdateSinkUserFilter() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("cn=users,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		//config.setUserSearchFilter("(|(memberof=cn=usersGroup9,ou=Group,dc=openstacklocal)(memberof=cn=usersGroup4,ou=Group,dc=openstacklocal))");
		config.setUserSearchFilter("(|(memberof=CN=Group10,OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com)(memberof=CN=Group11,OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com))");
		config.setGroupSearchBase("OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(false);
		config.setGroupSearchFirstEnabled(false);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(12, sink.getTotalUsers());
	}

	@Test
	public void testUpdateSinkTotalGroups() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("cn=users,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserSearchFilter("");
		config.setGroupSearchBase("OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(false);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(10, sink.getTotalGroups());
	}

	@Test
	public void testUpdateSinkGroupFilter() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("cn=users,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserSearchFilter("");
		config.setGroupSearchBase("OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=Group19");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(false);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(1, sink.getTotalGroups());
	}

	@Test
	public void testUpdateSinkGroupSearchDisable() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("cn=users,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserSearchFilter("");
		config.setGroupSearchBase("OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=Group19");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(false);
		config.setGroupSearchFirstEnabled(false);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(11, sink.getTotalGroups());
	}

	@Test
	public void testUpdateSinkMultipleOUs() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("cn=users,DC=ranger,DC=qe,DC=hortonworks,DC=com;ou=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;ou=BusinessUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserSearchFilter("cn=*");
		config.setGroupSearchBase("OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=*Group10");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(false);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(111, sink.getTotalUsers());
		assertEquals(1, sink.getTotalGroups());
	}

	@Test
	public void testUpdateSinkMultipleOUsNoGroupSearch() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("cn=users,DC=ranger,DC=qe,DC=hortonworks,DC=com;ou=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;ou=BusinessUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserSearchFilter("cn=*");
		config.setGroupSearchBase("OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=*Group10");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(false);
		config.setGroupSearchFirstEnabled(false);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(111, sink.getTotalUsers());
		assertEquals(12, sink.getTotalGroups());
	}

	@Test
	public void testMultipleOUGroupsNoGroupSearch() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("cn=users,DC=ranger,DC=qe,DC=hortonworks,DC=com;ou=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;ou=BusinessUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserSearchFilter("cn=*");
		config.setGroupSearchBase("OU=HdpGroups,OU=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=*Group10");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(false);
		config.setGroupSearchFirstEnabled(true);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(111, sink.getTotalUsers());
		assertEquals(12, sink.getTotalGroups());
	}

	@Test
	public void testMultipleOUGroupsWithGroupSearch() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("cn=users,DC=ranger,DC=qe,DC=hortonworks,DC=com;ou=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;ou=BusinessUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserSearchFilter("cn=*");
		config.setGroupSearchBase("OU=HdpGroups,OU=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=*");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(false);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(111, sink.getTotalUsers());
		assertEquals(11, sink.getTotalGroups());
	}

	@Test
	public void testUpdateSinkMultipleOUGroups() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("cn=users,DC=ranger,DC=qe,DC=hortonworks,DC=com;ou=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;ou=BusinessUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserSearchFilter("cn=*");
		config.setGroupSearchBase("OU=HdpGroups,OU=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=*Group10");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(false);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(111, sink.getTotalUsers());
		assertEquals(2, sink.getTotalGroups());
	}

	@Test
	public void testGroupBasedAllUsers() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("DC=ranger,DC=qe,DC=hortonworks,DC=com;");
		config.setUserSearchFilter("cn=*");
		config.setGroupSearchBase("OU=HdpGroups,OU=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=*Group10");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(true);
		config.setUserSearchEnabled(false);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(3, sink.getTotalUsers());
		assertEquals(2, sink.getTotalGroups());
	}

	@Test
	public void testGroupBasedWithUserFilter() throws Throwable {
		config.setUserSearchBase("DC=ranger,DC=qe,DC=hortonworks,DC=com;");
		config.setUserSearchFilter("cn=User*");
		config.setGroupSearchBase("OU=HdpGroups,OU=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=*Group10");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(true);
		config.setUserSearchEnabled(true);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(1, sink.getTotalUsers());
		assertEquals(2, sink.getTotalGroups());
	}

	@Test
	public void testGroupBasedWithNoUsers() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("DC=ranger,DC=qe,DC=hortonworks,DC=com;");
		config.setUserSearchFilter("cn=*");
		config.setGroupSearchBase("OU=HdpGroups,OU=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=Group2*");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(true);
		config.setUserSearchEnabled(true);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(0, sink.getTotalUsers());
		assertEquals(2, sink.getTotalGroups());
	}

	@Test
	public void testGroupBasedWithAllUsersAndGroups() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("DC=ranger,DC=qe,DC=hortonworks,DC=com;");
		config.setUserSearchFilter("cn=*");
		config.setGroupSearchBase("OU=HdpGroups,OU=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=*");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(true);
		config.setUserSearchEnabled(true);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(100, sink.getTotalUsers());
		assertEquals(13, sink.getTotalGroups());
	}

	@Test
	public void testGroupBasedWithSingleOU() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("DC=ranger,DC=qe,DC=hortonworks,DC=com;");
		config.setUserSearchFilter("cn=*");
		config.setGroupSearchBase("OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=*");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(true);
		config.setUserSearchEnabled(true);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(99, sink.getTotalUsers());
		assertEquals(12, sink.getTotalGroups());
	}

	@Test
	public void testUpdateSinkWithEmptyUserSearchBase() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("");
		config.setUserSearchFilter("");
		config.setGroupSearchBase("OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(false);
		config.setPagedResultsEnabled(true);
		config.setGroupSearchFirstEnabled(false);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(111, sink.getTotalUsers());
	}

	@Test
	public void testGBWithUserSearchDisabled() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("DC=ranger,DC=qe,DC=hortonworks,DC=com;");
		config.setUserSearchFilter("cn=User*");
		config.setGroupSearchBase("OU=HdpGroups,OU=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=*Group10");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(true);
		config.setUserSearchEnabled(false);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(3, sink.getTotalUsers());
		assertEquals(2, sink.getTotalGroups());
	}

	@Test
	public void testGBWithNoUsersAndUserSearchDisabled() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("DC=ranger,DC=qe,DC=hortonworks,DC=com;");
		config.setUserSearchFilter("cn=*");
		config.setGroupSearchBase("OU=HdpGroups,OU=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=Group2*");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(true);
		config.setUserSearchEnabled(false);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(0, sink.getTotalUsers());
		assertEquals(2, sink.getTotalGroups());
	}

	@Test
	public void testUpdateSinkShortUserName() throws Throwable {
		config.setUserNameAttribute("cn");
		config.setUserSearchBase("ou=people,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserSearchFilter("uid=*");
		config.setUserObjectClass("posixAccount");
		config.setGroupSearchBase("OU=pGroups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=*");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(false);
		config.setUserGroupMemberAttributeName("memberuid");
		config.setGroupObjectClass("posixGroup");
		config.setUserSearchEnabled(false);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(1, sink.getTotalUsers());
		assertEquals(3, sink.getTotalGroups());
	}

	@Test
	public void testShortUserNameWithGroupBased() throws Throwable {
		config.setUserNameAttribute("cn");
		config.setUserSearchBase("ou=people,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserSearchFilter("uid=*");
		config.setUserObjectClass("posixAccount");
		config.setGroupSearchBase("OU=pGroups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=*");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(true);
		config.setUserGroupMemberAttributeName("memberuid");
		config.setGroupObjectClass("posixGroup");
		config.setUserSearchEnabled(true);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(1, sink.getTotalUsers());
		assertEquals(3, sink.getTotalGroups());
	}

	@Test
	public void testUpdateSinkWithUserGroupMapping() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("cn=users,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserSearchFilter("");
		config.setGroupSearchBase("OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(false);

		config.setProperty(UserGroupSyncConfig.SYNC_MAPPING_USERNAME, "s/[=]/_/g");
		config.setProperty(UserGroupSyncConfig.SYNC_MAPPING_GROUPNAME, "s/[=]/_/g");

		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(10, sink.getTotalGroups());

		// no user should have an = character because of the mapping
		for (String user : sink.getAllUsers()) {
			assertFalse(user.contains("="));
		}

		// no group should have an = character because of the mapping
		for (String group : sink.getAllGroups()) {
			assertFalse(group.contains("="));
		}
	}
	
	@Test
	public void testGBWithInvalidOU() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("DC=ranger,DC=qe,DC=hortonworks,DC=com;");
		config.setUserSearchFilter("cn=User*");
		config.setGroupSearchBase("OU=HdpGroup1,OU=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=*Group10");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(true);
		config.setUserSearchEnabled(false);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(2, sink.getTotalUsers());
		assertEquals(1, sink.getTotalGroups());
	}
	
	@Test
	public void testMultipleOUInvalidOU() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("cn=users,DC=ranger,DC=qe,DC=hortonworks,DC=com;ou=HadoopUsers1,DC=ranger,DC=qe,DC=hortonworks,DC=com;ou=BusinessUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserSearchFilter("cn=*");
		config.setGroupSearchBase("OU=HdpGroups,OU=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;OU=Groups1,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=*");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(false);
		ldapBuilder.init();
		PolicyMgrUserGroupBuilderTest sink = new PolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(110, sink.getTotalUsers());
		assertEquals(0, sink.getTotalGroups());
	}
	
	@Test
	public void testDeltaUpdateSinkTotalGroups() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("cn=users,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserSearchFilter("");
		config.setGroupSearchBase("OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(false);
		config.setDeltaSync(true);
		//UserGroupSource ldapDeltaBuilder = config.getUserGroupSource();
		ldapBuilder = config.getUserGroupSource();
		ldapBuilder.init();
		LdapPolicyMgrUserGroupBuilderTest sink = new LdapPolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(10, sink.getTotalGroups());
	}
	
	@Test
	public void testDeltaUpdateSinkMultipleOUGroups() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("cn=users,DC=ranger,DC=qe,DC=hortonworks,DC=com;ou=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;ou=BusinessUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setUserSearchFilter("cn=*");
		config.setGroupSearchBase("OU=HdpGroups,OU=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=*Group10");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(false);
		config.setDeltaSync(true);
		ldapBuilder = config.getUserGroupSource();
		ldapBuilder.init();
		LdapPolicyMgrUserGroupBuilderTest sink = new LdapPolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(111, sink.getTotalUsers());
		assertEquals(2, sink.getTotalGroups());
	}
	
	@Test
	public void testDeltaGroupBasedWithUserFilter() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("DC=ranger,DC=qe,DC=hortonworks,DC=com;");
		config.setUserSearchFilter("cn=User*");
		config.setGroupSearchBase("OU=HdpGroups,OU=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=*Group10");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(true);
		config.setUserSearchEnabled(true);
		config.setDeltaSync(true);
		ldapBuilder = config.getUserGroupSource();
		ldapBuilder.init();
		LdapPolicyMgrUserGroupBuilderTest sink = new LdapPolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(1, sink.getTotalUsers());
		assertEquals(2, sink.getTotalGroups());
	}

	@Test
	public void testDeltaGroupBasedWithNoUsers() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("DC=ranger,DC=qe,DC=hortonworks,DC=com;");
		config.setUserSearchFilter("cn=*");
		config.setGroupSearchBase("OU=HdpGroups,OU=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=Group2*");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(true);
		config.setUserSearchEnabled(true);
		config.setDeltaSync(true);
		ldapBuilder = config.getUserGroupSource();
		ldapBuilder.init();
		LdapPolicyMgrUserGroupBuilderTest sink = new LdapPolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(0, sink.getTotalUsers());
		assertEquals(2, sink.getTotalGroups());
	}
	
	@Test
	public void testDeltaGBWithUserSearchDisabled() throws Throwable {
		config.setUserNameAttribute("sAMAccountName");
		config.setUserSearchBase("DC=ranger,DC=qe,DC=hortonworks,DC=com;");
		config.setUserSearchFilter("cn=User*");
		config.setGroupSearchBase("OU=HdpGroups,OU=HadoopUsers,DC=ranger,DC=qe,DC=hortonworks,DC=com;OU=Groups,DC=ranger,DC=qe,DC=hortonworks,DC=com");
		config.setGroupSearchFilter("cn=*Group10");
		config.setUserGroupMemberAttributeName("member");
		config.setUserObjectClass("organizationalPerson");
		config.setGroupObjectClass("groupOfNames");
		config.setGroupSearchEnabled(true);
		config.setGroupSearchFirstEnabled(true);
		config.setUserSearchEnabled(false);
		config.setDeltaSync(true);
		ldapBuilder = config.getUserGroupSource();
		ldapBuilder.init();
		LdapPolicyMgrUserGroupBuilderTest sink = new LdapPolicyMgrUserGroupBuilderTest();
		sink.init();
		ldapBuilder.updateSink(sink);
		assertEquals(3, sink.getTotalUsers());
		assertEquals(2, sink.getTotalGroups());
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
