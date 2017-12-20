/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.services.knox;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;
import org.apache.hadoop.gateway.GatewayServer;
import org.apache.hadoop.gateway.GatewayTestConfig;
import org.apache.hadoop.gateway.security.ldap.SimpleLdapDirectoryServer;
import org.apache.hadoop.gateway.services.DefaultGatewayServices;
import org.apache.hadoop.gateway.services.ServiceLifecycleException;
import org.apache.hadoop.test.mock.MockServer;
import org.apache.http.HttpStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mycila.xmltool.XMLDoc;
import com.mycila.xmltool.XMLTag;

import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;

/**
 * Test Apache Knox secured by Apache Ranger.
 */
public class KnoxRangerTest {

    private static GatewayTestConfig config;
    private static GatewayServer gateway;
    private static SimpleLdapDirectoryServer ldap;
    private static TcpTransport ldapTransport;
    private static MockServer hdfsServer;
    private static MockServer stormServer;
    private static MockServer hbaseServer;
    private static MockServer kafkaServer;
    private static MockServer solrServer;

    @BeforeClass
    public static void setupSuite() throws Exception {
        setupLdap();
        hdfsServer = new MockServer( "hdfs", true );
        stormServer = new MockServer( "storm", true );
        hbaseServer = new MockServer( "hbase", true );
        kafkaServer = new MockServer( "kafka", true );
        solrServer = new MockServer( "solr", true );

        setupGateway();
    }

    @AfterClass
    public static void cleanupSuite() throws Exception {
        gateway.stop();

        FileUtils.deleteQuietly( new File( config.getGatewayTopologyDir() ) );
        FileUtils.deleteQuietly( new File( config.getGatewayConfDir() ) );
        FileUtils.deleteQuietly( new File( config.getGatewaySecurityDir() ) );
        FileUtils.deleteQuietly( new File( config.getGatewayDeploymentDir() ) );
        FileUtils.deleteQuietly( new File( config.getGatewayDataDir() ) );

        hdfsServer.stop();
        stormServer.stop();
        hbaseServer.stop();
        kafkaServer.stop();
        solrServer.stop();

        ldap.stop( true );
    }

    private static void setupLdap() throws Exception {
        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            basedir = new File(".").getCanonicalPath();
        }
        Path path = FileSystems.getDefault().getPath(basedir, "/src/test/resources/users.ldif");
        ldapTransport = new TcpTransport( 0 );
        ldap = new SimpleLdapDirectoryServer( "dc=hadoop,dc=apache,dc=org", path.toFile(), ldapTransport );
        ldap.start();
    }

    private static void setupGateway() throws Exception {

        File targetDir = new File( System.getProperty( "user.dir" ), "target" );
        File gatewayDir = new File( targetDir, "gateway-home-" + UUID.randomUUID() );
        Assert.assertTrue(gatewayDir.mkdirs());

        config = new GatewayTestConfig();
        config.setGatewayHomeDir( gatewayDir.getAbsolutePath() );

        config.setGatewayServicesDir(targetDir.getPath() + File.separator + "services");

        File topoDir = new File( config.getGatewayTopologyDir() );
        Assert.assertTrue(topoDir.mkdirs());

        File deployDir = new File( config.getGatewayDeploymentDir() );
        Assert.assertTrue(deployDir.mkdirs());

        File descriptor = new File( topoDir, "cluster.xml" );
        FileOutputStream stream = new FileOutputStream( descriptor );
        createTopology().toStream( stream );
        stream.close();

        DefaultGatewayServices srvcs = new DefaultGatewayServices();
        Map<String,String> options = new HashMap<>();
        options.put( "persist-master", "false" );
        options.put( "master", "password" );
        try {
            srvcs.init( config, options );
        } catch ( ServiceLifecycleException e ) {
            e.printStackTrace(); // I18N not required.
        }

        gateway = GatewayServer.startGateway( config, srvcs );
    }

    /**
     * Creates a topology that is deployed to the gateway instance for the test suite.
     * Note that this topology is shared by all of the test methods in this suite.
     * @return A populated XML structure for a topology file.
     */
    private static XMLTag createTopology() {
        XMLTag xml = XMLDoc.newDocument( true )
            .addRoot( "topology" )
            .addTag( "gateway" )
            .addTag( "provider" )
            .addTag( "role" ).addText( "webappsec" )
            .addTag("name").addText("WebAppSec")
            .addTag("enabled").addText("true")
            .addTag( "param" )
            .addTag("name").addText("csrf.enabled")
            .addTag("value").addText("true").gotoParent().gotoParent()
            .addTag("provider")
            .addTag("role").addText("authentication")
            .addTag("name").addText("ShiroProvider")
            .addTag("enabled").addText("true")
            .addTag( "param" )
            .addTag("name").addText("main.ldapRealm")
            .addTag("value").addText("org.apache.hadoop.gateway.shirorealm.KnoxLdapRealm").gotoParent()
            .addTag( "param" )
            .addTag( "name" ).addText( "main.ldapRealm.userDnTemplate" )
            .addTag( "value" ).addText( "uid={0},ou=people,dc=hadoop,dc=apache,dc=org" ).gotoParent()
            .addTag( "param" )
            .addTag( "name" ).addText( "main.ldapRealm.contextFactory.url" )
            .addTag( "value" ).addText( "ldap://localhost:" + ldapTransport.getAcceptor().getLocalAddress().getPort() ).gotoParent()
            //.addTag( "value" ).addText(driver.getLdapUrl() ).gotoParent()
            .addTag( "param" )
            .addTag( "name" ).addText( "main.ldapRealm.contextFactory.authenticationMechanism" )
            .addTag( "value" ).addText( "simple" ).gotoParent()
            .addTag( "param" )
            .addTag( "name" ).addText( "urls./**" )
            .addTag( "value" ).addText( "authcBasic" ).gotoParent().gotoParent()
            .addTag("provider")
            .addTag("role").addText("identity-assertion")
            .addTag("enabled").addText("true")
            .addTag("name").addText("Default").gotoParent()
            .addTag("provider")
            .addTag( "role" ).addText( "authorization" )
            .addTag("name").addText("XASecurePDPKnox")
            .addTag( "enabled" ).addText( "true" )
            .gotoRoot()
            .addTag("service")
            .addTag("role").addText("WEBHDFS")
            .addTag("url").addText("http://localhost:" + hdfsServer.getPort()).gotoParent()
            .addTag("service")
            .addTag("role").addText("STORM")
            .addTag("url").addText("http://localhost:" + stormServer.getPort()).gotoParent()
            .addTag("service")
            .addTag("role").addText("WEBHBASE")
            .addTag("url").addText("http://localhost:" + hbaseServer.getPort()).gotoParent()
            .addTag("service")
            .addTag("role").addText("KAFKA")
            .addTag("url").addText("http://localhost:" + kafkaServer.getPort()).gotoParent()
            .addTag("service")
            .addTag("role").addText("SOLR")
            .addTag("url").addText("http://localhost:" + solrServer.getPort() + "/solr").gotoParent()
            .gotoRoot();
        return xml;
    }

    @Test
    public void testHDFSAllowed() throws IOException {
        makeWebHDFSInvocation(HttpStatus.SC_OK, "alice", "password");
    }

    @Test
    public void testHDFSNotAllowed() throws IOException {
        makeWebHDFSInvocation(HttpStatus.SC_FORBIDDEN, "bob", "password");
    }

    @Test
    public void testStormUiAllowed() throws Exception {
        makeStormUIInvocation(HttpStatus.SC_OK, "bob", "password");
    }

    @Test
    public void testStormNotUiAllowed() throws Exception {
        makeStormUIInvocation(HttpStatus.SC_FORBIDDEN, "alice", "password");
    }

    @Test
    public void testHBaseAllowed() throws Exception {
        makeHBaseInvocation(HttpStatus.SC_OK, "alice", "password");
    }

    @Test
    public void testHBaseNotAllowed() throws Exception {
        makeHBaseInvocation(HttpStatus.SC_FORBIDDEN, "bob", "password");
    }

    @Test
    public void testKafkaAllowed() throws IOException {
        makeKafkaInvocation(HttpStatus.SC_OK, "alice", "password");
    }

    @Test
    public void testKafkaNotAllowed() throws IOException {
        makeKafkaInvocation(HttpStatus.SC_FORBIDDEN, "bob", "password");
    }

    @Test
    public void testSolrAllowed() throws Exception {
        makeSolrInvocation(HttpStatus.SC_OK, "alice", "password");
    }

    @Test
    public void testSolrNotAllowed() throws Exception {
        makeSolrInvocation(HttpStatus.SC_FORBIDDEN, "bob", "password");
    }

    private void makeWebHDFSInvocation(int statusCode, String user, String password) throws IOException {

        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            basedir = new File(".").getCanonicalPath();
        }
        Path path = FileSystems.getDefault().getPath(basedir, "/src/test/resources/webhdfs-liststatus-test.json");

        hdfsServer
        .expect()
          .method( "GET" )
          .pathInfo( "/v1/hdfstest" )
          .queryParam( "op", "LISTSTATUS" )
        .respond()
          .status( HttpStatus.SC_OK )
          .content( IOUtils.toByteArray( path.toUri() ) )
          .contentType( "application/json" );

        ValidatableResponse response = given()
          .log().all()
          .auth().preemptive().basic( user, password )
          .header("X-XSRF-Header", "jksdhfkhdsf")
          .queryParam( "op", "LISTSTATUS" )
        .when()
          .get( "http://localhost:" + gateway.getAddresses()[0].getPort() + "/gateway/cluster/webhdfs" + "/v1/hdfstest" )
        .then()
          .statusCode(statusCode)
          .log().body();

        if (statusCode == HttpStatus.SC_OK) {
            response.body( "FileStatuses.FileStatus[0].pathSuffix", is ("dir") );
        }
    }

    private void makeStormUIInvocation(int statusCode, String user, String password) throws IOException {
        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            basedir = new File(".").getCanonicalPath();
        }
        Path path = FileSystems.getDefault().getPath(basedir, "/src/test/resources/cluster-configuration.json");

        stormServer
            .expect()
            .method("GET")
            .pathInfo("/api/v1/cluster/configuration")
            .respond()
            .status(HttpStatus.SC_OK)
            .content(IOUtils.toByteArray( path.toUri() ))
            .contentType("application/json");

        given()
            .auth().preemptive().basic(user, password)
            .header("X-XSRF-Header", "jksdhfkhdsf")
            .header("Accept", "application/json")
            .when().get( "http://localhost:" + gateway.getAddresses()[0].getPort() + "/gateway/cluster/storm" + "/api/v1/cluster/configuration")
            .then()
            .log().all()
            .statusCode(statusCode);

      }

    private void makeHBaseInvocation(int statusCode, String user, String password) throws IOException {
        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            basedir = new File(".").getCanonicalPath();
        }
        Path path = FileSystems.getDefault().getPath(basedir, "/src/test/resources/webhbase-table-list.xml");


        hbaseServer
        .expect()
        .method( "GET" )
        .pathInfo( "/" )
        .header( "Accept", ContentType.XML.toString() )
        .respond()
        .status( HttpStatus.SC_OK )
        .content( IOUtils.toByteArray( path.toUri() ) )
        .contentType( ContentType.XML.toString() );

        given()
            .log().all()
            .auth().preemptive().basic( user, password )
            .header("X-XSRF-Header", "jksdhfkhdsf")
            .header( "Accept", ContentType.XML.toString() )
            .when().get( "http://localhost:" + gateway.getAddresses()[0].getPort() + "/gateway/cluster/hbase" )
            .then()
            .statusCode( statusCode )
            .log().body();
    }

    private void makeKafkaInvocation(int statusCode, String user, String password) throws IOException {

        kafkaServer
        .expect()
        .method( "GET" )
        .pathInfo( "/topics" )
        .respond()
        .status( HttpStatus.SC_OK );

        given()
            .log().all()
            .auth().preemptive().basic( user, password )
            .header("X-XSRF-Header", "jksdhfkhdsf")
        .when()
            .get( "http://localhost:" + gateway.getAddresses()[0].getPort() + "/gateway/cluster/kafka" + "/topics" )
        .then()
            .statusCode(statusCode)
            .log().body();

    }

    private void makeSolrInvocation(int statusCode, String user, String password) throws IOException {
        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            basedir = new File(".").getCanonicalPath();
        }
        Path path = FileSystems.getDefault().getPath(basedir, "/src/test/resources/query_response.xml");

        solrServer
        .expect()
        .method("GET")
        .pathInfo("/solr/gettingstarted/select")
        .queryParam("q", "author_s:William+Shakespeare")
        .respond()
        .status(HttpStatus.SC_OK)
        .content(IOUtils.toByteArray( path.toUri() ))
        .contentType("application/json");

        given()
        .auth().preemptive().basic(user, password)
        .header("X-XSRF-Header", "jksdhfkhdsf")
        .header("Accept", "application/json")
        .when().get( "http://localhost:" + gateway.getAddresses()[0].getPort() + "/gateway/cluster/solr"
            + "/gettingstarted/select?q=author_s:William+Shakespeare")
        .then()
        .log().all()
        .statusCode(statusCode);

    }
}