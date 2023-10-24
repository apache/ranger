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

package org.apache.ranger.metrics.source;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.ranger.metrics.RangerMetricsSystemWrapper;
import org.apache.ranger.server.tomcat.EmbeddedServerMetricsCollector;
import org.junit.*;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRangerMetricsContainerSource {

    private static final String CONTAINER_METRIC_SOURCE_NAME = "RangerContainer";
    private static RangerMetricsSystemWrapper rangerMetricsSystemWrapper;

    private EmbeddedServerMetricsCollector embeddedServerMetricsCollector;

    private static MetricsSystem metricsSystem;

    public TestRangerMetricsContainerSource(){
    }

    @BeforeClass
    public static void init(){

        metricsSystem = DefaultMetricsSystem.instance();
        TestRangerMetricsContainerSource.rangerMetricsSystemWrapper = new RangerMetricsSystemWrapper();
        TestRangerMetricsContainerSource.rangerMetricsSystemWrapper.init("test", null, (List)null);
    }

    @AfterClass
    public static void tearDownAfterClass() {
        metricsSystem.shutdown();
    }

    // Without proper start of EmbeddedServer, embeddedServerMetricsCollector will be returned null.
    // That's why, mocked instance should be injected here.
    @Before
    public void before(){
        embeddedServerMetricsCollector = mock(EmbeddedServerMetricsCollector.class);
        ((RangerMetricsContainerSource)DefaultMetricsSystem.instance().getSource(CONTAINER_METRIC_SOURCE_NAME)).setEmbeddedServerMetricsCollector(embeddedServerMetricsCollector);
    }

    // Resetting it back to original state.
    @After
    public void after(){
        ((RangerMetricsContainerSource)DefaultMetricsSystem.instance().getSource(CONTAINER_METRIC_SOURCE_NAME)).setEmbeddedServerMetricsCollector(null);
    }


    /*
     * Test Case:
     *      This case verifies the tomcat metric collection when RangerMetricsContainerSource gets executed to collect the metrics.
     *      Mocking: Mocked the EmbeddedServerMetricsCollector as it gets initialised when Tomcat server starts.
     *               Simulated what to return when these APIs get called.
     *      Expected output: After metric collection through metric system, on fetching the json metrics it should return the stats.
     *      Note: DefaultMetricSystem is singleton and is being used by the RangerMetricsContainerSource
     */

    @Test
    public void testContainerMetricsCollection(){

        when(embeddedServerMetricsCollector.getActiveConnectionCount()).thenReturn(1L);
        when(embeddedServerMetricsCollector.getMaxAllowedConnection()).thenReturn(8192L);
        when(embeddedServerMetricsCollector.getConnectionAcceptCount()).thenReturn(100);
        when(embeddedServerMetricsCollector.getMaxContainerThreadsCount()).thenReturn(200);
        when(embeddedServerMetricsCollector.getMinSpareContainerThreadsCount()).thenReturn(10);
        when(embeddedServerMetricsCollector.getActiveContainerThreadsCount()).thenReturn(2);
        when(embeddedServerMetricsCollector.getConnectionTimeout()).thenReturn(60000L);
        when(embeddedServerMetricsCollector.getKeepAliveTimeout()).thenReturn(60000L);
        when(embeddedServerMetricsCollector.getTotalContainerThreadsCount()).thenReturn(15);

        metricsSystem.publishMetricsNow();

        Assert.assertEquals(1L,  rangerMetricsSystemWrapper.getRangerMetrics().get("RangerWebContainer").get("ActiveConnectionsCount"));
        Assert.assertEquals(60000L,  rangerMetricsSystemWrapper.getRangerMetrics().get("RangerWebContainer").get("ConnectionTimeout"));
        Assert.assertEquals(200,  rangerMetricsSystemWrapper.getRangerMetrics().get("RangerWebContainer").get("MaxWorkerThreadsCount"));
        Assert.assertEquals(15,  rangerMetricsSystemWrapper.getRangerMetrics().get("RangerWebContainer").get("TotalWorkerThreadsCount"));
        Assert.assertEquals(60000L,  rangerMetricsSystemWrapper.getRangerMetrics().get("RangerWebContainer").get("KeepAliveTimeout"));
        Assert.assertEquals(2,  rangerMetricsSystemWrapper.getRangerMetrics().get("RangerWebContainer").get("ActiveWorkerThreadsCount"));
        Assert.assertEquals(100,  rangerMetricsSystemWrapper.getRangerMetrics().get("RangerWebContainer").get("ConnectionAcceptCount"));
        Assert.assertEquals(10,  rangerMetricsSystemWrapper.getRangerMetrics().get("RangerWebContainer").get("MinSpareWorkerThreadsCount"));
        Assert.assertEquals(8192L,  rangerMetricsSystemWrapper.getRangerMetrics().get("RangerWebContainer").get("MaxConnectionsCount"));

    }

}
