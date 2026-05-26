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

package org.apache.ranger.audit.dispatcher.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.ranger.audit.destination.OpenSearchAuditDestination;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.audit.utils.AuditServerLogFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class AuditOpenSearchDispatcher extends AuditDispatcherBase {
    private static final Logger LOG = LoggerFactory.getLogger(AuditOpenSearchDispatcher.class);
    private static final String DEFAULT_GROUP = "ranger_audit_opensearch_dispatcher_group";
    private static final long   RETRY_SLEEP_MS = 5000L;

    private final OpenSearchAuditDestination openSearchAuditDestination;

    public AuditOpenSearchDispatcher(final Properties props, final String propPrefix) throws Exception {
        super(props, propPrefix, DEFAULT_GROUP);

        this.openSearchAuditDestination = new OpenSearchAuditDestination();

        init(props, propPrefix);
    }

    @Override
    protected final String getDispatcherName() {
        return "OPENSEARCH";
    }

    @Override
    protected final DispatcherWorker createDispatcherWorker(final String workerId, final List<Integer> assignedPartitions) {
        return new OpenSearchDispatcherWorker(workerId, assignedPartitions);
    }

    @Override
    protected final void shutdownDestination() {
        if (openSearchAuditDestination != null) {
            try {
                openSearchAuditDestination.stop();
            } catch (Exception e) {
                LOG.error("Error shutting down OpenSearch destination handler", e);
            }
        }
    }

    private void init(final Properties props, final String propPrefix) {
        LOG.info("==> AuditOpenSearchDispatcher.init()");

        openSearchAuditDestination.init(props, propPrefix);

        this.dispatcherThreadCount = MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_THREAD_COUNT, 1);
        this.offsetCommitStrategy  = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_OFFSET_COMMIT_STRATEGY, AuditServerConstants.DEFAULT_OFFSET_COMMIT_STRATEGY);
        this.offsetCommitInterval  = MiscUtil.getLongProperty(props, propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_OFFSET_COMMIT_INTERVAL, AuditServerConstants.DEFAULT_OFFSET_COMMIT_INTERVAL_MS);

        AuditServerLogFormatter.builder("AuditOpenSearchDispatcher Offset Management Configuration")
                .add("Thread Count", dispatcherThreadCount)
                .add("Commit Strategy", offsetCommitStrategy)
                .add("Commit Interval (ms)", offsetCommitInterval + " (manual mode only)")
                .logInfo(LOG);

        LOG.info("<== AuditOpenSearchDispatcher.init()");
    }

    private void processMessageBatch(final Collection<String> audits) throws Exception {
        boolean processed = audits != null && !audits.isEmpty() && openSearchAuditDestination.logJSON(audits);

        if (!processed) {
            throw new Exception("Failure in sending audits into OpenSearch");
        }
    }

    private class OpenSearchDispatcherWorker extends DispatcherWorker {
        OpenSearchDispatcherWorker(final String workerId, final List<Integer> assignedPartitions) {
            super(workerId, assignedPartitions);
        }

        @Override
        protected void processRecordBatch(final ConsumerRecords<String, String> records) {
            for (TopicPartition tp : records.partitions()) {
                List<ConsumerRecord<String, String>> tpRecords = records.records(tp);

                if (tpRecords.isEmpty()) {
                    continue;
                }

                try {
                    List<String> auditBatch = tpRecords.stream().map(ConsumerRecord::value).collect(Collectors.toList());
                    processMessageBatch(auditBatch);

                    ConsumerRecord<String, String> last = tpRecords.get(tpRecords.size() - 1);
                    pendingOffsets.put(tp, new OffsetAndMetadata(last.offset() + 1));
                    messagesProcessedSinceLastCommit.addAndGet(tpRecords.size());
                } catch (Exception e) {
                    LOG.error("Error processing batch in worker '{}', partition={}, batch size: {}", workerId, tp, tpRecords.size(), e);

                    ConsumerRecord<String, String> first = tpRecords.get(0);
                    pendingOffsets.put(tp, new OffsetAndMetadata(first.offset()));

                    try {
                        workerDispatcher.seek(tp, first.offset());
                    } catch (Exception seekEx) {
                        LOG.error("Failed to seek to offset {} for partition {} after OpenSearch batch error", first.offset(), tp, seekEx);
                    }

                    try {
                        Thread.sleep(RETRY_SLEEP_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }
}
