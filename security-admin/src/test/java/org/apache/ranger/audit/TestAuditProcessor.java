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

package org.apache.ranger.audit;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.AuditAsyncQueue;
import org.apache.ranger.audit.provider.AuditBatchProcessor;
import org.apache.ranger.audit.provider.AuditDestination;
import org.apache.ranger.audit.provider.AuditFileSpool;
import org.apache.ranger.audit.provider.AuditProvider;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.audit.provider.BaseAuditProvider;
import org.apache.ranger.audit.provider.FileAuditDestination;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.provider.MultiDestAuditProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAuditProcessor {

	private static final Log logger = LogFactory
			.getLog(TestAuditProcessor.class);

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	static private int seqNum = 0;

	@Test
	public void testAuditAsyncQueue() {
		logger.debug("testAuditAsyncQueue()...");
		TestConsumer testConsumer = new TestConsumer();
		AuditAsyncQueue queue = new AuditAsyncQueue(testConsumer);
		Properties props = new Properties();
		queue.init(props);

		queue.start();

		int messageToSend = 10;
		for (int i = 0; i < messageToSend; i++) {
			queue.log(createEvent());
		}
		queue.stop();
		queue.waitToComplete();
		// Let's wait for second
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// ignore
		}
		assertEquals(messageToSend, testConsumer.getCountTotal());
		assertEquals(messageToSend, testConsumer.getSumTotal());
		assertNull("Event not in sequnce", testConsumer.isInSequence());
	}

	@Test
	public void testMultipleQueue() {
		logger.debug("testAuditAsyncQueue()...");
		int destCount = 3;
		TestConsumer[] testConsumer = new TestConsumer[destCount];

		MultiDestAuditProvider multiQueue = new MultiDestAuditProvider();
		for (int i = 0; i < destCount; i++) {
			testConsumer[i] = new TestConsumer();
			multiQueue.addAuditProvider(testConsumer[i]);
		}

		AuditAsyncQueue queue = new AuditAsyncQueue(multiQueue);
		Properties props = new Properties();
		queue.init(props);
		queue.start();

		int messageToSend = 10;
		for (int i = 0; i < messageToSend; i++) {
			queue.log(createEvent());
		}
		queue.stop();
		queue.waitToComplete();
		// Let's wait for second
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// ignore
		}
		for (int i = 0; i < destCount; i++) {
			assertEquals("consumer" + i, messageToSend,
					testConsumer[i].getCountTotal());
			assertEquals("consumer" + i, messageToSend,
					testConsumer[i].getSumTotal());

		}
	}

	@Test
	public void testAuditBatchProcessorBySize() {
		logger.debug("testAuditBatchProcessor()...");
		int messageToSend = 10;

		String basePropName = "ranger.test.batch";
		int batchSize = messageToSend / 3;
		int expectedBatchSize = batchSize
				+ (batchSize * 3 < messageToSend ? 1 : 0);
		int queueSize = messageToSend * 2;
		int intervalMS = messageToSend * 100; // Deliberately big interval
		Properties props = new Properties();
		props.put(basePropName + "." + BaseAuditProvider.PROP_BATCH_SIZE, ""
				+ batchSize);
		props.put(basePropName + "." + BaseAuditProvider.PROP_QUEUE_SIZE, ""
				+ queueSize);
		props.put(basePropName + "." + BaseAuditProvider.PROP_BATCH_INTERVAL,
				"" + intervalMS);

		TestConsumer testConsumer = new TestConsumer();
		AuditBatchProcessor queue = new AuditBatchProcessor(testConsumer);
		queue.init(props, basePropName);
		queue.start();

		for (int i = 0; i < messageToSend; i++) {
			queue.log(createEvent());

		}
		// Let's wait for second
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// ignore
		}

		queue.waitToComplete();
		queue.stop();
		queue.waitToComplete();

		assertEquals("Total count", messageToSend, testConsumer.getCountTotal());
		assertEquals("Total sum", messageToSend, testConsumer.getSumTotal());
		assertEquals("Total batch", expectedBatchSize,
				testConsumer.getBatchCount());
		assertNull("Event not in sequnce", testConsumer.isInSequence());

	}

	@Test
	public void testAuditBatchProcessorByTime() {
		logger.debug("testAuditBatchProcessor()...");

		int messageToSend = 10;

		String basePropName = "ranger.test.batch";
		int batchSize = messageToSend * 2; // Deliberately big size
		int queueSize = messageToSend * 2;
		int intervalMS = (1000 / messageToSend) * 3; // e.g (1000/10 * 3) = 300
														// ms
		int pauseMS = 1000 / messageToSend + 3; // e.g. 1000/10 -5 = 95ms
		int expectedBatchSize = (messageToSend * pauseMS) / intervalMS + 1;

		Properties props = new Properties();
		props.put(basePropName + "." + BaseAuditProvider.PROP_BATCH_SIZE, ""
				+ batchSize);
		props.put(basePropName + "." + BaseAuditProvider.PROP_QUEUE_SIZE, ""
				+ queueSize);
		props.put(basePropName + "." + BaseAuditProvider.PROP_BATCH_INTERVAL,
				"" + intervalMS);

		TestConsumer testConsumer = new TestConsumer();
		AuditBatchProcessor queue = new AuditBatchProcessor(testConsumer);
		queue.init(props, basePropName);
		queue.start();

		for (int i = 0; i < messageToSend; i++) {
			queue.log(createEvent());
			try {
				Thread.sleep(pauseMS);
			} catch (InterruptedException e) {
				// ignore
			}
		}
		// Let's wait for second
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// ignore
		}
		queue.waitToComplete();
		queue.stop();
		queue.waitToComplete();

		assertEquals("Total count", messageToSend, testConsumer.getCountTotal());
		assertEquals("Total sum", messageToSend, testConsumer.getSumTotal());
		assertEquals("Total batch", expectedBatchSize,
				testConsumer.getBatchCount());
		assertNull("Event not in sequnce", testConsumer.isInSequence());
	}

	@Test
	public void testAuditBatchProcessorDestDown() {
		logger.debug("testAuditBatchProcessorDestDown()...");
		int messageToSend = 10;

		String basePropName = "ranger.test.batch";
		int batchSize = messageToSend / 3;
		int queueSize = messageToSend * 2;
		int intervalMS = Integer.MAX_VALUE; // Deliberately big interval
		Properties props = new Properties();
		props.put(basePropName + "." + BaseAuditProvider.PROP_NAME,
				"testAuditBatchProcessorDestDown");

		props.put(basePropName + "." + BaseAuditProvider.PROP_BATCH_SIZE, ""
				+ batchSize);
		props.put(basePropName + "." + BaseAuditProvider.PROP_QUEUE_SIZE, ""
				+ queueSize);
		props.put(basePropName + "." + BaseAuditProvider.PROP_BATCH_INTERVAL,
				"" + intervalMS);

		// Enable File Spooling
		props.put(basePropName + "." + "filespool.enable", "" + true);
		props.put(basePropName + "." + "filespool.dir", "target");

		TestConsumer testConsumer = new TestConsumer();
		testConsumer.isDown = true;

		AuditBatchProcessor queue = new AuditBatchProcessor(testConsumer);
		queue.init(props, basePropName);
		queue.start();

		for (int i = 0; i < messageToSend; i++) {
			queue.log(createEvent());

		}
		// Let's wait for second
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// ignore
		}

		queue.waitToComplete(5000);
		queue.stop();
		queue.waitToComplete();

		assertEquals("Total count", 0, testConsumer.getCountTotal());
		assertEquals("Total sum", 0, testConsumer.getSumTotal());
		assertEquals("Total batch", 0, testConsumer.getBatchCount());
		assertNull("Event not in sequnce", testConsumer.isInSequence());
	}

	//@Test
	public void testAuditBatchProcessorDestDownFlipFlop() {
		logger.debug("testAuditBatchProcessorDestDown()...");
		int messageToSend = 10;

		String basePropName = "ranger.test.batch";
		int batchSize = messageToSend / 3;
		int expectedBatchSize = batchSize
				+ (batchSize * 3 < messageToSend ? 1 : 0);
		int queueSize = messageToSend * 2;
		int intervalMS = 3000; // Deliberately big interval
		Properties props = new Properties();
		props.put(
				basePropName + "." + BaseAuditProvider.PROP_NAME,
				"testAuditBatchProcessorDestDownFlipFlop_"
						+ MiscUtil.generateUniqueId());

		props.put(basePropName + "." + BaseAuditProvider.PROP_BATCH_SIZE, ""
				+ batchSize);
		props.put(basePropName + "." + BaseAuditProvider.PROP_QUEUE_SIZE, ""
				+ queueSize);
		props.put(basePropName + "." + BaseAuditProvider.PROP_BATCH_INTERVAL,
				"" + intervalMS);

		// Enable File Spooling
		int destRetryMS = 10;
		props.put(
				basePropName + "." + BaseAuditProvider.PROP_FILE_SPOOL_ENABLE,
				"" + true);
		props.put(
				basePropName + "." + AuditFileSpool.PROP_FILE_SPOOL_LOCAL_DIR,
				"target");
		props.put(basePropName + "."
				+ AuditFileSpool.PROP_FILE_SPOOL_DEST_RETRY_MS, ""
				+ destRetryMS);

		TestConsumer testConsumer = new TestConsumer();
		testConsumer.isDown = false;

		AuditBatchProcessor queue = new AuditBatchProcessor(testConsumer);
		queue.init(props, basePropName);
		queue.start();

		try {
			queue.log(createEvent());
			queue.log(createEvent());
			queue.log(createEvent());
			Thread.sleep(1000);
			testConsumer.isDown = true;
			Thread.sleep(1000);
			queue.log(createEvent());
			queue.log(createEvent());
			queue.log(createEvent());
			Thread.sleep(1000);
			testConsumer.isDown = false;
			Thread.sleep(1000);
			queue.log(createEvent());
			queue.log(createEvent());
			queue.log(createEvent());
			Thread.sleep(1000);
			testConsumer.isDown = true;
			Thread.sleep(1000);
			queue.log(createEvent());
			Thread.sleep(1000);
			testConsumer.isDown = false;
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// ignore
		}
		// Let's wait for second
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// ignore
		}

		queue.waitToComplete(5000);
		queue.stop();
		queue.waitToComplete();

		assertEquals("Total count", messageToSend, testConsumer.getCountTotal());
		assertEquals("Total sum", messageToSend, testConsumer.getSumTotal());
		assertNull("Event not in sequnce", testConsumer.isInSequence());

	}

	/**
	 * See if we recover after restart
	 */
	public void testAuditBatchProcessorDestDownRestart() {
		logger.debug("testAuditBatchProcessorDestDownRestart()...");
		int messageToSend = 10;

		String basePropName = "ranger.test.batch";
		int batchSize = messageToSend / 3;
		int queueSize = messageToSend * 2;
		int intervalMS = 3000; // Deliberately big interval
		int maxArchivedFiles = 1;
		Properties props = new Properties();
		props.put(
				basePropName + "." + BaseAuditProvider.PROP_NAME,
				"testAuditBatchProcessorDestDownRestart_"
						+ MiscUtil.generateUniqueId());

		props.put(basePropName + "." + BaseAuditProvider.PROP_BATCH_SIZE, ""
				+ batchSize);
		props.put(basePropName + "." + BaseAuditProvider.PROP_QUEUE_SIZE, ""
				+ queueSize);
		props.put(basePropName + "." + BaseAuditProvider.PROP_BATCH_INTERVAL,
				"" + intervalMS);

		// Enable File Spooling
		int destRetryMS = 10;
		props.put(
				basePropName + "." + BaseAuditProvider.PROP_FILE_SPOOL_ENABLE,
				"" + true);
		props.put(
				basePropName + "." + AuditFileSpool.PROP_FILE_SPOOL_LOCAL_DIR,
				"target");
		props.put(basePropName + "."
				+ AuditFileSpool.PROP_FILE_SPOOL_DEST_RETRY_MS, ""
				+ destRetryMS);
		props.put(basePropName + "."
				+ AuditFileSpool.PROP_FILE_SPOOL_ARCHIVE_MAX_FILES_COUNT, ""
				+ maxArchivedFiles);

		TestConsumer testConsumer = new TestConsumer();
		testConsumer.isDown = true;

		AuditBatchProcessor queue = new AuditBatchProcessor(testConsumer);
		queue.init(props, basePropName);
		queue.start();

		for (int i = 0; i < messageToSend; i++) {
			queue.log(createEvent());

		}
		// Let's wait for second or two
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// ignore
		}

		queue.waitToComplete(5000);
		queue.stop();
		queue.waitToComplete();

		testConsumer.isDown = true;

		// Let's wait for second or two
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// ignore
		}
		
		
		// Let's now recreate the objects
		testConsumer = new TestConsumer();

		queue = new AuditBatchProcessor(testConsumer);
		queue.init(props, basePropName);
		queue.start();

		// Let's wait for second
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// ignore
		}

		queue.waitToComplete(5000);
		queue.stop();
		queue.waitToComplete();

		assertEquals("Total count", messageToSend, testConsumer.getCountTotal());
		assertEquals("Total sum", messageToSend, testConsumer.getSumTotal());
		assertNull("Event not in sequnce", testConsumer.isInSequence());

	}

	@Test
	public void testFileDestination() {
		logger.debug("testFileDestination()...");

		int messageToSend = 10;
		int batchSize = messageToSend / 3;
		int queueSize = messageToSend * 2;
		int intervalMS = 500; // Should be less than final sleep time

		String logFolderName = "target/testFileDestination";
		File logFolder = new File(logFolderName);
		String logFileName = "test_ranger_audit.log";
		File logFile = new File(logFolder, logFileName);

		Properties props = new Properties();
		// Destination
		String filePropPrefix = AuditProviderFactory.AUDIT_DEST_BASE + ".file";
		props.put(filePropPrefix, "enable");
		props.put(filePropPrefix + "." + BaseAuditProvider.PROP_NAME, "file");
		props.put(filePropPrefix + "."
				+ FileAuditDestination.PROP_FILE_LOCAL_DIR, logFolderName);
		props.put(filePropPrefix + "."
				+ FileAuditDestination.PROP_FILE_LOCAL_FILE_NAME_FORMAT,
				"%app-type%_ranger_audit.log");
		props.put(filePropPrefix + "."
				+ FileAuditDestination.PROP_FILE_FILE_ROLLOVER, "" + 10);

		props.put(filePropPrefix + "." + BaseAuditProvider.PROP_QUEUE, "batch");
		String batchPropPrefix = filePropPrefix + "." + "batch";

		props.put(batchPropPrefix + "." + BaseAuditProvider.PROP_BATCH_SIZE, ""
				+ batchSize);
		props.put(batchPropPrefix + "." + BaseAuditProvider.PROP_QUEUE_SIZE, ""
				+ queueSize);
		props.put(
				batchPropPrefix + "." + BaseAuditProvider.PROP_BATCH_INTERVAL,
				"" + intervalMS);

		// Enable File Spooling
		int destRetryMS = 10;
		props.put(batchPropPrefix + "."
				+ BaseAuditProvider.PROP_FILE_SPOOL_ENABLE, "" + true);
		props.put(batchPropPrefix + "."
				+ AuditFileSpool.PROP_FILE_SPOOL_LOCAL_DIR, "target");
		props.put(batchPropPrefix + "."
				+ AuditFileSpool.PROP_FILE_SPOOL_DEST_RETRY_MS, ""
				+ destRetryMS);

		AuditProviderFactory factory = AuditProviderFactory.getInstance();
		factory.init(props, "test");

		// FileAuditDestination fileDest = new FileAuditDestination();
		// fileDest.init(props, filePropPrefix);
		//
		// AuditBatchProcessor queue = new AuditBatchProcessor(fileDest);
		// queue.init(props, batchPropPrefix);
		// queue.start();

		AuditProvider queue = factory.getProvider();

		for (int i = 0; i < messageToSend; i++) {
			queue.log(createEvent());
		}
		// Let's wait for second
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// ignore
		}

		queue.waitToComplete();
		queue.stop();
		queue.waitToComplete();

		assertTrue("File created", logFile.exists());
		try {
			List<AuthzAuditEvent> eventList = new ArrayList<AuthzAuditEvent>();
			int totalSum = 0;
			BufferedReader br = new BufferedReader(new FileReader(logFile));
			String line;
			int lastSeq = -1;
			boolean outOfSeq = false;
			while ((line = br.readLine()) != null) {
				AuthzAuditEvent event = MiscUtil.fromJson(line,
						AuthzAuditEvent.class);
				eventList.add(event);
				totalSum += event.getFrequencyCount();
				if (event.getSeqNum() <= lastSeq) {
					outOfSeq = true;
				}
			}
			br.close();
			assertEquals("Total count", messageToSend, eventList.size());
			assertEquals("Total sum", messageToSend, totalSum);
			assertFalse("Event not in sequnce", outOfSeq);

		} catch (Throwable e) {
			logger.error("Error opening file for reading.", e);
			assertTrue("Error reading file. fileName=" + logFile + ", error="
					+ e.toString(), true);
		}

	}

	private AuthzAuditEvent createEvent() {
		AuthzAuditEvent event = new AuthzAuditEvent();
		event.setSeqNum(++seqNum);
		return event;
	}

	class TestConsumer extends AuditDestination {

		int countTotal = 0;
		int sumTotal = 0;
		int batchCount = 0;
		String providerName = getClass().getName();
		boolean isDown = false;
		int batchSize = 3;

		List<AuthzAuditEvent> eventList = new ArrayList<AuthzAuditEvent>();

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.ranger.audit.provider.AuditProvider#log(org.apache.ranger
		 * .audit.model.AuditEventBase)
		 */
		@Override
		public boolean log(AuditEventBase event) {
			if (isDown) {
				return false;
			}
			countTotal++;
			if (event instanceof AuthzAuditEvent) {
				AuthzAuditEvent azEvent = (AuthzAuditEvent) event;
				sumTotal += azEvent.getFrequencyCount();
				logger.info("EVENT:" + event);
				eventList.add(azEvent);
			}
			return true;
		}

		@Override
		public boolean log(Collection<AuditEventBase> events) {
			if (isDown) {
				return false;
			}
			batchCount++;
			for (AuditEventBase event : events) {
				log(event);
			}
			return true;
		}

		@Override
		public boolean logJSON(String jsonStr) {
			if (isDown) {
				return false;
			}
			countTotal++;
			AuthzAuditEvent event = MiscUtil.fromJson(jsonStr,
					AuthzAuditEvent.class);
			sumTotal += event.getFrequencyCount();
			logger.info("JSON:" + jsonStr);
			eventList.add(event);
			return true;
		}

		@Override
		public boolean logJSON(Collection<String> events) {
			if (isDown) {
				return false;
			}
			for (String event : events) {
				logJSON(event);
			}
			return true;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.ranger.audit.provider.AuditProvider#init(java.util.Properties
		 * )
		 */
		@Override
		public void init(Properties prop) {
			// Nothing to do here
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.ranger.audit.provider.AuditProvider#start()
		 */
		@Override
		public void start() {
			// Nothing to do here
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.ranger.audit.provider.AuditProvider#stop()
		 */
		@Override
		public void stop() {
			// Nothing to do here
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.ranger.audit.provider.AuditProvider#waitToComplete()
		 */
		@Override
		public void waitToComplete() {
		}

		@Override
		public int getMaxBatchSize() {
			return batchSize;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.ranger.audit.provider.AuditProvider#isFlushPending()
		 */
		@Override
		public boolean isFlushPending() {
			return false;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.ranger.audit.provider.AuditProvider#getLastFlushTime()
		 */
		@Override
		public long getLastFlushTime() {
			return 0;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.ranger.audit.provider.AuditProvider#flush()
		 */
		@Override
		public void flush() {
			// Nothing to do here
		}

		public int getCountTotal() {
			return countTotal;
		}

		public int getSumTotal() {
			return sumTotal;
		}

		public int getBatchCount() {
			return batchCount;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.ranger.audit.provider.AuditProvider#init(java.util.Properties
		 * , java.lang.String)
		 */
		@Override
		public void init(Properties prop, String basePropertyName) {

		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.ranger.audit.provider.AuditProvider#waitToComplete(long)
		 */
		@Override
		public void waitToComplete(long timeout) {

		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.ranger.audit.provider.AuditProvider#getName()
		 */
		@Override
		public String getName() {
			return providerName;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.ranger.audit.provider.AuditProvider#isDrain()
		 */
		@Override
		public boolean isDrain() {
			return false;
		}

		// Local methods
		public AuthzAuditEvent isInSequence() {
			int lastSeq = -1;
			for (AuthzAuditEvent event : eventList) {
				if (event.getSeqNum() <= lastSeq) {
					return event;
				}
			}
			return null;
		}
	}
}
