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

package org.apache.ranger.audit.provider;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.audit.model.AuditEventBase;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class Log4jAuditProvider extends BaseAuditProvider {

	private static final Log LOG      = LogFactory.getLog(Log4jAuditProvider.class);
	private static final Log AUDITLOG = LogFactory.getLog("xaaudit." + Log4jAuditProvider.class.getName());

	public static final String AUDIT_LOG4J_IS_ASYNC_PROP           = "xasecure.audit.log4j.is.async";
	public static final String AUDIT_LOG4J_MAX_QUEUE_SIZE_PROP     = "xasecure.audit.log4j.async.max.queue.size" ;
	public static final String AUDIT_LOG4J_MAX_FLUSH_INTERVAL_PROP = "xasecure.audit.log4j.async.max.flush.interval.ms";

	private Gson mGsonBuilder = null;

	public Log4jAuditProvider() {
		LOG.info("Log4jAuditProvider: creating..");
	}

	@Override
	public void init(Properties props) {
		LOG.info("Log4jAuditProvider.init()");

		super.init(props);

		try {
			mGsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").create();
		} catch(Throwable excp) {
			LOG.warn("Log4jAuditProvider.init(): failed to create GsonBuilder object. events will be formated using toString(), instead of Json", excp);
		}
	}

	@Override
	public void log(AuditEventBase event) {
		if(! AUDITLOG.isInfoEnabled())
			return;
		
		if(event != null) {
			String eventStr = mGsonBuilder != null ? mGsonBuilder.toJson(event) : event.toString();

			AUDITLOG.info(eventStr);
		}
	}

	@Override
	public void start() {
		// intentionally left empty
	}

	@Override
	public void stop() {
		// intentionally left empty
	}

	@Override
    public void waitToComplete() {
		// intentionally left empty
	}

	@Override
	public boolean isFlushPending() {
		return false;
	}
	
	@Override
	public long getLastFlushTime() {
		return 0;
	}

	@Override
	public void flush() {
		// intentionally left empty
	}
}
