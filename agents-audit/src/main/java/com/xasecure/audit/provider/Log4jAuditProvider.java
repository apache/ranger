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

package com.xasecure.audit.provider;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xasecure.audit.model.HBaseAuditEvent;
import com.xasecure.audit.model.HdfsAuditEvent;
import com.xasecure.audit.model.HiveAuditEvent;
import com.xasecure.audit.model.KnoxAuditEvent;
import com.xasecure.audit.model.StormAuditEvent;


public class Log4jAuditProvider implements AuditProvider {

	private static final Log LOG      = LogFactory.getLog(Log4jAuditProvider.class);
	private static final Log AUDITLOG = LogFactory.getLog("xaaudit." + Log4jAuditProvider.class);


	public Log4jAuditProvider() {
		LOG.info("Log4jAuditProvider: creating..");
	}

	@Override
	public void log(HBaseAuditEvent event) {
		if(! AUDITLOG.isInfoEnabled())
			return;

		AUDITLOG.info(event.toString());
	}

	@Override
	public void log(HdfsAuditEvent event) {
		if(! AUDITLOG.isInfoEnabled())
			return;

		AUDITLOG.info(event.toString());
	}

	@Override
	public void log(HiveAuditEvent event) {
		if(! AUDITLOG.isInfoEnabled())
			return;

		AUDITLOG.info(event.toString());
	}

	@Override
	public void log(KnoxAuditEvent event) {
		if(! AUDITLOG.isInfoEnabled())
			return;

		AUDITLOG.info(event.toString());
	}

	@Override
	public void log(StormAuditEvent event) {
		if(! AUDITLOG.isInfoEnabled())
			return;

		AUDITLOG.info(event.toString());
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
