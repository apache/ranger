package com.xasecure.audit.provider;


import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xasecure.audit.model.AuditEventBase;
import com.xasecure.audit.model.HBaseAuditEvent;
import com.xasecure.audit.model.HdfsAuditEvent;
import com.xasecure.audit.model.HiveAuditEvent;
import com.xasecure.audit.model.KnoxAuditEvent;

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

public class MultiDestAuditProvider implements AuditProvider {

	private static final Log LOG = LogFactory.getLog(MultiDestAuditProvider.class);

	protected List<AuditProvider> mProviders = new ArrayList<AuditProvider>();


	public MultiDestAuditProvider() {
		LOG.info("MultiDestAuditProvider: creating..");
	}

	public MultiDestAuditProvider(AuditProvider provider) {
		LOG.info("MultiDestAuditProvider: creating..");

		addAuditProvider(provider);
	}
	
	public void addAuditProvider(AuditProvider provider) {
		if(provider != null) {
			LOG.info("MultiDestAuditProvider.addAuditProvider(providerType=" + provider.getClass().getCanonicalName() + ")");

			mProviders.add(provider);
		}
	}

	public void addAuditProviders(List<AuditProvider> providers) {
		if(providers != null) {
			for(AuditProvider provider : providers) {
				addAuditProvider(provider);
			}
		}
	}

	@Override
	public void log(HBaseAuditEvent event) {
		LOG.debug("MultiDestAuditProvider.log(HBaseAuditEvent)");

		logEvent(event);
	}

	@Override
	public void log(HdfsAuditEvent event) {
		LOG.debug("MultiDestAuditProvider.log(HdfsAuditEvent)");

		logEvent(event);
	}

	@Override
	public void log(HiveAuditEvent event) {
		LOG.debug("MultiDestAuditProvider.log(HiveAuditEvent)");

		logEvent(event);
	}
	
	@Override
	public void log(KnoxAuditEvent event) {
		LOG.debug("MultiDestAuditProvider.log(KnoxAuditEvent)");

		logEvent(event);
	}

	@Override
	public void start() {
		for(AuditProvider provider : mProviders) {
			provider.start();
		}
	}

	@Override
	public void stop() {
		for(AuditProvider provider : mProviders) {
			provider.stop();
		}
	}

	@Override
    public void waitToComplete() {
		for(AuditProvider provider : mProviders) {
			provider.waitToComplete();
		}
	}
	
	@Override
	public boolean isFlushPending() {
		for(AuditProvider provider : mProviders) {
			if(provider.isFlushPending()) {
				return true;
			}
		}
		
		return false;
	}
	
	@Override
	public long getLastFlushTime() {
		long lastFlushTime = 0;
		for(AuditProvider provider : mProviders) {
			long flushTime = provider.getLastFlushTime();
			
			if(flushTime != 0) {
				if(lastFlushTime == 0 || lastFlushTime > flushTime) {
					lastFlushTime = flushTime;
				}
			}
		}
		
		return lastFlushTime;
	}
	
	@Override
	public void flush() {
		try {
			for(AuditProvider provider : mProviders) {
				provider.flush();
			}
		} catch(Throwable excp) {
			LOG.error("AsyncAuditProvider.flush(): failed to flush events", excp);
		}
	}
	
	protected void logEvent(AuditEventBase event) {
		try {
            for(AuditProvider provider : mProviders) {
                event.logEvent(provider);
            }
		} catch(Throwable excp) {
			LOG.error("failed to log event { " + event.toString() + " }", excp);
		}
	}
}
