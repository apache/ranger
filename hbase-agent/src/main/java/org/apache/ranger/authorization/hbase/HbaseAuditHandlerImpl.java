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
package org.apache.ranger.authorization.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;

public class HbaseAuditHandlerImpl extends RangerDefaultAuditHandler implements HbaseAuditHandler {

	static final List<AuthzAuditEvent> _EmptyList = new ArrayList<AuthzAuditEvent>();
	final List<AuthzAuditEvent> _allEvents = new ArrayList<AuthzAuditEvent>();
	// we replace its contents anytime new audit events are generated.
	AuthzAuditEvent _mostRecentEvent = null;
	boolean _superUserOverride = false;
	
	@Override
	public AuthzAuditEvent getAuthzEvents(RangerAccessResult result) {
		
		AuthzAuditEvent event = super.getAuthzEvents(result);
		// first accumulate last set of events and then capture these as the most recent ones
		if (_mostRecentEvent != null) {
			_allEvents.add(_mostRecentEvent);
		}
		_mostRecentEvent = event;
		// We return null because we don't want default audit handler to audit anything!
		return null;
	}
	
	@Override
	public List<AuthzAuditEvent> getCapturedEvents() {
		// construct a new collection since we don't want to lose track of which were the most recent events;
		List<AuthzAuditEvent> result = new ArrayList<AuthzAuditEvent>(_allEvents);
		if (_mostRecentEvent != null) {
			result.add(_mostRecentEvent);
		}
		applySuperUserOverride(result);
		return result;
	}

	@Override
	public AuthzAuditEvent  discardMostRecentEvent() {
		AuthzAuditEvent result = _mostRecentEvent;
		applySuperUserOverride(result);
		_mostRecentEvent = null;
		return result;
	}

	@Override
	public void setMostRecentEvent(AuthzAuditEvent event) {
		_mostRecentEvent = event;
	}

	@Override
	public void setSuperUserOverride(boolean override) {
		_superUserOverride = override;
	}
	
	void applySuperUserOverride(List<AuthzAuditEvent> events) {
		for (AuthzAuditEvent event : events) {
			applySuperUserOverride(event);
		}
	}
	
	void applySuperUserOverride(AuthzAuditEvent event) {
		if (event != null && _superUserOverride) {
			event.setAccessResult((short)1);
		}
	}
}
