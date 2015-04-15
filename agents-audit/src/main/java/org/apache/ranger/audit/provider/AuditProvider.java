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

import java.util.Collection;
import java.util.Properties;

import org.apache.ranger.audit.model.AuditEventBase;

public interface AuditProvider {
	public boolean log(AuditEventBase event);
	public boolean log(Collection<AuditEventBase> events);	

	public boolean logJSON(String event);
	public boolean logJSON(Collection<String> events);	

    public void init(Properties prop);
    public void init(Properties prop, String basePropertyName);
    public void start();
    public void stop();
    public void waitToComplete();
    public void waitToComplete(long timeout);

    /**
     * Name for this provider. Used only during logging. Uniqueness is not guaranteed
     */
    public String getName();

    /**
     * If this AuditProvider in the state of shutdown
     * @return
     */
    public boolean isDrain();
    
    public int getMaxBatchSize();
    public int getMaxBatchInterval();
	public boolean isFlushPending();
	public long    getLastFlushTime();
    public void    flush();
}
