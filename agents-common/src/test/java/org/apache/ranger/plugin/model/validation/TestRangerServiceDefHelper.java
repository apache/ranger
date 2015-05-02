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

package org.apache.ranger.plugin.model.validation;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Set;

import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper.Delegate;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestRangerServiceDefHelper {

	@Test
	public void test_getResourceHierarchies() {
		/*
		 * Create a service-def with following resource graph
		 * 
		 *   Database -> UDF
		 *       |
		 *       v
		 *      Table -> Column
		 *         |
		 *         v
		 *        Table-Attribute 
		 *  
		 *  It contains following hierarchies
		 *  - [ Database UDF]
		 *  - [ Database Table Column ]
		 *  - [ Database Table Table-Attribute ] 
		 */
		RangerResourceDef Database = createResourceDef("Database", "");
		RangerResourceDef UDF = createResourceDef("UDF", "Database");
		RangerResourceDef Table = createResourceDef("Table", "Database");
		RangerResourceDef Column = createResourceDef("Column", "Table");
		RangerResourceDef Table_Atrribute = createResourceDef("Table-Attribute", "Table");
		// order of resources in list sould not matter
		List<RangerResourceDef> resourceDefs = Lists.newArrayList(Column, Database, Table, Table_Atrribute, UDF);
		// stuff this into a service-def
		RangerServiceDef serviceDef = mock(RangerServiceDef.class);
		when(serviceDef.getResources()).thenReturn(resourceDefs);
		when(serviceDef.getName()).thenReturn("a-service-def");
		// now assert the behavior
		RangerServiceDefHelper serviceDefHelper = new RangerServiceDefHelper(serviceDef);
		Set<List<RangerResourceDef>> hierarchies = serviceDefHelper.getResourceHierarchies();
		// there should be 
		List<RangerResourceDef> hierarchy = Lists.newArrayList(Database, UDF);
		assertTrue(hierarchies.contains(hierarchy));
		hierarchy = Lists.newArrayList(Database, Table, Column);
		assertTrue(hierarchies.contains(hierarchy));
		hierarchy = Lists.newArrayList(Database, Table, Table_Atrribute);
		assertTrue(hierarchies.contains(hierarchy));
	}
	
	@Test
	public final void test_cacheBehavior() {
		// wipe the cache clean
		RangerServiceDefHelper._Cache.clear();
		// let's add one entry to the cache
		Delegate delegate = mock(Delegate.class);
		Date aDate = getNow();
		String serviceName = "a-service-def";
		when(delegate.getServiceFreshnessDate()).thenReturn(aDate);
		when(delegate.getServiceName()).thenReturn(serviceName);
		RangerServiceDefHelper._Cache.put(serviceName, delegate);
		
		// create a service def with matching date value
		RangerServiceDef serviceDef = mock(RangerServiceDef.class);
		when(serviceDef.getName()).thenReturn(serviceName);
		when(serviceDef.getUpdateTime()).thenReturn(aDate);
		
		// since cache has it, we should get back the one that we have added
		RangerServiceDefHelper serviceDefHelper = new RangerServiceDefHelper(serviceDef);
		assertTrue("Didn't get back the same object that was put in cache", delegate == serviceDefHelper._delegate);
		
		// if we change the date then that should force helper to create a new delegate instance
		/*
		 * NOTE:: current logic would replace the cache instance even if the one in the cache is newer.  This is not likely to happen but it is important to call this out
		 * as in rare cases one may end up creating re creating delegate if threads have stale copies of service def.
		 */
		when(serviceDef.getUpdateTime()).thenReturn(getLastMonth());
		serviceDefHelper = new RangerServiceDefHelper(serviceDef);
		assertTrue("Didn't get a delegate different than what was put in the cache", delegate != serviceDefHelper._delegate);
		// now that a new instance was added to the cache let's ensure that it got added to the cache 
		Delegate newDelegate = serviceDefHelper._delegate;
		serviceDefHelper = new RangerServiceDefHelper(serviceDef);
		assertTrue("Didn't get a delegate different than what was put in the cache", newDelegate == serviceDefHelper._delegate);
	}
	
	RangerResourceDef createResourceDef(String name, String parent) {
		RangerResourceDef resourceDef = mock(RangerResourceDef.class);
		when(resourceDef.getName()).thenReturn(name);
		when(resourceDef.getParent()).thenReturn(parent);
		return resourceDef;
	}

	Date getLastMonth() {
		Calendar cal = GregorianCalendar.getInstance();
		cal.add( Calendar.MONTH, 1);
		Date lastMonth = cal.getTime();
		return lastMonth;
	}
	
	Date getNow() {
		Calendar cal = GregorianCalendar.getInstance();
		Date now = cal.getTime();
		return now;
	}
}
