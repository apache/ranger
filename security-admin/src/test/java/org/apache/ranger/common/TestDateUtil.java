/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.common;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

@SuppressWarnings("deprecation")
public class TestDateUtil {

	@Autowired
	DateUtil dateUtil = new DateUtil();

	public void testGetDateFromNow() {
		int days = 1;		
		Date dateCheck= dateUtil.getDateFromNow(days);
		int minutes=dateCheck.getMinutes();
		int hourse=dateCheck.getHours();
		Assert.assertEquals(dateCheck.getDay(),days+2);
		Assert.assertEquals(dateCheck.getMinutes(), minutes);
		Assert.assertEquals(dateCheck.getHours(), hourse);
	}

	@Test
	public void testDateToString() {
		Date date = new Date();
		SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd-MM-yyyy");
		String dateFromat = DATE_FORMAT.format(date);
		String dateCheck = DateUtil.dateToString(date, dateFromat);
		Assert.assertEquals(dateCheck,dateFromat);
	}
	
	@Test
	public void testGetDateFromGivenDate(){
		Date date = new Date();
		int days=0;
		int hours=date.getHours();
		int minutes=date.getMinutes();
		int second=date.getSeconds();
		Date currentDate = dateUtil.getDateFromGivenDate(date, days, 0, 0, 0);
		Assert.assertEquals(currentDate.getDay(),date.getDay()+days);
		Assert.assertEquals(currentDate.getHours(),hours);
		Assert.assertEquals(currentDate.getMinutes(),minutes);
		Assert.assertEquals(currentDate.getSeconds(),second);
	}
	
	@Test
	public void testAddTimeOffset(){
		Date date = new Date();
		int mins=date.getMinutes();
		Date currentDate=dateUtil.addTimeOffset(date, 0);
		Assert.assertEquals(currentDate.getDate(),date.getDate());
		Assert.assertEquals(currentDate.getMinutes(),mins);
	}

}