package com.xasecure.common;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import com.xasecure.common.DateUtil;

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
	
	@Test
	public void testGetUTCDate1(){
		Date date=new Date();
		Date userdate=DateUtil.getUTCDate();
		Assert.assertEquals(userdate.getDate(),date.getDate());
	}
}