/**
 *
 */
package com.xasecure.common;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import org.springframework.stereotype.Component;


@Component
public class DateUtil {    

    public Date getDateFromNow(int days) {
    	return getDateFromNow(days, 0, 0);
    }   

    public Date getDateFromNow(int days, int hours, int minutes) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, days);
		cal.add(Calendar.HOUR, hours);
		cal.add(Calendar.MINUTE, minutes);
		return cal.getTime();
    }
    
	public static String dateToString(Date date, String dateFromat) {
		SimpleDateFormat formatter = new SimpleDateFormat(dateFromat);
		return formatter.format(date).toString();
	}

	public Date getDateFromGivenDate(Date date, int days, int hours,int minutes, int second) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, days);
		cal.add(Calendar.HOUR, hours);
		cal.add(Calendar.MINUTE, minutes);
		cal.add(Calendar.SECOND, second);
		return cal.getTime();
	}
	/**
	 * useful for converting client time zone Date to UTC Date 
	 * @param date
	 * @param mins
	 * @return
	 */
	public Date addTimeOffset(Date date, int mins) {
		if (date == null) {
			return date;
		}
		long t = date.getTime();
		Date newDate = new Date(t + (mins * 60000));
		return newDate;
	}
	
	
	public static Date getUTCDate(){
		try{
			Calendar local=Calendar.getInstance();
		    int offset = local.getTimeZone().getOffset(local.getTimeInMillis());
		    GregorianCalendar utc = new GregorianCalendar(TimeZone.getTimeZone("GMT+0"));
		    utc.setTimeInMillis(local.getTimeInMillis());
		    utc.add(Calendar.MILLISECOND, -offset);
		    return utc.getTime();
		}catch(Exception ex){
			return null;
		}
	}

	public static Date getUTCDate(long epoh) {
		if(epoh==0){
			return null;
		}
		try{
			Calendar local=Calendar.getInstance();
		    int offset = local.getTimeZone().getOffset(epoh);
		    GregorianCalendar utc = new GregorianCalendar(TimeZone.getTimeZone("GMT+0"));
		    utc.setTimeInMillis(epoh);
		    utc.add(Calendar.MILLISECOND, -offset);	   
		    return utc.getTime();
	    }catch(Exception ex){
	    	return null;
	    }	    		
	}

}
