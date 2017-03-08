package com.buabook.kdb;

import java.lang.reflect.Array;
import java.sql.Date;

import javax.xml.datatype.Duration;

import org.joda.time.DateTime;

import com.kx.c.Timespan;

/**
 * <h3>Conversion Methods for Java &lt;&dash;&gt; kdb Communication</h3>
 * (c) 2017 Sport Trades Ltd
 * 
 * @author Jas Rajasansir
 * @version 1.0.0
 * @since 12 Aug 2016
 */
public final class Converters {
	
	private static final int NANO_SECONDS_IN_1_MS = 1000000;
	
	
	/** @return kdb {@link Timespan} equivalent to XML duration */
	public static Timespan durationToTimespan(Duration xmlDuration) {
		if(xmlDuration == null)
			return new Timespan(0l);
		
		return new Timespan(NANO_SECONDS_IN_1_MS * xmlDuration.getTimeInMillis(DateTime.now().toDate()));
	}
	
	/** Convert a long date (milliseconds after 1970) to a {@link Date} object. This represents a date-only object in kdb */
	public static Date longToSqlDate(long dateTime){
		return new Date(dateTime);
	}
	
	/**
	 * Converts the supplied array into an object array. This method can also deal with primitive
	 * arrays and convert them to object arrays appropriately. If the array contains non-primitive
	 * objects, the array is simply cast to <code>Object[]</code>.
	 * @param array The array to convert
	 * @return The converted array
	 */
	public static Object[] arrayToObjectArray(Object array) {
		if(! Types.getArrayPrimitiveTypes().contains(array.getClass()))
			return (Object[]) array;
		
		Object[] outputArray = null;
		int arrlength = Array.getLength(array);
		outputArray = new Object[arrlength];
        
        for(int i = 0; i < arrlength; ++i)
        	outputArray[i] = Array.get(array, i);
        
        return outputArray;
	}
}
