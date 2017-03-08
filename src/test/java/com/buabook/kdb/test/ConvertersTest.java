package com.buabook.kdb.test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.sql.Date;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.Duration;

import org.joda.time.DateTime;
import org.junit.Test;

import com.buabook.kdb.Converters;
import com.kx.c.Timespan;

public class ConvertersTest {
	
	// Converters.durationToTimespan
	
	@Test
	public void testDurationToTimespanReturns0TimespanIfNullDuration() {
		Timespan timespan = Converters.durationToTimespan(null);
		
		assertThat(timespan, is(not(nullValue())));
		assertThat(timespan.j, is(equalTo(0l)));
	}
	
	@Test
	public void testDurationToTimespanReturnsDurationMillisInNanoseconds() throws DatatypeConfigurationException {
		Duration duration = DatatypeFactory.newInstance().newDuration(12345);
		Timespan timespan = Converters.durationToTimespan(duration);
		
		assertThat(timespan, is(not(nullValue())));
		assertThat(timespan.j / 1000000, is(equalTo(12345l)));
	}
	
	// Converters.longToSqlDate
	
	@Test
	public void testLongToSqlDateReturnsSqlDateForLong() {
		long now = DateTime.now().getMillis();
		
		Date date = Converters.longToSqlDate(now);
		
		assertThat(date, is(not(nullValue())));
		assertThat(date.getTime(), is(equalTo(now)));
	}

	// Converters.arrayToObjectArray
	
	@Test
	public void testConvertArrayToObjectArrayReturnsObjectArrayForNonPrimitiveType() {
		String[] strArray = { "abc", "def", "ghi" };
		Object[] result = Converters.arrayToObjectArray(strArray);
		
		assertThat(result, is(not(nullValue())));
		assertThat(result.length, is(equalTo(3)));
	}
	
	@Test
	public void testConvertArrayToObjectArrayReturnsObjectArrayForPrimitiveType() {
		boolean[] boolArray = { false, true, true };
		Object[] result = Converters.arrayToObjectArray(boolArray);
		
		assertThat(result, is(not(nullValue())));
		assertThat(result.length, is(equalTo(3)));
	}

	// Constructor
	
	@Test
	public void testConstructorConstructsWithoutError() {
		new Converters();
	}
}
