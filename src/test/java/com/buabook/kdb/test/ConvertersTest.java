package com.buabook.kdb.test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import org.junit.Test;

import com.buabook.kdb.Converters;

public class ConvertersTest {

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

}
