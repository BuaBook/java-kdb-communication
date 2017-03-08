package com.buabook.kdb.data.test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

import com.buabook.kdb.data.PrintableDict;
import com.kx.c.Dict;

public class PrintableDictTest {
	
	// PrintableDict(Dict)
	
	@Test
	public void testConstructorWithDictConvertsSuccessfully() {
		Object[] keys = { 1, 2, 3 };
		Object[] vals = { 4, 5, 6 };
		
		Dict dict = new Dict(keys, vals);
		PrintableDict pDict = new PrintableDict(dict);
		
		assertThat(pDict.getSize(), is(equalTo(3)));
	}
	
	// PrintableDict.toString

	@Test
	public void testDictToStringPrintsSimpleTypes() {
		Object[] keys = { 1, 2, 3 };
		Object[] vals = { 4, 5, 6 };
		
		PrintableDict dict = new PrintableDict(keys, vals);
		assertThat(dict.toString(), is(equalTo("{ 1 = 4; 2 = 5; 3 = 6 }")));
	}
	
	@Test
	public void testDictToStringPrintsStrings() {
		Object[] keys = { 1, 2, 3 };
		Object[] vals = { "abc", "bcd", "def" };
		
		PrintableDict dict = new PrintableDict(keys, vals);
		assertThat(dict.toString(), is(equalTo("{ 1 = abc; 2 = bcd; 3 = def }")));
	}
	
	@Test
	public void testDictToStringPrintsValueArrays() {
		Object[] keys = { 1, 2, 3 };
		
		Object[] col1 = { 1.0, 1.1, 1.2 };
		Object[] col2 = { 7, 8, 9 };
		Object[] col3 = { "x", "y", "z" };
		
		Object[] cols = { col1, col2, col3 };
		
		PrintableDict dict = new PrintableDict(keys, cols);
		assertThat(dict.toString(), is(equalTo("{ 1 = 1.0, 1.1, 1.2; 2 = 7, 8, 9; 3 = x, y, z }")));
	}
	
	// PrintableDict.getSize
	
	@Test
	public void testGetSizeReturnsZeroForEmptyDict() {
		assertThat(new PrintableDict(null, null).getSize(), is(equalTo(0)));
	}
	
	@Test
	public void testGetSizeReturnsCorrectNumberOfElementsForDict() {
		Object[] keys = { 1, 2, 3 };
		Object[] vals = { "abc", "bcd", "def" };
		
		PrintableDict dict = new PrintableDict(keys, vals);
		
		assertThat(dict.getSize(), is(equalTo(3)));
	}
	
}
