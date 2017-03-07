package com.buabook.kdb.test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

import com.buabook.kdb.Dicts;
import com.buabook.kdb.data.KdbDict;
import com.kx.c.Dict;

public class DictsTest {

	// Dicts.isNullOrEmpty(KdbDict)

	@Test
	public void testIsNullOrEmptyKdbDictReturnsTrueForNullDict() {
		assertThat(Dicts.isNullOrEmpty((KdbDict) null), is(equalTo(true)));
	}
	
	@Test
	public void testIsNullOrEmptyKdbDictReturnsTrueForEmptyDict() {
		KdbDict dict = new KdbDict();
		
		assertThat(Dicts.isNullOrEmpty(dict), is(equalTo(true)));
	}
	
	@Test
	public void testIsNullOrEmptyKdbDictReturnsFalseForNonEmptyDict() {
		KdbDict dict = new KdbDict()
								.add("key1", "string")
								.add("key2", 1234f);
		
		assertThat(Dicts.isNullOrEmpty(dict), is(equalTo(false)));
	}
	
	// Dicts.isNullOrEmpty(Dict)
	
	@Test
	public void testIsNullOrEmptyDictReturnsTrueForNullDict() {
		assertThat(Dicts.isNullOrEmpty((Dict) null), is(equalTo(true)));
	}
	
	@Test
	public void testIsNullOrEmptyDictReturnsTrueForEmptyDict() {
		Dict dict = new Dict(null, null);
		
		assertThat(Dicts.isNullOrEmpty(dict), is(equalTo(true)));
	}
	
	@Test
	public void testIsNullOrEmptyDictReturnsFalseForNonEmptyDict() {
		Object[] keys = { "key1", "key2", "key3" };
		Object[] vals = { 1, 3, 5 };
		
		Dict dict = new Dict(keys, vals);
		
		assertThat(Dicts.isNullOrEmpty(dict), is(equalTo(false)));
	}
	
	// Dicts.getSize
	
	@Test
	public void testGetSizeReturnsZeroForNullDict() {
		assertThat(Dicts.getSize(null), is(equalTo(0)));
	}
	
	@Test
	public void testGetSizeReturnsZeroForEmptyDict() {
		assertThat(Dicts.getSize(new Dict(null, null)), is(equalTo(0)));
	}
	
	@Test
	public void testGetSizeReturnsSizeOfNonEmptyDict() {
		Object[] keys = { "key1", "key2", "key3" };
		Object[] vals = { 1, 3, 5 };
		
		Dict dict = new Dict(keys, vals);
		
		assertThat(Dicts.getSize(dict), is(equalTo(3)));
	}
}
