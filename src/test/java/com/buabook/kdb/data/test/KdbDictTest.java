package com.buabook.kdb.data.test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Test;

import com.buabook.kdb.data.KdbDict;
import com.buabook.kdb.exceptions.DataOverwriteNotPermittedException;
import com.google.common.collect.ImmutableList;

/**
 * <h3>Unit Tests for {@link KdbDict}</h3>
 * (c) 2015 Sport Trades Ltd
 * 
 * @author Jas Rajasansir
 * @version 1.0.0
 * @since 29 Apr 2015
 */
public class KdbDictTest {
	
	// KdbDict.add

	@Test(expected=IllegalArgumentException.class)
	public void testAddThrowsExceptionIfNullPassedForValue() {
		new KdbDict()
				.add("key", null);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testAddThrowsExceptionIfNullPassedForKey() {
		new KdbDict()
				.add(null, "value");
	}
	
	@Test
	public void testAddAddsAtomicValue() {
		KdbDict dict = new KdbDict()
								.add("atom", 4);
		
		assertThat(dict.get("atom"), is(instanceOf(Integer.class)));
		assertThat(dict.get("atom"), is(equalTo((Object) 4)));
	}
	
	@Test
	public void testAddAddsArray() {
		int[] array = { 1, 2, 3, 4 };
		
		KdbDict dict = new KdbDict()
								.add("array", array);
		
		Object added = dict.get("array");
		
		assertThat(added, is(instanceOf(int[].class)));
		assertThat(added, is(equalTo((Object) array)));
	}
	
	@Test
	public void testAddConvertsListToArray() {
		List<Boolean> list = ImmutableList.of(true, false, true);
		boolean[] expected = { true, false, true };
		
		KdbDict dict = new KdbDict();
		dict.add("list", list);
		
		Object added = dict.get("list");
		
		assertThat(added, is(instanceOf(Object[].class)));
		assertThat(added, is(equalTo((Object) expected)));
	}
	
	@Test
	public void testAddWithNullAndTypeAddsKdbNull() {
		KdbDict dict = new KdbDict()
								.add("key", null, Long.class)
								.add("key2", null, String.class);
		
		assertThat(dict.get("key"), is(instanceOf(Long.class)));
		assertThat(dict.get("key2"), is(instanceOf(String.class)));
	}
	
	@Test(expected=DataOverwriteNotPermittedException.class)
	public void testAddThrowsExceptionIfAttemptingToAddExistingKey() {
		new KdbDict()
				.add("key", "value")
				.add("key", 1234);
	}
	
	// KdbDict.get
	
	@Test
	public void testGetReturnsRequestedValue() {
		KdbDict dict = new KdbDict()
								.add("key", "value")
								.add("key2", 124f);
		
		assertThat(dict.get("key"), is(equalTo("value")));
		assertThat(dict.get("key2"), is(equalTo(124f)));
	}
	
	@Test
	public void testGetReturnsNullForUnknownKey() {
		KdbDict dict = new KdbDict()
								.add("key", "value")
								.add("key2", 124f);
		
		assertThat(dict.get("unknown"), is(nullValue()));
	}
	
	// KdbDict.getAs
	
	@Test
	public void testGetAsReturnsValueInCorrectType() {
		KdbDict dict = new KdbDict()
								.add("key", "value")
								.add("key2", 124f);
		
		assertThat(dict.getAs("key", String.class), is(equalTo("value")));
	}
	
	@Test(expected=ClassCastException.class)
	public void testGetAsThrowsExceptionIfIncorrectTypeToCast() {
		KdbDict dict = new KdbDict()
								.add("key", "value")
								.add("key2", 124f);
		
		dict.getAs("key", Integer.class);
	}
	
	// KdbDict.getKeys
	
	@Test
	public void testGetKeysReturnsListOfKeys() {
		KdbDict dict = new KdbDict()
								.add("key", "value")
								.add("key2", 124f);
		
		assertThat(dict.getKeys(), containsInAnyOrder("key", "key2"));
	}

	// KdbDict.getKeysAsString
	
	@Test
	public void testGetKeysAsStringReturnsListOfKeys() {
		KdbDict dict = new KdbDict()
								.add("key", "value")
								.add("key2", 124f);
		
		assertThat(dict.getKeysAsString(), containsInAnyOrder("key", "key2"));
	}
	
	@Test
	public void testGetKeysAsStringReturnsListOfKeysToString() {
		KdbDict dict = new KdbDict()
								.add(123, "value")
								.add(456f, 124f);
		
		assertThat(dict.getKeys(), containsInAnyOrder(123, 456f));
		assertThat(dict.getKeysAsString(), containsInAnyOrder("123", "456.0"));
	}

}