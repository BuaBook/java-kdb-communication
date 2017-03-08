package com.buabook.kdb.data.test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Test;

import com.buabook.kdb.Dicts;
import com.buabook.kdb.data.KdbDict;
import com.buabook.kdb.data.PrintableDict;
import com.buabook.kdb.exceptions.DataOverwriteNotPermittedException;
import com.buabook.kdb.exceptions.DictUnionNotPermittedException;
import com.google.common.collect.ImmutableList;
import com.kx.c.Dict;

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
	
	// KdbDict.getDataStore
	
	@Test
	public void testGetDataStoreReturnsUnderlyingDataStore() {
		KdbDict dict = new KdbDict()
								.add(123, "value")
								.add("key2", "a3343r");
		
		assertThat(dict.getDataStore(), hasKey(123));
		assertThat(dict.getDataStore(), hasKey("key2"));
	}
	
	@Test
	public void testGetDataStoreReturnsEmptyMapWhenNoDataInDict() {
		assertThat(new KdbDict().getDataStore(), is(anEmptyMap()));
	}
	
	// KdbDict.getDataStoreWithStringKeys
	
	@Test
	public void testGetDataStoreWithStringKeysReturnsUnderlyingDataStoreWithStringKeys() {
		KdbDict dict = new KdbDict()
				.add(123, "value")
				.add("key2", "a3343r");

		assertThat(dict.getDataStoreWithStringKeys(), hasKey("123"));
		assertThat(dict.getDataStoreWithStringKeys(), hasKey("key2"));
	}
	
	@Test
	public void testGetDataStoreWithStringKeysReturnsEmptyMapWhenNoDataInDict() {
		assertThat(new KdbDict().getDataStoreWithStringKeys(), is(anEmptyMap()));
	}
	
	// KdbDict.union
	
	@Test(expected=DictUnionNotPermittedException.class)
	public void testUnionThrowsExceptionIfBothDictsContainTheSameKey() {
		KdbDict dict = new KdbDict()
								.add("key1", "some-value");
		
		dict.union(dict);
	}
	
	@Test
	public void testUnionReturnsDictUnmodifiedIfToUnionIsNull() {
		KdbDict source = new KdbDict()
									.add("key1", "some-value")
									.add("key2", 12345);
		
		assertThat(source.union(null), is(equalTo(source)));
	}
	
	@Test
	public void testUnionReturnsDictWithBothKeys() {
		KdbDict dict1 = new KdbDict()
									.add("key1", "some-value");
		KdbDict dict2 = new KdbDict()
									.add("key2", "another-value");
		
		KdbDict result = dict1.union(dict2);
		
		assertThat(result.getKeys(), containsInAnyOrder("key1", "key2"));
	}
	
	// KdbDict.convertToDict
	
	@Test
	public void testConvertToDictGivesEmptyDictIfEmpty() {
		Dict emptyDict = new KdbDict().convertToDict();
		assertThat(Dicts.getSize(emptyDict), is(equalTo(0)));
	}
	
	@Test
	public void testConvertToDictConvertsToDict() {
		Dict dict = new KdbDict()
								.add("key1", "value")
								.add("key2", -123)
								.convertToDict();
		
		assertThat(dict.x, is(instanceOf(Object[].class)));
		assertThat((((Object[]) dict.x)[0]), is(equalTo("key1")));
		assertThat((((Object[]) dict.x)[1]), is(equalTo("key2")));
	}
	
	// KdbDict.has
	
	@Test
	public void testHasReturnsFalseForAnyKeyWhenEmpty() {
		KdbDict dict = new KdbDict();
		
		assertThat(dict.has("key1"), is(equalTo(false)));
		assertThat(dict.has(123423), is(equalTo(false)));
	}
	
	@Test
	public void testHasReturnsResultForKeys() {
		KdbDict dict = new KdbDict()
								.add("key1", "12345")
								.add("key2", 9876543);
		
		assertThat(dict.has("key1"), is(equalTo(true)));
		assertThat(dict.has("key3"), is(equalTo(false)));
	}
	
	// KdbDict.toString
	
	@Test
	public void testToStringReturnsMapStringRepresentation() {
		KdbDict dict = new KdbDict()
								.add("key1", "12345")
								.add("key2", 9876543);
		
		assertThat(dict.toString(), is(not(emptyString())));
		assertThat(dict.toString(), is(equalTo(dict.getDataStore().toString())));
	}
	
	// KdbDict.fromObject
	
	@Test
	public void testFromObjectReturnsNullIfNoObject() {
		assertThat(KdbDict.fromObject(null), is(nullValue()));
	}
	
	@Test
	public void testFromObjectReturnsKdbDictFromDict() {
		Object[] keys = { 1, 2, 3 };
		Object[] vals = { 4, 5, 6 };
		
		PrintableDict dict = new PrintableDict(keys, vals);
		
		KdbDict converted = KdbDict.fromObject(dict);
		
		assertThat(converted, is(not(nullValue())));
		assertThat(converted.has(1), is(equalTo(true)));
		assertThat(converted.has(2), is(equalTo(true)));
	}
}