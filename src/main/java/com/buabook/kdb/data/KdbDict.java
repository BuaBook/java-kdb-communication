package com.buabook.kdb.data;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.buabook.kdb.Converters;
import com.buabook.kdb.Types;
import com.buabook.kdb.query.KdbQuery;
import com.buabook.kdb.exceptions.DataOverwriteNotPermittedException;
import com.buabook.kdb.exceptions.DictUnionNotPermittedException;
import com.google.common.collect.Lists;
import com.kx.c.Dict;

/**
 * <h3>KDB Dictionary Container</h3>
 * <p>Provides the ability to represent (and generate) kdb dictionaries in a more Java
 * friendly way. Objects are <b>not</b> thread-safe.</p>
 * (c) 2014 - 2017 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.1.0
 * @since 8 Jun 2014
 */
public class KdbDict {
	private static final Logger log = LoggerFactory.getLogger(KdbDict.class);
	
	/** The dictionary as stored within Java */
	private final Map<Object, Object> data;

	
	public KdbDict() {
		this.data = new HashMap<>();
	}
	
	/**
	 * @param kdbDict The dictionary as received from kdb to generate this object from
	 */
	public KdbDict(Dict kdbDict) {
		this();
		setInitialDataSet(kdbDict);
	}
	
	
	/**
	 * Takes the provided kdb dictionary and maps it into {@link #data} for use in this object
	 * @param kdbDict The dictionary as received from kdb to generates this object from
	 * @throws DataOverwriteNotPermittedException If the dictionary already has data in it
	 * @throws UnsupportedOperationException If the dictionary is does not contain object arrays, or if the <code>x</code> 
	 * and <code>y</code> array lengths do not line up
	 */
	public void setInitialDataSet(Dict kdbDict) throws DataOverwriteNotPermittedException, UnsupportedOperationException {
		if(! isEmpty())
			throw new DataOverwriteNotPermittedException();
		
		if(! (kdbDict.x instanceof Object[]))
			if(! Types.getArrayPrimitiveTypes().contains(kdbDict.x.getClass()))
				throw new UnsupportedOperationException("Dictionary keys must be either an Object or primitive array");
		
		if(! (kdbDict.y instanceof Object[]))
			if(! Types.getArrayPrimitiveTypes().contains(kdbDict.y.getClass()))
				throw new UnsupportedOperationException("Dictionary values must be either an Object or primitive array");
		
		Object[] keys = Converters.arrayToObjectArray(kdbDict.x);
		Object[] values = Converters.arrayToObjectArray(kdbDict.y);
		
		if(keys.length != values.length)
			throw new UnsupportedOperationException("Dictionary keys / values lengths mismatch");
		
		for(int i = 0; i < keys.length; i++)
			data.put(keys[i], values[i]);
		
		log.trace("Received kdb dictionary: {}", data);
	}
	
	/**
	 * <p>Adds a new key / value pair to the dictionary</p>
	 * <p>If the value object is a {@link List}, it will be automatically converted to an array for proper serialisation to kdb.</p>
	 * <p><b>NOTE</b>: Java's <code>null</code> cannot be provided as either the key or value parameter. If there is a possibility that the value
	 * to be added <i>might</i> be <code>null</code>, you should use {@link #add(Object, Object, Class)}.</p>
	 * @throws IllegalArgumentException If either the key or value to be added to the dictionary are <code>null</code>
	 * @throws DataOverwriteNotPermittedException If there is already a key in the dictionary that matches the specified key
	 */
	public KdbDict add(Object key, Object value) throws IllegalArgumentException, DataOverwriteNotPermittedException {
		if(key == null)
			throw new IllegalArgumentException("key cannot be null");
		
		if(data.containsKey(key))
			throw new DataOverwriteNotPermittedException("Key '" + key + "' already exists");
		
		if(value == null)
			throw new IllegalArgumentException("Java 'null' cannot be added to a kdb dictionary");
		
		if(value instanceof List<?>)
			value = ((List<?>) value).toArray();
		
		data.put(key, value);
		return this;
	}
	
	/**
	 * <p>Adds a new key / value pair to the dictionary with <code>null</code> detection</p>
	 * <p>If the specified value is <code>null</code>, the appropriate kdb value is selected to represent null in the specified type.</p>
	 * @param elementType The expected type of the value. Used to derive the correct kdb value to represent null.
	 * @see #add(Object, Object)
	 * @see Types#getKdbNullFor(Class)
	 */
	public KdbDict add(Object key, Object value, Class<?> elementType) throws DataOverwriteNotPermittedException {
		if(value == null)
			value = Types.getKdbNullFor(elementType);
		
		return add(key, value);
	}
	
	/** @see HashMap#get(Object) */
	public Object get(Object key) {
		return data.get(key);
	}
	
	/**
	 * Gets the specified key and also casts it to the specified type
	 * @param key
	 * @param returnType The type that the object should be cast to before returning
	 * @return The object in its cast form
	 * @throws ClassCastException If the cast fails for any reason
	 */
	public <T> T getAs(Object key, Class<T> returnType) throws ClassCastException {
		return returnType.cast(get(key));
	}
	
	/** @return All the keys in the dictionary */
	public List<Object> getKeys() {
		return Lists.newArrayList(data.keySet());
	}
	
	/** @return All the keys in the dictionary, in String format (useful for tables) */
	public List<String> getKeysAsString() {
		return data.keySet().stream()
								.map(Objects::toString)
								.collect(Collectors.toList());
	}
	
	public Map<Object, Object> getDataStore() {
		return data;
	}
	
	public Map<String, Object> getDataStoreWithStringKeys() {
		return data.entrySet().stream()
									.collect(Collectors.toMap(	e -> Objects.toString(e.getKey()), 
																Entry::getValue));
	}
	
	/**
	 * Allows the joining of 2 dictionaries of distinct elements together
	 * @param that The other dictionary to join together
	 * @throws DictUnionNotPermittedException If there is one or more matching elements in both dictionaries
	 */
	public KdbDict union(KdbDict that) throws DictUnionNotPermittedException {
		if(that == null)
			return this;
		
		if(! Collections.disjoint(that.getKeys(), this.getKeys()))
			throw new DictUnionNotPermittedException("The 2 rows contain one or more of the same element. Cannot union together.");
		
		data.putAll(that.data);
		
		return this;
	}
	
	/**
	 * Generates a {@link Dict} for use before sending the object down the wire to a kdb process
	 * @return The object ready for sending to the kdb object
	 * @see PrintableDict
	 */
	public Dict convertToDict() {
		Object keys = data.keySet().toArray();
		Object values = data.values().toArray();
		
		return new PrintableDict(keys, values);
	}
	
	public Boolean isEmpty() {
		return data.isEmpty();
	}
	
	/** 
	 * @return <code>true</code> if the dictionary contains the specified key, <code>false</code> otherwise
	 * @see HashMap#containsKey(Object)
	 */
	public Boolean has(Object key) {
		return data.containsKey(key);
	}
	
	@Override
	public String toString() {
		return data.toString();
	}
	
	/**
	 * <p>Generates a new {@link KdbDict} object from an object. This method will only succeed if the object provided can be cast into a {@link Dict}.</p>
	 * <p>Use this method along with {@link KdbQuery} to convert a query result into a dictionary when one is expected.</p> 
	 * @param object The object to generate a new {@link KdbDict} from
	 * @return A new dictionary object or <code>null</code> if the provided object is null 
	 * @throws ClassCastException If the specified object cannot be cast into a {@link Dict}
	 */
	public static KdbDict fromObject(Object object) throws ClassCastException {
		if(object == null)
			return null;
		
		return new KdbDict((Dict) object);
	}

}
