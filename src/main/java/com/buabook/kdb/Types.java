package com.buabook.kdb;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.kx.c;
import com.kx.c.Minute;
import com.kx.c.Month;
import com.kx.c.Second;
import com.kx.c.Timespan;

/**
 * <h3>Java &lt;-&gt; kdb Type Conversions</h3>
 * (c) 2017 Sport Trades Ltd
 * 
 * @author Jas Rajasansir
 * @version 1.0.0
 * @since 21 Feb 2017
 */
public final class Types {

	/** All class definitions of primitive array types, as a {@link List} (e.g. <code>int[].class</code>) */
	private static final List<Class<?>> ARRAY_PRIMITIVE_TYPES_LIST = ImmutableList.<Class<?>>builder()
																								.add(int[].class)
																								.add(float[].class)
																								.add(double[].class)
																								.add(boolean[].class)
																								.add(byte[].class)
																								.add(short[].class)
																								.add(long[].class)
																								.add(char[].class)
																								.build();
	
	/** Java class to kdb object <code>null</code> mapping */
	private static final Map<Class<?>, Object> KDB_NULL_TYPES = ImmutableMap.<Class<?>, Object>builder()
																								.put(Boolean.class,			c.NULL('b'))
																								.put(UUID.class,			c.NULL('g'))
																								.put(Byte.class,			c.NULL('x'))
																								.put(Short.class,			c.NULL('h'))
																								.put(Integer.class,			c.NULL('i'))
																								.put(Long.class,			c.NULL('j'))
																								.put(Float.class,			c.NULL('e'))
																								.put(Double.class,			c.NULL('f'))
																								.put(Character.class,		c.NULL('c'))
																								.put(String.class,			c.NULL('s'))
																								.put(Timestamp.class,		c.NULL('p'))
																								.put(Month.class,			c.NULL('m'))
																								.put(java.sql.Date.class,	c.NULL('d'))
																								.put(java.util.Date.class,	c.NULL('z'))
																								.put(Timespan.class,		c.NULL('n'))
																								.put(Minute.class,			c.NULL('u'))
																								.put(Second.class,			c.NULL('v'))
																								.put(Time.class,			c.NULL('t'))
																								.put(char[].class,			new char[0])		// kdb String's are character arrays
																								.build();
	
	
	
	/** @return The primitive array types as a list */
	public static List<Class<?>> getArrayPrimitiveTypes() {
		return ARRAY_PRIMITIVE_TYPES_LIST;
	}
	
	/** 
	 * The source of the kdb null is instantiated statically, so this function can be called as many
	 * times as necessary without any object creation cost.
	 * @return kdb null object for the specified Java type
	 */
	public static Object getKdbNullFor(Class<?> javaType) {
		return KDB_NULL_TYPES.get(javaType);
	}
}
