package com.buabook.kdb;

import com.buabook.kdb.data.KdbDict;
import com.kx.c.Dict;

/**
 * <h3>{@link Dict} and {@link KdbDict} Helpers</h3>
 * (c) 2017 Sport Trades Ltd
 * 
 * @author Jas Rajasansir
 * @version 1.0.0
 * @since 21 Feb 2017
 */
public final class Dicts {

	/** @return <code>true</code> if the specified dictionary is <code>null</code> or has no data in it; <code>false</code> otherwise */
	public static boolean isNullOrEmpty(KdbDict dict) {
		return dict == null || dict.isEmpty();
	}
	
	/** @return <code>true</code> if the specified dictionary is <code>null</code> or has no data in it; <code>false</code> otherwise */
	public static boolean isNullOrEmpty(Dict dict) {
		return getSize(dict) == 0;
	}

	/** @return The number of elements within the specified dictionary */
	public static int getSize(Dict dict) {
		if(dict == null)
			return 0;
		
		Object[] keys = (Object[]) dict.x;
		
		if(keys == null)
			return 0;
		
		return keys.length;
	}
	
}
