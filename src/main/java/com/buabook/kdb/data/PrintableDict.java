package com.buabook.kdb.data;

import com.buabook.common.Printers;
import com.kx.c.Dict;

/**
 * <h3>{@link Dict} with {@link Object#toString()} Support</h3>
 * <p>This class is identical to the standard kx {@link Dict} class except that 
 * the {@link #toString()} method has been overloaded to correctly print the 
 * contents of the dictionary when logging.</p>
 * (c) 2017 Sport Trades Ltd
 * 
 * @author Jas Rajasansir
 * @version 1.0.1
 * @since 22 Jun 2015
 */
public class PrintableDict extends Dict {

	public PrintableDict(Object X, Object Y) {
		super(X, Y);
	}
	
	/** Provides an easy way to build a new instance of this object for an existing {@link Dict} object. */
	public PrintableDict(Dict dict) {
		super(dict.x, dict.y);
	}
	
	@Override
	public String toString() {
		Object[] keys = (Object[]) x;
		Object[] vals = (Object[]) y;
		
		StringBuilder dictStr = new StringBuilder().append("{ ");
		
		for(int kCnt = 0; kCnt < keys.length; kCnt++) {
			dictStr.append(keys[kCnt] + " = ");
			
			Object val = vals[kCnt];
			
			if(val instanceof Object[])
				dictStr.append(Printers.arrayToString((Object[]) vals[kCnt]));
			else
				dictStr.append(val.toString());
			
			dictStr.append((kCnt == keys.length - 1) ? "" : "; ");
		}
		
		return dictStr.append(" }").toString();
	}
	
	/** @return The number of entries in the dictionary (equivalent to the kdb+ <code>count</code> function) */
	public int getSize() {
		if(x == null)
			return 0;
		
		return ((Object[]) x).length;
	}
}
