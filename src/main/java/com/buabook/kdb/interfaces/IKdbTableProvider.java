package com.buabook.kdb.interfaces;

import java.util.List;

import com.buabook.kdb.data.KdbTable;
import com.google.common.collect.Lists;

/**
 * <h3>{@link KdbTable} Data Provider Interface</h3>
 * <p>Interface for all classes that can provide a table of data for kdb (using
 * the {@link KdbTable} class)</p>
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.0.1
 * @since 6 Apr 2014
 */
public interface IKdbTableProvider {
	
	/**
	 * Provides the object's data in {@link KdbTable} format, in the anticipation
	 * that it will be sent to a kdb process. A list is expected due to the complex 
	 * nested types that comes with XML data.
	 * @return The object's data
	 * @see Lists#newArrayList(Object...)
	 */
	public List<KdbTable> getKdbTables();
}
