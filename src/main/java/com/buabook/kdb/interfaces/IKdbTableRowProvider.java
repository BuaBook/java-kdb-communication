package com.buabook.kdb.interfaces;

import com.buabook.kdb.data.KdbDict;
import com.buabook.kdb.data.KdbTable;

/**
 * <h3>{@link KdbDict} Data Provider Interface</h3>
 * <p>Interface for all classes that can provide a row of data for kdb (using the {@link KdbDict} class)</p>
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 2.0.0
 * @since 6 Apr 2014
 */
public interface IKdbTableRowProvider {
	/**
	 * Provides the object's data in {@link KdbDict} format in anticipation that it will
	 * be sent to a kdb process
	 * @return The object's data
	 */
	public KdbDict getTableRow();
	
	/**
	 * Provides the <b>only</b> {@link KdbTable} name that is supported by the conversion
	 * of this object into a {@link KdbDict}. This will be validated on the addition of such
	 * a row into a {@link KdbTable}.
	 * @return The supported table name
	 */
	public String getTableName();
}
