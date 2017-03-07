package com.buabook.kdb;

import com.buabook.kdb.data.KdbTable;
import com.kx.c.Flip;

/**
 * <h3>{@link Flip} and {@link KdbTable} Helpers</h3>
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.0.2
 * @since 15 Apr 2014
 */
public final class Flips {
	
	/**
	 * Returns the row count of the specified table
	 * @param table The table to count the number of rows
	 * @return The row count
	 * @see Flips#getColumn(Flip, Integer)
	 */
	public static int getRowCount(Flip table) {
		Object[] firstCol = getColumn(table, 0);
		
		if(firstCol == null)
			return 0;
		
		return firstCol.length;
	}
	
	/**
	 * Returns the specified column from the data. This column value is indexed from 0
	 * @param table The table to get the column from
	 * @param column The index of the column to return
	 * @return The column
	 */
	public static Object[] getColumn(Flip table, Integer column) {
		if(table == null || column == null)
			return null;
		
		if(table.y == null)
			return null;
		
		return (Object[]) table.y[column];
	}
	
	/** @return <code>true</code> if the specified table is <code>null</code> or has no data in it; <code>false</code> otherwise */
	public static boolean isNullOrEmpty(KdbTable table) {
		return table == null || table.isEmpty();
	}
	
	/** @return <code>true</code> if the specified table is <code>null</code> or has no data in it; <code>false</code> otherwise */
	public static boolean isNullOrEmpty(Flip table) {
		return table == null || getRowCount(table) == 0;
	}
}
