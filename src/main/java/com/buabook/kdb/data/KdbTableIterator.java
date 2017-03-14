package com.buabook.kdb.data;

import java.util.Iterator;

/**
 * <h2>Iterator for {@link KdbTable}</h2>
 * <p>The iterator for {@link KdbTable} to allow iteration over the table with the {@link KdbDict} class.</p>
 * (c) 2014 - 2017 Sport Trades Ltd
 * 
 * @see KdbTable
 *
 * @author Jas Rajasansir
 * @version 1.1.0
 * @since 11 Aug 2014
 */
class KdbTableIterator implements Iterator<KdbDict> {
	
	private int rowCounter;
	
	private final KdbTable table; 

	
	public KdbTableIterator(KdbTable table) {
		this.rowCounter = 0;
		this.table = table;
	}

	
	@Override
	public boolean hasNext() {
		return rowCounter < table.getRowCount();
	}

	@Override
	public KdbDict next() {
		KdbDict next = table.getRow(rowCounter);
		rowCounter++;
		
		return next;
	}

	@Override
	public void remove() {}

}
