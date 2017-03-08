package com.buabook.kdb.exceptions;

/**
 * <h3>TableColumnAlreadyExistsException</h3>
 * (c) 2015 Sport Trades Ltd
 * 
 * @author Jas Rajasansir
 * @version 1.0.0
 * @since 6 Apr 2015
 */
public class TableColumnAlreadyExistsException extends RuntimeException {
	private static final long serialVersionUID = -4365103741333373595L;
	
	private static final String message = "This table already contains the specified column.";


	public TableColumnAlreadyExistsException(String arg0) {
		super(message + " " + arg0);
	}

}
