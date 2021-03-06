package com.buabook.kdb.exceptions;

/**
 * <h3>TableSchemaMismatchException</h3>
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.0.0
 * @since 1 Apr 2014
 */
public class TableSchemaMismatchException extends RuntimeException {
	private static final long serialVersionUID = 4934474187618428557L;
	
	private static final String message = "The object cannot be added to the current table due to column differences.";


	public TableSchemaMismatchException(String message) {
		super(TableSchemaMismatchException.message + " " + message);
	}
}
