package com.buabook.kdb.exceptions;

/**
 * <h3>QueryExecutionFailedException</h3>
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.0.0
 * @since 8 Jun 2014
 */
public class QueryExecutionFailedException extends Exception {
	private static final long serialVersionUID = 265769875753286622L;
	
	private static final String message = "The supplied query failed to execute.";

	public QueryExecutionFailedException() {
		super(message);
	}

	public QueryExecutionFailedException(String msg) {
		super(message + " " + msg);
	}

	public QueryExecutionFailedException(Throwable cause) {
		super(message, cause);
	}

	public QueryExecutionFailedException(String msg, Throwable cause) {
		super(message + " " + msg, cause);
	}

}
