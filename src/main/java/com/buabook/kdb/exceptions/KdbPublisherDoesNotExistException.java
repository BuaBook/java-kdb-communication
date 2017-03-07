package com.buabook.kdb.exceptions;

/**
 * <h3>KdbPublisherDoesNotExistException</h3>
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.0.0
 * @since 17 Apr 2014
 */
public class KdbPublisherDoesNotExistException extends RuntimeException {
	private static final long serialVersionUID = 7539139533037035679L;
	
	private static final String message = "There is no publisher available to the specified KDB process.";

	public KdbPublisherDoesNotExistException() {
		super(message);
	}

	public KdbPublisherDoesNotExistException(String arg0) {
		super(message + " " +arg0);
	}

	public KdbPublisherDoesNotExistException(Throwable arg0) {
		super(message, arg0);
	}

	public KdbPublisherDoesNotExistException(String arg0, Throwable arg1) {
		super(message + " " + arg0, arg1);
	}

	public KdbPublisherDoesNotExistException(String arg0, Throwable arg1, boolean arg2, boolean arg3) {
		super(message + " " + arg0, arg1, arg2, arg3);
	}

}
