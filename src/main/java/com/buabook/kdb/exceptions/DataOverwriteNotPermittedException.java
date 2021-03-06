package com.buabook.kdb.exceptions;

/**
 * <h3>DataOverwriteNotPermittedException</h3>
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.0.0
 * @since 1 Apr 2014
 */
public class DataOverwriteNotPermittedException extends RuntimeException {
	private static final long serialVersionUID = 6459052823468242309L;

	private static final String message = "Data cannot be overwritten unless explicitly requested.";
	
	public DataOverwriteNotPermittedException() {
		super(message);
	}

	public DataOverwriteNotPermittedException(String arg0) {
		super(message + " " + arg0);
	}

}
