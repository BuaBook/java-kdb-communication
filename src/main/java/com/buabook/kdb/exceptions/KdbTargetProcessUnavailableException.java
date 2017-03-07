package com.buabook.kdb.exceptions;

/**
 * <h3>KdbTargetProcessUnavailableException</h3>
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.0.0
 * @since 15 Apr 2014
 */
public class KdbTargetProcessUnavailableException extends Exception {
	private static final long serialVersionUID = -4913861976678362516L;
	
	private static final String message = "This Java process could not connect to the specified kdb process.";

	public KdbTargetProcessUnavailableException() {
		super(message);
	}

	public KdbTargetProcessUnavailableException(String arg0) {
		super(message + " " + arg0);
	}

	public KdbTargetProcessUnavailableException(Throwable arg0) {
		super(message, arg0);
	}

	public KdbTargetProcessUnavailableException(String arg0, Throwable arg1) {
		super(message + " " + arg0, arg1);
	}

	public KdbTargetProcessUnavailableException(String arg0, Throwable arg1, boolean arg2, boolean arg3) {
		super(message + " " + arg0, arg1, arg2, arg3);
	}

}
