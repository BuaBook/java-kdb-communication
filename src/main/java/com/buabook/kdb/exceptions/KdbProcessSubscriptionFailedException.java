package com.buabook.kdb.exceptions;

/**
 * <h3>KdbProcessSubscriptionFailedException</h3>
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.0.0
 * @since 27 Apr 2014
 */
public class KdbProcessSubscriptionFailedException extends Exception {
	private static final long serialVersionUID = -1431592397824302942L;
	
	private static final String message = "The subscription to the specified kdb process failed.";


	public KdbProcessSubscriptionFailedException(String arg0) {
		super(message + " " + arg0);
	}
}
