package com.buabook.kdb.exceptions;

/**
 * <h3>KdbPublisherAlreadyExistsException</h3>
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.0.0
 * @since 17 Apr 2014
 */
public class KdbPublisherAlreadyExistsException extends Exception {
	private static final long serialVersionUID = -7182809623913032050L;
	
	private static final String message = "There is already a KDB publisher for the specified KDB process.";


	public KdbPublisherAlreadyExistsException(String arg0) {
		super(message + " " +arg0);
	}
}
