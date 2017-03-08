package com.buabook.kdb.exceptions;

/**
 * <h3>DictUnionNotPermittedException</h3>
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.0.0
 * @since 6 Apr 2014
 */
public class DictUnionNotPermittedException extends RuntimeException {
	private static final long serialVersionUID = -3127901545788139093L;
	
	private static final String message = "The attempted union of the two rows failed.";


	public DictUnionNotPermittedException(String arg0) {
		super(message + " " + arg0);
	}

}
