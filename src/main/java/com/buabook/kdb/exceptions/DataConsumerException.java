package com.buabook.kdb.exceptions;

/**
 * (c) 2015 Sport Trades Ltd
 * 
 * @author Jas Rajasansir
 * @version 1.0.0
 * @since 15 Mar 2015
 */
public class DataConsumerException extends Exception {
	private static final long serialVersionUID = 7241519361208107148L;
	
	private static final String message = "The data consuming function failed.";

	public DataConsumerException() {
		super(message);
	}

	public DataConsumerException(String message) {
		super(DataConsumerException.message + " " + message);
	}

	public DataConsumerException(Throwable cause) {
		super(DataConsumerException.message + " " + message,cause);
	}

	public DataConsumerException(String message, Throwable cause) {
		super(DataConsumerException.message + " " + message, cause);
	}

	public DataConsumerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(DataConsumerException.message + " " + message, cause, enableSuppression, writableStackTrace);
	}

}
