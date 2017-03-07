package com.buabook.kdb.interfaces;

import com.buabook.kdb.consumer.KdbConsumer;
import com.buabook.kdb.exceptions.DataConsumerException;

/**
 * <h3>Raw kdb Data Consumer Interface</h3>
 * <p>Interface to specify support for an object to consume standard kdb
 * objects as they come off the wire (in {@link Object} form</p>
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.0.1
 * @since 27 Apr 2014
 * 
 * @see KdbConsumer
 */
public interface IKdbRawDataConsumer {
	
	/**
	 * Consumes the specified kdb object to perform some action on the received data.
	 * @param kdbObject The kdb object to consume
	 */
	public void consume(Object kdbObject) throws DataConsumerException;
}
