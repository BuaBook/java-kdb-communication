package com.buabook.kdb.interfaces;

import com.buabook.kdb.consumer.KdbConsumer;
import com.buabook.kdb.data.KdbTable;
import com.buabook.kdb.exceptions.DataConsumerException;

/**
 * <h3>{@link KdbTable} Data Consumer Interface</h3>
 * <p>Interface to specify support for an object to consume a kdb object, wrapped
 * in the custom {@link KdbTable} format</p>
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.0.1
 * @since 27 Apr 2014
 * 
 * @see KdbConsumer
 */
public interface IKdbTableConsumer {
	
	/**
	 * Consumes the specified kdb table to perform some action on the received data.
	 * @param table The table of data received from a kdb process
	 */
	public void consume(KdbTable table) throws DataConsumerException;
}
