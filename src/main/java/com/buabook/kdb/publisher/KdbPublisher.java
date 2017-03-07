package com.buabook.kdb.publisher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.buabook.kdb.Flips;
import com.buabook.kdb.connection.KdbConnection;
import com.buabook.kdb.connection.KdbProcess;
import com.buabook.kdb.data.KdbTable;
import com.buabook.kdb.exceptions.KdbTargetProcessUnavailableException;
import com.google.common.base.Strings;
import com.kx.c.Flip;

/**
 * <h3>KDB Data Publisher</h3>
 * <p>Provides the ability to publish some data (in either {@link KdbTable} or 
 * {@link Flip} format) to a specific kdb process.</p>
 * <p>Any instantiation of this class will cause the publisher to run in the current
 * thread. Use {@link KdbPublisherThread} if you need a new thread.</p>
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.1.0
 * @since 6 Apr 2014
 */
public class KdbPublisher extends KdbConnection {
	private static final Logger log = LoggerFactory.getLogger(KdbPublisher.class);
	
	/** @see #resetConnectionDuration */ 
	private static final Duration DEFAULT_RESET_CONNECTION_DURATION = Duration.standardMinutes(30);
	

	/**
	 * <p>The maximum amount of time after the last publish / connect allowed before the connection to the kdb
	 * process is reset. This is due to socket timeout on the target machine that may not be noticed by the
	 * local Java process.</p>
	 * <p>On Linux boxes, <code>/proc/sys/net/ipv4/tcp_keepalive_time</code> will tell you how long a socket 
	 * can stay open.</p>
	 */
	private final Duration resetConnectionDuration;
	
	/** The time of the last successful publish to the kdb process */
	private DateTime lastPublishTime;

	
	/** @see #KdbPublisher(KdbProcess, Duration) */
	public KdbPublisher(KdbProcess server) throws KdbTargetProcessUnavailableException {
		this(server, null);
	}
	
	/**
	 * Generates a new KDB publisher. <b>NOTE</b>: The process must be online and
	 * available to connect to when this constructor is called; reconnection is not supported here.
	 * @param server The server to connect to 
	 * @param resetConnectionDuration The maximum time allowed between publishes before the connection is reset. If this is 
	 * <code>null</code>, {@link #DEFAULT_RESET_CONNECTION_DURATION} will be used
	 * @throws KdbTargetProcessUnavailableException If the target KDB process is unavailable
	 * @see #connect()
	 * @see #resetConnectionDuration
	 */
	public KdbPublisher(KdbProcess server, Duration resetConnectionDuration) throws KdbTargetProcessUnavailableException {
		super(server);
		connect();
		
		if(resetConnectionDuration == null)
			this.resetConnectionDuration = DEFAULT_RESET_CONNECTION_DURATION;
		else
			this.resetConnectionDuration = resetConnectionDuration;
		
		this.lastPublishTime = DateTime.now();
		
		log.info("Successfully connected to kdb process for publishing [ Target: {} ] [ Connection Reset After: {} ]", server, resetConnectionDuration);
	}

	/**
	 * Allows a list of tables to be published (in sequence) to the target KDB process
	 * @param tables The list of tables to publish
	 * @return The publish status for each table in the list
	 * @see #publish(KdbTable)
	 */
	public List<Boolean> publish(List<KdbTable> tables) {
		if(tables == null || tables.size() == 0)
			return new ArrayList<>();
		
		List<Boolean> results = new ArrayList<>();
		
		for(KdbTable t : tables)
			results.add(publish(t));
		
		return results;
	}
	
	/**
	 * Allows a table (as an internal {@link KdbTable} to be published to the target KDB process
	 * @param table The table to publish
	 * @return <code>true</code> if the publish was successful, <code>false</code> otherwise
	 * @see #publish(String, Flip)
	 */
	public Boolean publish(KdbTable table) {
		if(table == null)
			return false;
		
		return publish(table.getTableName(), table.convertToFlip());
	}
	
	/**
	 * Performs the publish of a {@link Flip} table structure to the target KDB process
	 * @param tableName The name of the table to publish
	 * @param tableData The table contents
	 * @return <code>true</code> if the publish was successful, <code>false</code> otherwise. <b>NOTE</b>:
	 * The function will also return <code>true</code> if either parameter is <code>null</code>
	 */
	public Boolean publish(String tableName, Flip tableData) {
		if(Strings.isNullOrEmpty(tableName) || tableData == null)
			return true;
		
		if(lastPublishTime.plus(resetConnectionDuration).isBeforeNow()) {
			log.info("Maximum connection duration has elapsed. Resetting connection before publishing");
			reconnect();
		}
		
		if(! isConnected()) {
			log.warn("Connection to kdb process lost! Attempting reconnect now...");
			reconnect();
		}
		
		log.debug("Publishing table update [ Table Name: {} ] [ Table Size: {} ]", tableName, Flips.getRowCount(tableData));
		
		try {
			getConnection().ks(".u.upd", tableName, tableData);
		} catch (RuntimeException e) { 
			log.error("Uncaught RuntimeException during publishing. Error - {}", e.getMessage(), e);
			return false;
		} catch (IOException e) {
			log.error("Basic I/O exception occurred. Assuming connection has been corrupted. Attempting reconnect. Error - {}", e.getMessage());
			reconnect();
			
			return false;
		}
		
		lastPublishTime = DateTime.now();
		
		return true;
	}
}
