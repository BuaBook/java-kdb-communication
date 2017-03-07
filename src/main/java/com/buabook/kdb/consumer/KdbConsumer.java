package com.buabook.kdb.consumer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.buabook.common.Printers;
import com.buabook.kdb.connection.KdbConnection;
import com.buabook.kdb.connection.KdbProcess;
import com.buabook.kdb.data.KdbDict;
import com.buabook.kdb.data.KdbTable;
import com.buabook.kdb.exceptions.DataConsumerException;
import com.buabook.kdb.exceptions.KdbProcessSubscriptionFailedException;
import com.buabook.kdb.exceptions.KdbTargetProcessUnavailableException;
import com.buabook.kdb.interfaces.IKdbRawDataConsumer;
import com.buabook.kdb.interfaces.IKdbTableConsumer;
import com.google.common.collect.ImmutableList;
import com.kx.c.Dict;
import com.kx.c.Flip;
import com.kx.c.KException;

/**
 * <h3>KDB Data Consumer</h3>
 * <p>Provides the ability to consume real-time streaming data from a KDB process
 * into a {@link KdbTable} for use within a Java application</p>
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.1.0
 * @since 23 Apr 2014
 */
public class KdbConsumer extends KdbConnection {
	private static final Logger log = LoggerFactory.getLogger(KdbConsumer.class);
	
	private static final List<String> SUPPORTED_UPD_FUNCTIONS = ImmutableList.<String>builder()
																							.add("upd")
																							.add(".u.upd")
																							.build();
	
	private static final String SUB_FUNCTION = ".u.sub";
	
	/** The length of the array returned by kdb in order for it to be considered as a valid update message */
	private static final Integer UPD_ARRAY_LENGTH = 3;

	
	/** The list of tables that this consumer will subscribe to */
	private final List<String> subscriptionTables;
	
	/** An optional dictionary of subscription information if the upstream kdb process supports subscription configuration */
	private final KdbDict subscriptionConfiguration;
	
	/** A listener object that will consume <b>any</b> valid message received from the kdb process */
	private final IKdbRawDataConsumer rawDataConsumer;
	
	/** A listener object that will only consume table updates */ 
	private final IKdbTableConsumer tableConsumer;
	
	
	private KdbConsumer(KdbProcess server, List<String> tables, KdbDict subscriptionConfiguration, IKdbRawDataConsumer rawDataConsumer, IKdbTableConsumer tableConsumer) throws KdbTargetProcessUnavailableException {
		super(server);
		
		if(rawDataConsumer == null && tableConsumer == null)
			throw new NullPointerException("Must provied either a raw data or KdbTable consuming object, or both to this object");
		
		this.subscriptionTables = tables;
		this.subscriptionConfiguration = subscriptionConfiguration;
		this.rawDataConsumer = rawDataConsumer;
		this.tableConsumer = tableConsumer;
		
		connect();
		log.info("Connected to kdb process [ Target: " + server.toString() + " ]");
	}

	/**
	 * Generates a new kdb consumer (which is generally a consumer from a kdb TickerPlant)
	 * @param server The kdb process to connect to
	 * @param tables The list of tables that should be subscribed to. <b>NOTE</b>: This cannot be null, pass an empty list
	 * @param rawDataConsumer A listener object that will consume every message from the kdb process
	 * @param tableConsumer A listener object that will consume only table messages from the kdb process
	 * @throws KdbTargetProcessUnavailableException If the consumer cannot connect to the target kdb process
	 * @throws NullPointerException If either <code>tables</code> or <code>syms</code> is null. Also if both <code>rawDataConsumer</code>
	 * and <code>tableConsumer</code> null, the constructor must be passed one or the other
	 */
	protected KdbConsumer(KdbProcess server, List<String> tables, IKdbRawDataConsumer rawDataConsumer, IKdbTableConsumer tableConsumer) throws KdbTargetProcessUnavailableException {
		this(server, tables, null, rawDataConsumer, tableConsumer);
		
		if(tables == null)
			throw new NullPointerException("Tables for a consumer cannot be null. Provide an empty list for ALL tables.");
	}
	
	protected KdbConsumer(KdbProcess server, KdbDict subscriptionConfiguration, IKdbRawDataConsumer rawDataConsumer, IKdbTableConsumer tableConsumer) throws KdbTargetProcessUnavailableException {
		this(server, null, subscriptionConfiguration, rawDataConsumer, tableConsumer);
		
		if(subscriptionConfiguration == null || subscriptionConfiguration.isEmpty())
			throw new NullPointerException("No subscription configuration supplied. Cannot subscribe to process");
	}
	
	/** @see #KdbConsumer(KdbProcess, List, IKdbRawDataConsumer, IKdbTableConsumer) */
	public KdbConsumer(KdbProcess server, List<String> tables, IKdbTableConsumer tableConsumer) throws KdbTargetProcessUnavailableException {
		this(server, tables, null, tableConsumer);
	}
	
	
	/**
	 * Once connection to the process has been established (performed during object construction), then this function
	 * is called to first subscribe to the kdb TickerPlant and, if successful, start listening for update messages
	 * @throws KdbProcessSubscriptionFailedException If the subscription to the TickerPlant fails
	 * @see #subscribe()
	 * @see #listen()
	 */
	public void subscribeAndListen() throws KdbProcessSubscriptionFailedException {
		Boolean sub = subscribe();
		
		if(sub)
			log.info("Subscription successful [ Process: {} ]", getRemoteProcess());
		else
			throw new KdbProcessSubscriptionFailedException("Subscribe result was neither a dictionary nor a boolean true result.");
		
		listen();
	}
	
	/**
	 * Performs the reconnection logic as defined in the super class. If the reconnection is
	 * successful then we re-subscribe to the process before continuing to listen for update
	 * messages.
	 */
	@Override
	public void reconnect() {
		super.reconnect();

		Boolean resubResult = subscribe();

		while(! resubResult) {
			log.error("Re-subscription to kdb process failed [ Process: {} ]", getRemoteProcess());
			
			try {
				Thread.sleep(super.reconnectIntervalMs);
			} catch (InterruptedException e) {}
			
			resubResult = subscribe();
		}
		
		log.info("Re-subscription successful [ Process: {} ]", getRemoteProcess());
	}

	
	/**
	 * Performs a subscription request to the kdb process (<code>.u.sub</code>) with the specified list of tables
	 * and symbols specified at object construction time
	 * @return <code>True</code> if the subscription result from the kdb process is not null, <code>false</code> otherwise 
	 */
	private Boolean subscribe() throws UnsupportedOperationException {
		Object subscribeObject = null;
		
		if(subscriptionTables != null) {
			if(subscriptionTables.isEmpty())
				subscribeObject = "";
			else
				subscribeObject = subscriptionTables.toArray();
			
			log.info("Attempting to subscribe to kdb process [ Process: {} ] [ Standard Table Subscription: {} ]", getRemoteProcess(), Printers.listToString(subscriptionTables));
		} else if(subscriptionConfiguration != null) {
			subscribeObject = subscriptionConfiguration.convertToDict();
			
			log.info("Attempting to subscribe to kdb process [ Process: {} ] [ Dict Config Subscription: {} ]", getRemoteProcess(), subscriptionConfiguration);
		} else {
			log.error("No subscription configuration or subscription tables specified. Cannot subscribe to process!");
			throw new UnsupportedOperationException("No subscription configuration or subscription tables");
		}
		
		Object subscribeResult = null;
		
		try {
			subscribeResult = getConnection().k(SUB_FUNCTION, subscribeObject);
		} catch (KException | IOException e) {
			log.error("Subscription to kdb process failed [ Process: {} ]. Error - {}", getRemoteProcess(), e.getMessage());
			return false;
		}
		
		if(subscribeResult instanceof Dict) {
			KdbDict snapshots = KdbDict.fromObject(subscribeResult);
			
			log.info("Subscription returned snapshots for tables: {}", Printers.listToString(snapshots.getKeys()));
			
			for(Object snapshot : snapshots.getKeys())
				try {
					tableConsumer.consume(new KdbTable((String) snapshot, snapshots.getAs(snapshot, Flip.class)));
				} catch (DataConsumerException e) {
					log.warn(e.getMessage(), e);
				}
		}
		
		return subscribeResult != null;
	}
	
	/**
	 * Commences listening for messages to be sent to this consumer from the kdb process. The thread this object is 
	 * running in will block waiting for each message and then route the message to the two listener objects as
	 * appropriate
	 * @see #rawDataConsumer
	 * @see #tableConsumer 
	 */
	private void listen() {
		log.debug("Commencing listening for updates from kdb process [ Process: {} ]", getRemoteProcess());
		
		while(isConnected()) {
			Object receivedKdbObject = null;
			
			try {
				receivedKdbObject = getConnection().k();
			} catch (UnsupportedEncodingException e) {
				log.warn("Unsupported data was received from the kdb process. Ignoring. Error - {}", e.getMessage());
				continue;
			} catch (KException e) {
				log.warn("KDB exception has occurred whilst trying to receive data. Ignoring. Error - {}", e.getMessage());
				continue;
			} catch (IOException e) {
				log.error("Low-level I/O exception has occurred. Disconnecting and reconnecting. Error - {}", e.getMessage());
				reconnect();
			}
			
			if(receivedKdbObject == null)
				continue;
			
			if(rawDataConsumer != null) {
				try {
					rawDataConsumer.consume(receivedKdbObject);
				} catch (DataConsumerException e) {
					log.warn(e.getMessage(), e);
				}
			}
			
			if(tableConsumer == null)
				continue;
			
			Object[] kdbObjectAsList = null;
			
			try {
				kdbObjectAsList = (Object[]) receivedKdbObject;
			} catch (ClassCastException e) {
				log.debug("Received kdb object could not be cast into an object array. Not a table update. Error - {}", e.getMessage());
				continue;
			}
			
			if(kdbObjectAsList.length != UPD_ARRAY_LENGTH) {
				log.debug("Received kdb object is not of the correct length to be a table update. [ Expected: {} ] [ Actual: {} ]", UPD_ARRAY_LENGTH, kdbObjectAsList.length);
				continue;
			}
			
			String tableName = null;
			Flip tableData = null;
			
			try {
				String updFunc = (String) kdbObjectAsList[0];
				
				if(! SUPPORTED_UPD_FUNCTIONS.contains(updFunc)) {
					log.debug("Element 0 of received list is not one of the supported upd function: {}. Not a table update message", Printers.listToString(SUPPORTED_UPD_FUNCTIONS));
					continue;
				}
				
				tableName = (String) kdbObjectAsList[1];
				tableData = (Flip) kdbObjectAsList[2];
			} catch (ClassCastException e) {
				log.debug("Received kdb object elements could not be cast into a String (for table name) or Flip (for table data). Error - {}", e.getMessage());
				continue;
			}
			
			try {
				tableConsumer.consume(new KdbTable(tableName, tableData));
			} catch (DataConsumerException e) {
				log.warn(e.getMessage(), e);
			}
		}
		
		log.warn("This consumer has disconnected from the kdb process. Listening has stopped.");
	}
}
