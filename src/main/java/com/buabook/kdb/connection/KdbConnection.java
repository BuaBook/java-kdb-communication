package com.buabook.kdb.connection;

import java.io.IOException;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.buabook.kdb.exceptions.KdbTargetProcessUnavailableException;
import com.kx.c;
import com.kx.c.KException;

/**
 * <h3>KDB Connection Wrapper</h3>
 * <p>Class provides connect / disconnect / reconnect functionality on top
 * of the default {@link c} class.</p>
 * <p>NOTE: By default this class exposes no send / receive functionality. This
 * is left up to extension classes to provide these features.</p>
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.0.1
 * @since 27 Apr 2014
 */
public class KdbConnection implements AutoCloseable {
	private static final Logger log = LoggerFactory.getLogger(KdbConnection.class);
	
	/** The default reconnection interval in milliseconds */
	private static final Integer DEFAULT_RECONNECT_INTERVAL_MS = 2000;
	
	
	/** The amount of time in milliseconds to wait between connection attempts */
	protected final Integer reconnectIntervalMs; 
	
	/** The details of the process to connect to */
	private final KdbProcess process;
	
	
	/** The current active connection to the kdb process */
	private c connection;

	
	/**
	 * Creates a new kdb connection object with the reconnection interval set as the default
	 * (as defined by {@link #DEFAULT_RECONNECT_INTERVAL_MS}. <b>NOTE</b>: Calling this constructor will 
	 * not initiate the connection. This must be done manually by calling {@link #connect()}.
	 * @param process The kdb process to connect to
	 * @see #KdbConnection(KdbProcess, Integer)
	 */
	public KdbConnection(KdbProcess process) {
		this(process, DEFAULT_RECONNECT_INTERVAL_MS);
	}
	
	/**
	 * Creates a new kdb connection object with the specified reconnection interval. <b>NOTE</b>: Calling
	 * this constructor will not initiate the connection. This must be done manually by calling {@link #connect()}.
	 * @param process The kdb process to connect to
	 * @param reconnectIntervalMs The reconnect interval (in milliseconds) in case the connection is lost
	 */
	public KdbConnection(KdbProcess process, Integer reconnectIntervalMs) {
		this.process = process;
		this.reconnectIntervalMs = reconnectIntervalMs;
	}
	
	/**
	 * Performs the connection to the KDB process
	 * @throws KdbTargetProcessUnavailableException If the target KDB process is unavailable
	 * @see #reconnect()
	 */
	public void connect() throws KdbTargetProcessUnavailableException {
		if(process == null)
			throw new KdbTargetProcessUnavailableException(new NullPointerException("The process specified is null!"));
		
		if(isConnected())
			return;
		
		log.info("Attempting to connect to: {}", process.toString());
		
		try {
			connection = new c(process.getHostname(), process.getPort(), process.getUserAndPassword());
		} catch (KException | IOException e) {
			connection = null;
			log.error("Failed to connect to '{}'. Error - {}", process, e.getMessage());
			throw new KdbTargetProcessUnavailableException("Target: " + process, e);
		}
		
		log.info("Successfully connected to: {}", process.toString());
	}
	
	/** Disconnects from the KDB process (by closing the underlying socket) */
	public void disconnect() {
		if(! isConnected())
			return;
		
		try {
			connection.close();
		} catch (IOException e) {
			log.debug("Failed to disconnect from the KDB process. Will null socket manually. Error - {}", e.getMessage());
		}
		
		connection = null;
		
		log.info("Disconnected from KDB process: {}", process);
	}
	
	/**
	 * @return <code>False</code> if either the connection object or the connection {@link Socket} is
	 * <code>null</code>. <b>NOTE:</b> These are the only two circumstances where <code>false</code>
	 * will be returned. <code>True</code> will be returned in all other cases.
	 */
	public boolean isConnected() {
		if(connection == null || connection.s == null)
			return false;
		
		return true;
	}
	
	/** @return The current connection to the kdb process */
	public c getConnection() {
		return connection;
	}
	
	/** @return The remote process that this connection object is connected to */
	public KdbProcess getRemoteProcess() {
		return process;
	}
	
	/**
	 * <p>Provides the ability to reconnect to the target KDB process if necessary (generally after an {@link IOException}).
	 * Function will also force a disconnect of the underlying {@link Socket}, just to be sure.</p>
	 * <p><b>NOTE:</b> This function will block in a retry loop if the kdb process is unavailable when attempting
	 * to connect.</p>
	 * @see #reconnectIntervalMs
	 */
	public void reconnect() {
		disconnect();

		while (! isConnected()) {
			try {
				connect();
			} catch (KdbTargetProcessUnavailableException e) {
				log.warn("KDB process is still unavailable. Waiting {} ms... [ Process: {} ]", reconnectIntervalMs, process);
				
				try {
					Thread.sleep(reconnectIntervalMs);
				} catch (InterruptedException e1) {}
			}
		}
		
		log.info("Successfully reconnected to KDB process [ Process: {} ]", process);
	}

	@Override
	public void close() {
		disconnect();
	}
}
