package com.buabook.kdb.publisher;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.buabook.kdb.connection.KdbProcess;
import com.buabook.kdb.data.KdbTable;
import com.buabook.kdb.exceptions.KdbTargetProcessUnavailableException;
import com.google.common.base.Strings;
import com.kx.c.Flip;

/**
 * <h3>KDB Publisher Thread</h3>
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.0.3
 * @since 17 Apr 2014
 */
public class KdbPublisherThread extends Thread {
	private static final Logger log = LoggerFactory.getLogger(KdbPublisherThread.class);
	
	/** Specifies the default time the thread should sleep between cycles */
	private static final Long DEFAULT_THREAD_SLEEP_MS = 10l;
	
	
	/** The time the thread should sleep between cycles */
	private final Long threadSleepMs;
	
	/** The publisher class containing the code to publish to the KDB process */
	private KdbPublisher publisher;
	
	/** Buffer for all tables that are to be published on this thread to the KDB process */
	private ConcurrentLinkedQueue<KdbTable> publishBuffer;
	
	
	/**
	 * Configures the publisher thread with the default thread sleep interval 
	 * @see #KdbPublisherThread(KdbProcess, Long) 
	 * @see #DEFAULT_THREAD_SLEEP_MS
	 */
	public KdbPublisherThread(KdbProcess server) throws KdbTargetProcessUnavailableException {
		this(server, DEFAULT_THREAD_SLEEP_MS);
	}
	
	/**
	 * Provides a instantiation of a {@link KdbPublisher} but wrapped up in this threaded
	 * class to allow execution to occur separately from the main thread
	 * @param server The KDB process to connect to
	 * @param threadSleepMs The time the thread should sleep when there is nothing to publish
	 * @throws KdbTargetProcessUnavailableException If the KDB process is unavailable at instantiation time
	 * @see KdbPublisher#KdbPublisher(KdbProcess)
	 */
	public KdbPublisherThread(KdbProcess server, Long threadSleepMs) throws KdbTargetProcessUnavailableException {
		this(server, threadSleepMs, null);
	}

	public KdbPublisherThread(KdbProcess server, Long threadSleepMs, Duration resetConnectionDuration) throws KdbTargetProcessUnavailableException {
		this(new KdbPublisher(server, resetConnectionDuration), threadSleepMs);
	}
	
	public KdbPublisherThread(KdbPublisher publisher, Long threadSleepMs) {
		super();
		
		this.threadSleepMs = threadSleepMs;
		this.publisher = publisher;
		this.publishBuffer = new ConcurrentLinkedQueue<>();
		this.setName("KdbPublisher-" + publisher.getRemoteProcess().getHostname() + "-" + publisher.getRemoteProcess().getPort());
		
		this.start();
	}


	/**
	 * Thread sleeps for the specified {@link #threadSleepMs} before retrieving the latest
	 * table from the head of the queue and publishes to the kdb process.
	 * @see Thread#sleep(long)
	 * @see KdbPublisher#isConnected()
	 * @see KdbPublisher#publish(KdbTable)
	 */
	@Override
	public void run() {
		while(publisher.isConnected()) {
			
			if(publishBuffer.isEmpty()) {
				try {
					Thread.sleep(threadSleepMs);
				} catch (InterruptedException e) { }
				
				continue;
			}
			
			KdbTable toPublish = publishBuffer.peek();
			
			if(log.isDebugEnabled())
				log.debug("Publishing table update [ Table Name: {} ] [ Table Size: {} ] [ Queue Size: {} ]", toPublish.getTableName(), toPublish.getRowCount(), publishBuffer.size());
			
			boolean published = publisher.publish(toPublish);
			
			if(! published) {
				log.warn("Kdb publishing failed. Will reattempt again. [ Table: {} ] [ Queue Size: {} ]", toPublish.getTableName(), publishBuffer.size());
				continue;
			}

			publishBuffer.remove();
		};
		
		log.error("KDB publisher thread has disconnected, thread will now exit [ KDB Process: {} ]", publisher);
		
		// Let's empty the buffer and null it so anyone attempting to add more elements knows that
		// this publisher is dead
		publishBuffer.clear();
		publishBuffer = null;
	}
	
	/** 
	 * <p>This method adds the list of tables to the publish buffer ({@link #publishBuffer}) ready for
	 * publishing to the kdb process on the next iteration of the main thread loop.</p>
	 * <p><b>NOTE:</b> Do not send lists with <code>null</code> {@link KdbTable} elements.</p> 
	 * @see ConcurrentLinkedQueue#addAll(java.util.Collection)
	 */
	public void publish(List<KdbTable> tables) {
		if(tables == null || tables.isEmpty())
			return;
		
		if(tables.contains(null)) {
			int beforeNullSize = tables.size();
			
			tables = tables.stream()
								.filter(Objects::nonNull)
								.collect(Collectors.toList());

			log.warn("One or more tables to be published are null and will not be published [ Before Size: {} ] [ After Size: {} ]", beforeNullSize, tables.size());
		}
		
		publishBuffer.addAll(tables);
	}
	
	/** @see KdbPublisher#publish(KdbTable) */
	public void publish(KdbTable table) {
		if(table == null)
			return;
		
		publishBuffer.add(table);
	}
	
	/** @see KdbPublisher#publish(String, Flip) */
	public void publish(String tableName, Flip tableData) { 
		if(Strings.isNullOrEmpty(tableName) || tableData == null)
			return;
		
		publish(new KdbTable(tableName, tableData));
	}
	
	/** @see KdbPublisher#disconnect() */
	public synchronized void disconnect() {
		publisher.disconnect();
	}
}
