package com.buabook.kdb.consumer;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.buabook.kdb.connection.KdbProcess;
import com.buabook.kdb.data.KdbDict;
import com.buabook.kdb.data.KdbTable;
import com.buabook.kdb.exceptions.KdbProcessSubscriptionFailedException;
import com.buabook.kdb.exceptions.KdbTargetProcessUnavailableException;
import com.buabook.kdb.interfaces.IKdbConsumerFailedListener;
import com.buabook.kdb.interfaces.IKdbConsumerFailedListener.EFailureReason;
import com.buabook.kdb.interfaces.IKdbRawDataConsumer;
import com.buabook.kdb.interfaces.IKdbTableConsumer;

/**
 * <h3>KDB Consumer Thread</h3>
 * <p>Wraps {@link KdbConsumer} in a separate thread</p>
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.0.1
 * @since 3 May 2014
 */
public class KdbConsumerThread extends Thread {
	private static final Logger log = LoggerFactory.getLogger(KdbConsumerThread.class);
	
	/** The underlying consumer object that will listen and notify listeners when messages are received */
	private KdbConsumer consumer;
	
	private IKdbConsumerFailedListener failureListener;
	

	/**
	 * <p><b>NOTE</b>: At least one of {@link IKdbRawDataConsumer} or {@link IKdbTableConsumer} must be supplied. Both can
	 * also be supplied.</p>
	 * 
	 * <p><b>NOTE 2</b>: If a failure listener ({@link IKdbConsumerFailedListener}) is supplied and the connection to
	 * the kdb process fails, this will be handled before the exception is thrown.</p> 
	 * 
	 * @param server The server to connect and subscribe to
	 * @param tables The list of tables to subscribe to
	 * 
	 * @param rawDataConsumer Optional class to consume the raw {@link Object}'s that are received from kdb. Pass <code>null</code> for
	 * no consumer
	 * @param tableConsumer Optional class to consume nicer {@link KdbTable}'s as received from kdb. Pass <code>null</code> for no 
	 * consumer
	 * 
	 * @param failureListener Optional class to be notified if the consumer fails to start up or subscribe. Pass <code>null</code> 
	 * for no listener
	 * 
	 * @throws KdbTargetProcessUnavailableException
	 * @see KdbConsumer#KdbConsumer(KdbProcess, List, IKdbRawDataConsumer, IKdbTableConsumer)
	 */
	public KdbConsumerThread(KdbProcess server, List<String> tables, IKdbRawDataConsumer rawDataConsumer, IKdbTableConsumer tableConsumer, IKdbConsumerFailedListener failureListener) throws KdbTargetProcessUnavailableException {

		this.failureListener = failureListener;
		
		try {
			this.consumer = new KdbConsumer(server, tables, rawDataConsumer, tableConsumer);
		} catch(KdbTargetProcessUnavailableException e) {
			if(failureListener != null)
				failureListener.notifyFailure(EFailureReason.CONNECTION_FAILED, e);
			
			throw e;
		}
		
		this.setName("KdbConsumer-" + server.getHostname() + ":" + server.getPort());
		this.start();
	}
	
	public KdbConsumerThread(KdbProcess server, KdbDict subscriptionConfig, IKdbRawDataConsumer rawDataConsumer, IKdbTableConsumer tableConsumer, IKdbConsumerFailedListener failureListener) throws KdbTargetProcessUnavailableException {

		this.failureListener = failureListener;
		
		try {
			this.consumer = new KdbConsumer(server, subscriptionConfig, rawDataConsumer, tableConsumer);
		} catch(KdbTargetProcessUnavailableException e) {
			if(failureListener != null)
				failureListener.notifyFailure(EFailureReason.CONNECTION_FAILED, e);
			
			throw e;
		}
		
		this.setName("KdbConsumer-" + server.getHostname() + ":" + server.getPort());
		this.start();
	}
	

	@Override
	public void run() {
		try {
			consumer.subscribeAndListen();
		} catch (KdbProcessSubscriptionFailedException e) {
			log.error("The consumer failed to connect and subscribe to the kdb process. This thread will now die. Error - {}", e.getMessage());
			
			if(failureListener != null)
				failureListener.notifyFailure(EFailureReason.SUBSCRIPTION_FAILED, e);
		}
	}
}
