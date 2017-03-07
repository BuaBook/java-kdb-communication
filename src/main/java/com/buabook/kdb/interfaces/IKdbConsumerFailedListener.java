package com.buabook.kdb.interfaces;

import com.buabook.kdb.consumer.KdbConsumerThread;

/**
 * <h3>{@link KdbConsumerThread} Failure Listener</h3>
 * <p>Provides the ability for an application to be notified when a {@link KdbConsumerThread} fails
 * for any reason.</p>
 * (c) 2015 Sport Trades Ltd
 * 
 * @author Jas Rajasansir
 * @version 1.0.0
 * @since 24 Apr 2015
 */
public interface IKdbConsumerFailedListener {

	/** Standard failure causes as an enumeration for reference. */
	public enum EFailureReason {
		SUBSCRIPTION_FAILED,
		CONNECTION_FAILED
	};
	
	/**
	 * On consumer failure, this method will be caused with the reason for the failure and,
	 * optionally, any causing exception.
	 */
	public void notifyFailure(EFailureReason reason, Throwable cause);
}