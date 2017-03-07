package com.buabook.kdb.publisher;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.buabook.kdb.connection.KdbProcess;
import com.buabook.kdb.data.KdbTable;
import com.buabook.kdb.exceptions.KdbPublisherAlreadyExistsException;
import com.buabook.kdb.exceptions.KdbPublisherDoesNotExistException;
import com.buabook.kdb.exceptions.KdbTargetProcessUnavailableException;

/**
 * <h3>KDB Publisher Manager</h3>
 * <p>Provides the ability to maintain numerous kdb publishers and interface with them 
 * all through a single method call, within this class.</p>
 * <p>Use cases include PROD & DR dual-publishing</p>
 * (c) 2014 - 2017 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.0.2
 * @since 18 Apr 2014
 */
public class KdbPublisherManager {
	private static final Logger log = LoggerFactory.getLogger(KdbPublisherManager.class);
	
	/** Map of publishers that are available */
	private final ConcurrentHashMap<KdbProcess, KdbPublisherThread> publishers;
	
	
	public KdbPublisherManager() {
		publishers = new ConcurrentHashMap<>();
	}
	
	
	public void addPublisher(KdbProcess server) throws KdbTargetProcessUnavailableException, KdbPublisherAlreadyExistsException {
		if(publishers.containsKey(server)) {
			log.error("This manager already contains a publisher to this KDB process! [ Process: " + server.toString() + " ]");
			throw new KdbPublisherAlreadyExistsException(server.toString());
		}
		
		publishers.put(server, new KdbPublisherThread(server));
	}
	
	/** Publishes the specified tables to all processes that are managed by this class. */
	public void publish(List<KdbTable> tables) {
		publishers.forEach((process, publisher) -> publisher.publish(tables));
	}
	
	/**
	 * Provides the ability to publish a number of {@link KdbTable}'s to a specified list of 
	 * kdb processes. These processes must already have been added to this manager before attempting
	 * to publish. 
	 * @param servers The known kdb processes to publish the data to
	 * @param tables The tables to publish to the kdb processes
	 * @throws KdbPublisherDoesNotExistException If a server is passed that is not known to this manager
	 */
	public void publish(List<KdbProcess> servers, List<KdbTable> tables) throws KdbPublisherDoesNotExistException {
		if(! publishers.keySet().containsAll(servers)) {
			log.error("One or more of the specified KDB processes do not have a publisher defined in this manager!");
			throw new KdbPublisherDoesNotExistException();
		}
		
		for(KdbProcess server : servers)
			publishers.get(server).publish(tables);
	}
	
	/** @return All target kdb processes that are controlled by this publish manager */
	public List<KdbProcess> getTargetProcesses() {
		return Collections.list(publishers.keys());
	}
	
	/**
	 * Disconnects from the specified kdb server and removes it from the internal management object 
	 * @param server The kdb process to disconnect from
	 * @throws KdbPublisherDoesNotExistException If the specified kdb process does not exist in this manager 
	 */
	public void disconnect(KdbProcess server) throws KdbPublisherDoesNotExistException {
		if(! publishers.containsKey(server)) {
			log.error("The specified KDB process does not exist within this manager [ Process: " + server.toString() + " ]");
			throw new KdbPublisherDoesNotExistException(server.toString());
		}
		
		log.info("Disconnect request received for server: " + server.toString());
		
		publishers.get(server).disconnect();
		publishers.remove(server);
	}
	
	/** Simple wrapper for {@link #disconnect(KdbProcess)} to disconnect all processes in the current manager. */
	public void shutdown() {
		publishers.keySet().forEach(this::disconnect);
	}
}
