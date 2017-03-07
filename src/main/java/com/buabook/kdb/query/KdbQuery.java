package com.buabook.kdb.query;

import com.buabook.kdb.connection.KdbConnection;
import com.buabook.kdb.connection.KdbProcess;
import com.buabook.kdb.data.KdbDict;
import com.buabook.kdb.exceptions.KdbTargetProcessUnavailableException;
import com.buabook.kdb.exceptions.QueryExecutionFailedException;

/**
 * <h3>KDB Query Class (Abstract)</h3>
 * <p>Contains base connectivity (wrapping {@link KdbConnection}) and some wrapper functions
 * around the abstract {@link #query(String)} function
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.0.1
 * @since 8 Jun 2014
 */
public abstract class KdbQuery implements AutoCloseable {
	/** The connection to use to query the kdb process */
	KdbConnection connection;
	
	/** Constructs a new query object, first opening an connection to the specified process
	 * @param target The kdb process to connect to, ready for querying
	 * @throws KdbTargetProcessUnavailableException If the process is unavailable
	 */
	public KdbQuery(KdbProcess target) throws KdbTargetProcessUnavailableException {
		connection = new KdbConnection(target);
		connection.connect();
	}
	
	/**
	 * Constructs a new query object, <i>re-using</i> the specified connection object. If the connection
	 * is not currently active then a connection attempt is made. <b>NOTE</b>: You should not use the same
	 * connection object across multiple threads as data corruption is likely to occur.
	 * @param existingConnection The existing connection object to use.
	 * @throws KdbTargetProcessUnavailableException
	 */
	public KdbQuery(KdbConnection existingConnection) throws KdbTargetProcessUnavailableException {
		connection = existingConnection;

		if(! connection.isConnected())
			connection.connect();
	}
	
	/**
	 * Closes the underlying kdb connection
	 * @see KdbConnection#disconnect()
	 */
	@Override
	public void close() {
		if(! connection.isConnected())
			return;
		
		connection.disconnect();
	}
	
	/**
	 * Method to query a kdb process with a string argument only. Example: <code>"aFunction[]"</code>
	 * or <code>"aFunctionWithArgs[1;`symbol]</code>. 
	 * @param query The string query to execute
	 * @return The query result
	 * @throws QueryExecutionFailedException If the query fails for any reason
	 */
	public abstract Object query(String query) throws QueryExecutionFailedException;
	
	/**
	 * Method to query a kdb process with a function name and a dictionary argument.
	 * @param query The function to execute
	 * @param arguments The dictionary of arguments
	 * @return The query result
	 * @throws QueryExecutionFailedException If the query fails for any reason
	 */
	public abstract Object query(String query, KdbDict arguments) throws QueryExecutionFailedException;
	
}
