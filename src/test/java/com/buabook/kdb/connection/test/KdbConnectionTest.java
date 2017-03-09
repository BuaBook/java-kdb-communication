package com.buabook.kdb.connection.test;

import org.junit.Test;

import com.buabook.kdb.connection.KdbConnection;
import com.buabook.kdb.connection.KdbProcess;
import com.buabook.kdb.exceptions.KdbTargetProcessUnavailableException;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

@SuppressWarnings("resource")
public class KdbConnectionTest {

	// KdbConnection.connect
	
	@Test(expected=KdbTargetProcessUnavailableException.class)
	public void testConnectThrowsExceptionIfNullProcess() throws KdbTargetProcessUnavailableException {
		new KdbConnection(null).connect();
	}
	
	@Test(expected=KdbTargetProcessUnavailableException.class)
	public void testConnectThrowsExceptionIfCannotConnectToProcess() throws KdbTargetProcessUnavailableException {
		new KdbConnection(new KdbProcess("localhost", 1)).connect();
	}
	
	// KdbConnection.isConnected
	
	@Test
	public void testIsConnectedReturnsFalseWhenConnectionIsNotConnected() {
		KdbConnection connection = new KdbConnection(new KdbProcess("localhost", 12345));
		assertThat(connection.isConnected(), is(equalTo(false)));
	}
	
	// KdbConnection.getRemoteProcess
	
	@Test
	public void testGetRemoteProcessReturnsConfiguredProcess() {
		KdbProcess target = new KdbProcess("my-test-host.com", 34343);
		assertThat(new KdbConnection(target).getRemoteProcess(), is(equalTo(target)));
	}
}
