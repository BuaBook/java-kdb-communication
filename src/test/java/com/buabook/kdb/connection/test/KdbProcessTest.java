package com.buabook.kdb.connection.test;

import org.junit.Test;

import com.buabook.kdb.connection.KdbProcess;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class KdbProcessTest {

	// KdbProcess(String, Integer)
	
	@Test
	public void testConstructorAcceptsAllCominbationsOfHostAndPort() {
		new KdbProcess("hostname", 12345);
		new KdbProcess(null, 12345);
		new KdbProcess("hostname", (Integer) null);
	}

	// KdbProcess(String, String)
	
	@Test
	public void testConstructorAcceptsHostnameAndPort() {
		new KdbProcess("hostname", "12345");
	}
	
	@Test(expected=NumberFormatException.class)
	public void testConstructorThrowsExceptionIfNonIntegerPort() {
		new KdbProcess("hostname", "hostname");
	}
	
	@Test(expected=NumberFormatException.class)
	public void testConstructorThrowsExceptionIfNullPort() {
		new KdbProcess("hostname", (String) null);
	}
	
	// KdbProcess(String, Integer, String, String)
	
	@Test
	public void testConstructorWithIntegerPortAcceptsAllCombinationsOfUsernameAndPassword() {
		new KdbProcess("hostname", 12345, "username", "password");
		new KdbProcess("hostname", 12345, "username", null);
		new KdbProcess("hostname", 12345, null, "password");
	}
	
	// KdbProcess(String, String, String, String)
	
	@Test
	public void testConstructorWithStringPortAcceptsAllCombinationsOfUsernameAndPassword() {
		new KdbProcess("hostname", "12345", "username", "password");
		new KdbProcess("hostname", "12345", "username", null);
		new KdbProcess("hostname", "12345", null, "password");
	}
	
	// KdbProcess.getUsername
	
	@Test
	public void testGetUsernameReturnsUsername() {
		KdbProcess process = new KdbProcess("hostname", 12345, "username", "password");
		assertThat(process.getUsername(), is(equalTo("username")));
	}
	
	@Test
	public void testGetUsernameReturnsNullIfNullUsername() {
		KdbProcess process = new KdbProcess("hostname", 12345, null, "password");
		assertThat(process.getUsername(), is(nullValue()));
	}
	
	// KdbProcess.getPassword
	
	@Test
	public void testGetPasswordReturnsPassword() {
		KdbProcess process = new KdbProcess("hostname", 12345, "username", "password");
		assertThat(process.getPassword(), is(equalTo("password")));
	}
	
	@Test
	public void testGetPasswordReturnsNullIfNullPassword() {
		KdbProcess process = new KdbProcess("hostname", 12345, "username", null);
		assertThat(process.getPassword(), is(nullValue()));
	}
	
	// KdbProcess.toString
	
	@Test
	public void testToStringReturnsHostnameAndPortIfNoUsernameAndPassword() {
		KdbProcess process = new KdbProcess("hostname", 12455, null, null);
		assertThat(process.toString(), containsString("hostname:12455"));
	}
	
	@Test
	public void testToStringReturnsHostnamePortUsernameIfNoPassword() {
		KdbProcess process = new KdbProcess("hostname", 12345, "username", null);
		assertThat(process.toString(), containsString("hostname:12345"));
		assertThat(process.toString(), containsString("username"));
	}
	
	@Test
	public void testToStringReturnsHostnamePortUsernamePassword() {
		KdbProcess process = new KdbProcess("hostname", 12345, "username", "password");
		assertThat(process.toString(), containsString("hostname:12345"));
		assertThat(process.toString(), containsString("username"));
		assertThat(process.toString(), containsString("hostname:12345"));
		assertThat(process.toString(), containsString("password"));
	}
	
	// KdbProcess.getUserAndPassword
	
	@Test
	public void testGetUserAndPasswordReturnsEmptyStringIfNoUsername() {
		KdbProcess process = new KdbProcess("hostname", 12345, null, null);
		assertThat(process.getUserAndPassword(), is(emptyString()));
	}
	
	@Test
	public void testGetUserAndPasswordReturnsUsernameIfNoPassword() {
		KdbProcess process = new KdbProcess("hostname", 12345, "username", null);
		assertThat(process.getUserAndPassword(), is(equalTo("username")));
	}
	
	@Test
	public void testGetUserAndPasswordReturnsUserAndPasswordInKdbConnectionFormat() {
		KdbProcess process = new KdbProcess("hostname", 12345, "username", "password");
		assertThat(process.getUserAndPassword(), is(equalTo("username:password")));
	}
	
	// KdbProcess.equals
	
	@Test
	public void testEqualsReturnsTrueIfHostPortUsernamePasswordMatch() {
		KdbProcess process = new KdbProcess("hostname", 12345, "username", "password");
		
		KdbProcess differentUser = new KdbProcess("hostname", 12345, "a-different-username", "password");
		KdbProcess differentPass = new KdbProcess("hostname", 12345, "username", "a-different-password");
		
		assertThat(process.equals(null), is(equalTo(false)));
		assertThat(process.equals("string"), is(equalTo(false)));
		assertThat(process.equals(process), is(equalTo(true)));
		assertThat(process.equals(differentUser), is(equalTo(false)));
		assertThat(process.equals(differentPass), is(equalTo(false)));
	}
	
	// KdbProces.hashCode
	
	@Test
	public void testHashCodeReturnsSameHashCodeForSameObject() {
		KdbProcess process1 = new KdbProcess("hostname", 12345, "username", "password");
		KdbProcess process2 = new KdbProcess("hostname", 12345, "username", "password");
		
		assertThat(process1.hashCode(), is(equalTo(process2.hashCode())));
	}
}
