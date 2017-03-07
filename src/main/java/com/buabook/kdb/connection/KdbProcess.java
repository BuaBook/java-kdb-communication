package com.buabook.kdb.connection;

import com.buabook.common.connection.Process;
import com.google.common.base.Strings;

/**
 * <h3>kdb-specific Server Definition Container</h3>
 * (c) 2014 - 2015 Sport Trades Ltd
 *
 * @author Jas Rajasansir
 * @version 1.0.1
 * @since 31 Mar 2014
 */
public class KdbProcess extends Process {
	private final String username;
	
	private final String password;

	
	public KdbProcess(String hostname, Integer port) {
		this(hostname, port, null, null);
	}
	
	public KdbProcess(String hostname, String portStr) {
		this(hostname, portStr, null, null);
	}
	
	public KdbProcess(String hostname, Integer port, String username, String password) {
		super(hostname, port);
		
		this.username = username;
		this.password = password;
	}
	
	public KdbProcess(String hostname, String portStr, String username, String password) {
		super(hostname, portStr);
		
		this.username = username;
		this.password = password;
	}

	
	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	@Override
	public String toString() {
		String userAndPass = getUserAndPassword();
		
		if(! Strings.isNullOrEmpty(userAndPass))
			userAndPass = " (User/Pass : " + userAndPass + ")";	
		
		return "(kdb) " + getHostname() + ":" + getPort() + userAndPass;
	}
	
	/**
	 * @return The server definition as a kdb connection string (i.e. host:port)
	 */
	public String getAsKdbConnection() {
		return getHostname() + ":" + getPort();
	}
	
	/**
	 * @return The user name and password valid for a kdb connection (i.e. username:password) 
	 */
	public String getUserAndPassword() {
		if(Strings.isNullOrEmpty(getUsername()))
			return "";
		
		if(Strings.isNullOrEmpty(getPassword()))
			return getUsername();
		
		return getUsername() + ":" + getPassword();
	}

	@Override
	public boolean equals(Object obj) {
		if(obj == null)
			return false;
		
		if(! (obj instanceof KdbProcess))
			return false;

		boolean superEquals = super.equals(obj);
		KdbProcess kObj = (KdbProcess) obj;
		
		return superEquals &&
					((this.username == null) ? kObj.username == null : this.username.equals(kObj.username)) &&
					((this.password == null) ? kObj.password == null : this.password.equals(kObj.password));
	}
	
	@Override
	public int hashCode() {
		return super.hashCode() +
				((this.username == null) ? 0 : this.username.hashCode()) +
				((this.password == null) ? 0 : this.password.hashCode());
	}
}
