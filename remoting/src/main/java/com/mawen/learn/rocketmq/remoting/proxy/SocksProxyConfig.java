package com.mawen.learn.rocketmq.remoting.proxy;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class SocksProxyConfig {

	private String addr;

	private String username;

	private String password;

	public SocksProxyConfig() {
	}

	public SocksProxyConfig(String addr) {
		this.addr = addr;
	}

	public SocksProxyConfig(String addr, String username, String password) {
		this.addr = addr;
		this.username = username;
		this.password = password;
	}

	public String getAddr() {
		return addr;
	}

	public void setAddr(String addr) {
		this.addr = addr;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	@Override
	public String toString() {
		return "SocksProxyConfig{" +
				"addr='" + addr + '\'' +
				", username='" + username + '\'' +
				", password='" + password + '\'' +
				'}';
	}
}
