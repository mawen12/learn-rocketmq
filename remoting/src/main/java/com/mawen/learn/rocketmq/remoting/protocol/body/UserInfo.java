package com.mawen.learn.rocketmq.remoting.protocol.body;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class UserInfo {

	private String username;

	private String password;

	private String userType;

	private String userStatus;

	public static UserInfo of(String username, String password, String userType) {
		UserInfo userInfo = new UserInfo();
		userInfo.setUsername(username);
		userInfo.setPassword(password);
		userInfo.setUserType(userType);
		return userInfo;
	}

	public static UserInfo of(String username, String password, String userType, String userStatus) {
		UserInfo userInfo = new UserInfo();
		userInfo.setUsername(username);
		userInfo.setPassword(password);
		userInfo.setUserType(userType);
		userInfo.setUserStatus(userStatus);
		return userInfo;
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

	public String getUserType() {
		return userType;
	}

	public void setUserType(String userType) {
		this.userType = userType;
	}

	public String getUserStatus() {
		return userStatus;
	}

	public void setUserStatus(String userStatus) {
		this.userStatus = userStatus;
	}
}
