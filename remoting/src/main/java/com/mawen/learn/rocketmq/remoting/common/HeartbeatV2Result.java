package com.mawen.learn.rocketmq.remoting.common;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public class HeartbeatV2Result {

	private int version = 0;

	private boolean isSubChange;

	private boolean isSupportV2;

	public HeartbeatV2Result(int version, boolean isSubChange, boolean isSupportV2) {
		this.version = version;
		this.isSubChange = isSubChange;
		this.isSupportV2 = isSupportV2;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public boolean isSubChange() {
		return isSubChange;
	}

	public void setSubChange(boolean subChange) {
		isSubChange = subChange;
	}

	public boolean isSupportV2() {
		return isSupportV2;
	}

	public void setSupportV2(boolean supportV2) {
		isSupportV2 = supportV2;
	}
}
