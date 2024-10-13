package com.mawen.learn.rocketmq.remoting.protocol.heartbeat;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class ProducerData {

	private String groupName;

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	@Override
	public String toString() {
		return "ProducerData{" +
		       "groupName='" + groupName + '\'' +
		       '}';
	}
}
