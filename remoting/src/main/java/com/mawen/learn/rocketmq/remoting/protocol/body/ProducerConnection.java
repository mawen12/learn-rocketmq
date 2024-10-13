package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.HashSet;
import java.util.Set;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class ProducerConnection extends RemotingSerializable {

	private Set<Connection> connectionSet = new HashSet<>();

	public Set<Connection> getConnectionSet() {
		return connectionSet;
	}

	public void setConnectionSet(Set<Connection> connectionSet) {
		this.connectionSet = connectionSet;
	}
}