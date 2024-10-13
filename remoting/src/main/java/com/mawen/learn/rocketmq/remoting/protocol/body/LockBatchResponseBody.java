package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.HashSet;
import java.util.Set;

import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class LockBatchResponseBody extends RemotingSerializable {

	private Set<MessageQueue> lockOKMQSet = new HashSet<>();

	public Set<MessageQueue> getLockOKMQSet() {
		return lockOKMQSet;
	}

	public void setLockOKMQSet(Set<MessageQueue> lockOKMQSet) {
		this.lockOKMQSet = lockOKMQSet;
	}
}
