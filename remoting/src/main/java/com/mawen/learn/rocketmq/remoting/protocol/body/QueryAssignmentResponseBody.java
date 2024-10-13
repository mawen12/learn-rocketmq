package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.Set;

import com.mawen.learn.rocketmq.common.message.MessageQueueAssignment;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class QueryAssignmentResponseBody extends RemotingSerializable {

	private Set<MessageQueueAssignment> messageQueueAssignments;

	public Set<MessageQueueAssignment> getMessageQueueAssignments() {
		return messageQueueAssignments;
	}

	public void setMessageQueueAssignments(Set<MessageQueueAssignment> messageQueueAssignments) {
		this.messageQueueAssignments = messageQueueAssignments;
	}
}
