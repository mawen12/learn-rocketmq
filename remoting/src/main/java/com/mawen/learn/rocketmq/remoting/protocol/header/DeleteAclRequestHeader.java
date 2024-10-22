package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/22
 */
@Getter
@Setter
@NoArgsConstructor
@RocketMQAction(value = RequestCode.AUTH_DELETE_ACL, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class DeleteAclRequestHeader implements CommandCustomHeader {

	private String subject;

	private String policyType;

	private String resource;

	public DeleteAclRequestHeader(String resource, String subject) {
		this.resource = resource;
		this.subject = subject;
	}

	@Override
	public void checkFields() throws RemotingCommandException {

	}
}
