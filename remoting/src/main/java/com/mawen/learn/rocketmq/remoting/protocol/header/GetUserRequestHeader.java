package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.AUTH_GET_USER, resource = ResourceType.CLUSTER, action = Action.GET)
public class GetUserRequestHeader implements CommandCustomHeader {

	private String username;

	public GetUserRequestHeader() {
	}

	public GetUserRequestHeader(String username) {
		this.username = username;
	}

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}
}
