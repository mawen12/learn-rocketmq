package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class MessageRequestModeSerializeWrapper extends RemotingSerializable {

	private ConcurrentMap<String, ConcurrentMap<String, SetMessageRequestModeRequestBody>> messageRequestBody = new ConcurrentHashMap<>();

	public ConcurrentMap<String, ConcurrentMap<String,SetMessageRequestModeRequestBody>> getMessageRequestBody() {
		return messageRequestBody;
	}

	public void setMessageRequestBody(ConcurrentMap<String, ConcurrentMap<String,SetMessageRequestModeRequestBody>> messageRequestBody) {
		this.messageRequestBody = messageRequestBody;
	}
}
