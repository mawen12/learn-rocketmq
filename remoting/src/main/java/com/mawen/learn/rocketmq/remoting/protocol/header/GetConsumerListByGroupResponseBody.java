package com.mawen.learn.rocketmq.remoting.protocol.header;

import java.util.List;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class GetConsumerListByGroupResponseBody extends RemotingSerializable {

	private List<String> consumerIdList;

	public List<String> getConsumerIdList() {
		return consumerIdList;
	}

	public void setConsumerIdList(List<String> consumerIdList) {
		this.consumerIdList = consumerIdList;
	}
}
