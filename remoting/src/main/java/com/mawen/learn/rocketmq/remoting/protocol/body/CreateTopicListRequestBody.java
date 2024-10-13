package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.List;

import com.mawen.learn.rocketmq.common.TopicConfig;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class CreateTopicListRequestBody extends RemotingSerializable {

	@CFNotNull
	private List<TopicConfig> topicConfigList;

	public CreateTopicListRequestBody() {
	}

	public CreateTopicListRequestBody(List<TopicConfig> topicConfigList) {
		this.topicConfigList = topicConfigList;
	}

	public List<TopicConfig> getTopicConfigList() {
		return topicConfigList;
	}

	public void setTopicConfigList(List<TopicConfig> topicConfigList) {
		this.topicConfigList = topicConfigList;
	}
}
