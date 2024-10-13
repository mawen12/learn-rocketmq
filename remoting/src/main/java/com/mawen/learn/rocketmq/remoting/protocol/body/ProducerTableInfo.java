package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.List;
import java.util.Map;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class ProducerTableInfo extends RemotingSerializable {

	private Map<String, List<ProducerInfo>> data;

	public ProducerTableInfo(Map<String, List<ProducerInfo>> data) {
		this.data = data;
	}

	public Map<String, List<ProducerInfo>> getData() {
		return data;
	}

	public void setData(Map<String, List<ProducerInfo>> data) {
		this.data = data;
	}
}
