package com.mawen.learn.rocketmq.remoting.protocol.heartbeat;

import java.util.HashSet;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
@Getter
@Setter
@ToString
public class HeartbeatData extends RemotingSerializable {

	private String clientID;

	private Set<ProducerData> producerDataSet = new HashSet<>();

	private Set<ConsumerData> consumerDataSet = new HashSet<>();

	private int heartbeatFingerprint = 0;

	private boolean isWithoutSub = false;

	public int computeHeartbeatFingerprint() {
		HeartbeatData heartbeatDataCopy = JSON.parseObject(JSON.toJSONString(this), HeartbeatData.class);
		for (ConsumerData consumerData : heartbeatDataCopy.getConsumerDataSet()) {
			for (SubscriptionData subscriptionData : consumerData.getSubscriptionDataSet()) {
				subscriptionData.setSubVersion(0L);
			}
		}
		heartbeatDataCopy.setWithoutSub(false);
		heartbeatDataCopy.setHeartbeatFingerprint(0);
		heartbeatDataCopy.setClientID("");
		return JSON.toJSONString(heartbeatDataCopy).hashCode();
	}
}
