package com.mawen.learn.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.Map;

import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import lombok.AllArgsConstructor;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/16
 */
@AllArgsConstructor
public class DefaultMessageFilter implements MessageFilter {

	private SubscriptionData subscriptionData;

	@Override
	public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqUnit cqUnit) {
		if (tagsCode == null || subscriptionData == null) {
			return true;
		}

		if (subscriptionData.isClassFilterMode()) {
			return true;
		}

		return subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)
				|| subscriptionData.getCodeSet().contains(tagsCode.intValue());
	}

	@Override
	public boolean isMatchedByCommitLog(ByteBuffer buffer, Map<String, String> properties) {
		return true;
	}
}
