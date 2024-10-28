package com.mawen.learn.rocketmq.remoting.protocol.filter;

import java.util.Arrays;

import com.mawen.learn.rocketmq.common.filter.ExpressionType;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.commons.lang3.StringUtils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class FilterAPI {

	public static SubscriptionData buildSubscriptionData(String topic, String subString) throws Exception {
		SubscriptionData subscriptionData = new SubscriptionData();
		subscriptionData.setTopic(topic);
		subscriptionData.setSubString(subString);

		if (StringUtils.isEmpty(subString) || subString.equals(SubscriptionData.SUB_ALL)) {
			subscriptionData.setSubString(SubscriptionData.SUB_ALL);
			return subscriptionData;
		}

		String[] tags = subString.split("\\|\\|");
		if (tags.length > 0) {
			Arrays.stream(tags)
					.map(String::trim)
					.filter(tag -> !tag.isEmpty())
					.forEach(tag -> {
						subscriptionData.getTagsSet().add(tag);
						subscriptionData.getCodeSet().add(tag.hashCode());
					});
		}
		else {
			throw new Exception("subString split error");
		}

		return subscriptionData;
	}

	public static SubscriptionData buildSubscriptionData(String topic, String subString, String expressionType) throws Exception {
		SubscriptionData subscriptionData = buildSubscriptionData(topic, subString);
		if (StringUtils.isNotBlank(expressionType)) {
			subscriptionData.setExpressionType(expressionType);
		}
		return subscriptionData;
	}

	public static SubscriptionData build(String topic, String subString, String type) throws Exception {
		if (ExpressionType.TAG.equals(type) || type == null) {
			return buildSubscriptionData(topic, subString);
		}

		if (StringUtils.isEmpty(subString)) {
			throw new IllegalArgumentException("Expression can't be null!" + type);
		}

		SubscriptionData subscriptionData = new SubscriptionData();
		subscriptionData.setTopic(topic);
		subscriptionData.setSubString(subString);
		subscriptionData.setExpressionType(type);
		return subscriptionData;
	}
}
