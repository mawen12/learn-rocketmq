package com.mawen.learn.rocketmq.common.utils;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.mawen.learn.rocketmq.common.TopicAttributes;
import com.mawen.learn.rocketmq.common.TopicConfig;
import com.mawen.learn.rocketmq.common.attribute.CQType;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class QueueTypeUtils {

	public static boolean isBatchCq(Optional<TopicConfig> topicConfig) {
		return Objects.equals(CQType.BatchCQ, getCQType(topicConfig));
	}

	public static CQType getCQType(Optional<TopicConfig> topicConfig) {
		if (!topicConfig.isPresent()) {
			return CQType.valueOf(TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getDefaultValue());
		}

		String attributeName = TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getName();

		Map<String, String> attributes = topicConfig.get().getAttributes();
		if (attributes == null || attributes.size() == 0) {
			return CQType.valueOf(TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getDefaultValue());
		}

		if (attributes.containsKey(attributeName)) {
			return CQType.valueOf(attributes.get(attributeName));
		}
		else {
			return CQType.valueOf(TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getDefaultValue());
		}
	}
}
