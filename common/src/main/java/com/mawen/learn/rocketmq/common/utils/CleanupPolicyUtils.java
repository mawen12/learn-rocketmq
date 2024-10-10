package com.mawen.learn.rocketmq.common.utils;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.mawen.learn.rocketmq.common.TopicAttributes;
import com.mawen.learn.rocketmq.common.TopicConfig;
import com.mawen.learn.rocketmq.common.attribute.CleanupPolicy;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class CleanupPolicyUtils {

	public static boolean isCompaction(Optional<TopicConfig> topicConfig) {
		return Objects.equals(CleanupPolicy.COMPACTION, getDeletePolicy(topicConfig));
	}

	public static CleanupPolicy getDeletePolicy(Optional<TopicConfig> topicConfig) {
		if (!topicConfig.isPresent()) {
			return CleanupPolicy.valueOf(TopicAttributes.CLEANUP_POLICY_ATTRIBUTE.getDefaultValue());
		}

		String attributeName = TopicAttributes.CLEANUP_POLICY_ATTRIBUTE.getName();

		Map<String, String> attributes = topicConfig.get().getAttributes();
		if (attributes == null || attributes.size() == 0) {
			return CleanupPolicy.valueOf(TopicAttributes.CLEANUP_POLICY_ATTRIBUTE.getDefaultValue());
		}

		if (attributes.containsKey(attributeName)) {
			return CleanupPolicy.valueOf(attributes.get(attributeName));
		}
		else {
			return CleanupPolicy.valueOf(TopicAttributes.CLEANUP_POLICY_ATTRIBUTE.getDefaultValue());
		}
	}
}
