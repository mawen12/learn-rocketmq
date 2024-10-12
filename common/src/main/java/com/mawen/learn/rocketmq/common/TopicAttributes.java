package com.mawen.learn.rocketmq.common;

import java.util.HashMap;
import java.util.Map;

import com.mawen.learn.rocketmq.common.attribute.Attribute;
import com.mawen.learn.rocketmq.common.attribute.EnumAttribute;
import com.mawen.learn.rocketmq.common.attribute.TopicMessageType;

import static com.google.common.collect.Sets.*;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class TopicAttributes {

	public static final EnumAttribute QUEUE_TYPE_ATTRIBUTE = new EnumAttribute(
			"queue.type",
			false,
			newHashSet("BatchCQ", "SimpleCQ"),
			"SimpleCQ");

	public static final EnumAttribute CLEANUP_POLICY_ATTRIBUTE = new EnumAttribute(
			"cleanup.policy",
			false,
			newHashSet("DELETE", "COMPACTION"),
			"DELETE"
	);

	public static final EnumAttribute TOPIC_MESSAGE_TYPE_ATTRIBUTE = new EnumAttribute(
			"message.type",
			true,
			TopicMessageType.topicMessageTypeSet(),
			TopicMessageType.NORMAL.getValue()
	);

	public static final Map<String, Attribute> ALL;

	static {
		ALL = new HashMap<>();
		ALL.put(QUEUE_TYPE_ATTRIBUTE.getName(), QUEUE_TYPE_ATTRIBUTE);
		ALL.put(CLEANUP_POLICY_ATTRIBUTE.getName(), CLEANUP_POLICY_ATTRIBUTE);
		ALL.put(TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName(), TOPIC_MESSAGE_TYPE_ATTRIBUTE);
	}
}
