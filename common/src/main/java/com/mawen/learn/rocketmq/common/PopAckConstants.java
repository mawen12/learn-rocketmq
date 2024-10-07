package com.mawen.learn.rocketmq.common;

import com.mawen.learn.rocketmq.common.topic.TopicValidator;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/7
 */
public class PopAckConstants {

	public static long ackTimeInterval = 1000;
	public static final long SECOND = 1000;

	public static long ackTime = 5000;
	public static int retryQueueNum = 1;

	public static final String REVIVE_GROUP = MixAll.CID_RMQ_SYS_PREFIX + "REVIVE_GROUP";
	public static final String LOCAL_HOST = "127.0.0.1";
	public static final String REVIVE_TOPIC = TopicValidator.SYSTEM_TOPIC_PREFIX + "REVIVE_LOG_";
	public static final String CK_TAG = "ck";
	public static final String ACK_TAG = "ack";
	public static final String BATCH_ACK_TAG = "bAck";
	public static final String SPLIT = "@";

	public static String buildClusterReviveTopic(String clusterName) {
		return PopAckConstants.REVIVE_TOPIC + clusterName;
	}

	public static boolean isStartWithRevivePrefix(String topicName) {
		return topicName != null && topicName.startsWith(REVIVE_TOPIC);
	}
}
