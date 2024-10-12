package com.mawen.learn.rocketmq.remoting.protocol.header;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mawen.learn.rocketmq.common.KeyBuilder;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageConst;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class ExtraInfoUtil {

	private static final String NORMAL_TOPIC = "0";
	private static final String RETRY_TOPIC = "1";
	private static final String RETRY_TOPIC_V2 = "2";
	private static final String QUEUE_OFFSET = "qo";

	public static String[] split(String extraInfo) {
		if (extraInfo == null) {
			throw new IllegalArgumentException("split extraInfo is null");
		}
		return extraInfo.split(MessageConst.KEY_SEPARATOR);
	}

	public static Long getCkQueueOffset(String[] extraInfoStrs) {
		assertExtraInfoStrs(extraInfoStrs, 1, "getCkQueueOffset");
		return Long.valueOf(extraInfoStrs[0]);
	}

	public static Long getPopTime(String[] extraInfoStrs) {
		assertExtraInfoStrs(extraInfoStrs, 2, "getPopTime");
		return Long.valueOf(extraInfoStrs[1]);
	}

	public static Long getInvisibleTime(String[] extraInfoStrs) {
		assertExtraInfoStrs(extraInfoStrs, 3, "getInvisibleTime");
		return Long.valueOf(extraInfoStrs[2]);
	}

	public static int getReviveQid(String[] extraInfoStrs) {
		assertExtraInfoStrs(extraInfoStrs, 4, "getReviveQid");
		return Integer.parseInt(extraInfoStrs[3]);
	}

	public static String getRealTopic(String[] extraInfoStrs, String topic, String cid) {
		assertExtraInfoStrs(extraInfoStrs, 5, "getRealTopic");

		if (RETRY_TOPIC.equals(extraInfoStrs[4])) {
			return KeyBuilder.buildPopRetryTopicV1(topic, cid);
		}
		else if (RETRY_TOPIC_V2.equals(extraInfoStrs[4])) {
			return KeyBuilder.buildPopRetryTopicV2(topic, cid);
		}
		else {
			return topic;
		}
	}

	public static String getRealTopic(String topic, String cid, String retry) {
		if (NORMAL_TOPIC.equals(retry)) {
			return topic;
		}
		else if (RETRY_TOPIC.equals(retry)) {
			return KeyBuilder.buildPopRetryTopicV1(topic, cid);
		}
		else if (RETRY_TOPIC_V2.equals(retry)) {
			return KeyBuilder.buildPopRetryTopicV2(topic, cid);
		}

		throw new IllegalArgumentException("getRetry fail, format is wrong");
	}

	public static String getRetry(String[] extraInfoStrs) {
		assertExtraInfoStrs(extraInfoStrs, 5, "getRetry");
		return extraInfoStrs[4];
	}

	public static String getBrokerName(String[] extraInfoStrs) {
		assertExtraInfoStrs(extraInfoStrs, 6, "getBrokerName");
		return extraInfoStrs[5];
	}

	public static int getQueueId(String[] extraInfoStrs) {
		assertExtraInfoStrs(extraInfoStrs, 7, "getQueueId");
		return Integer.parseInt(extraInfoStrs[6]);
	}

	public static long getQueueOffset(String[] extraInfoStrs) {
		assertExtraInfoStrs(extraInfoStrs, 8, "getQueueOffset");
		return Long.parseLong(extraInfoStrs[7]);
	}

	public static String buildExtraInfo(long ckQueueOffset, long popTime, long invisibleTime, int reviveQid,
			String topic, String brokerName, int queueId) {
		String t = getRetry(topic);

		return ckQueueOffset
				+ MessageConst.KEY_SEPARATOR + popTime
				+ MessageConst.KEY_SEPARATOR + invisibleTime
				+ MessageConst.KEY_SEPARATOR + reviveQid
				+ MessageConst.KEY_SEPARATOR + t
				+ MessageConst.KEY_SEPARATOR + brokerName
				+ MessageConst.KEY_SEPARATOR + queueId;
	}

	public static String buildExtraInfo(long ckQueueOffset, long popTime, long invisibleTime, int reviveQid,
			String topic, String brokerName, int queueId, long msgQueueOffset) {
		String t = getRetry(topic);

		return ckQueueOffset
				+ MessageConst.KEY_SEPARATOR + popTime
				+ MessageConst.KEY_SEPARATOR + invisibleTime
				+ MessageConst.KEY_SEPARATOR + reviveQid
				+ MessageConst.KEY_SEPARATOR + t
				+ MessageConst.KEY_SEPARATOR + brokerName
				+ MessageConst.KEY_SEPARATOR + queueId
				+ MessageConst.KEY_SEPARATOR + msgQueueOffset;
	}

	public static void buildStartOffsetInfo(StringBuilder stringBuilder, String topic, int queueId, long startOffset) {
		if (stringBuilder == null) {
			throw new IllegalArgumentException("stringBuilder cannot be null");
		}

		if (stringBuilder.length() > 0) {
			stringBuilder.append(";");
		}

		stringBuilder.append(getRetry(topic)).append(MessageConst.KEY_SEPARATOR)
				.append(queueId).append(MessageConst.KEY_SEPARATOR)
				.append(startOffset);
	}

	public static void buildQueueIdOrderCountInfo(StringBuilder stringBuilder, String topic, int queueId, int orderCount) {
		if (stringBuilder == null) {
			throw new IllegalArgumentException("stringBuilder cannot be null");
		}

		if (stringBuilder.length() > 0) {
			stringBuilder.append(";");
		}

		stringBuilder.append(getRetry(topic)).append(MessageConst.KEY_SEPARATOR)
				.append(queueId).append(MessageConst.KEY_SEPARATOR)
				.append(orderCount);
	}

	public static void buildQueueOffsetOrderCountInfo(StringBuilder stringBuilder, String topic, long queueId, long queueOffset, int orderCount) {
		if (stringBuilder == null) {
			throw new IllegalArgumentException("stringBuilder cannot be null");
		}

		if (stringBuilder.length() > 0) {
			stringBuilder.append(";");
		}

		stringBuilder.append(getRetry(topic)).append(MessageConst.KEY_SEPARATOR)
				.append(getQueueOffsetKeyValueKey(queueId, queueOffset)).append(MessageConst.KEY_SEPARATOR)
				.append(orderCount);
	}

	public static void buildMsgOffsetInfo(StringBuilder stringBuilder, String topic, int queueId, List<Long> msgOffsets) {
		if (stringBuilder == null) {
			throw new IllegalArgumentException("stringBuilder cannot be null");
		}

		if (stringBuilder.length() > 0) {
			stringBuilder.append(";");
		}

		stringBuilder.append(getRetry(topic)).append(MessageConst.KEY_SEPARATOR)
				.append(queueId).append(MessageConst.KEY_SEPARATOR);

		for (int i = 0; i < msgOffsets.size(); i++) {
			stringBuilder.append(msgOffsets.get(i));
			if (i < msgOffsets.size() - 1) {
				stringBuilder.append(",");
			}
		}
	}

	public static Map<String, List<Long>> parseMsgOffsetInfo(String msgOffsetInfo) {
		if (msgOffsetInfo == null || msgOffsetInfo.length() == 0) {
			return null;
		}

		Map<String, List<Long>> msgOffsetMap = new HashMap<>(4);
		String[] array;
		if (msgOffsetInfo.indexOf(";") < 0) {
			array = new String[]{msgOffsetInfo};
		}
		else {
			array = msgOffsetInfo.split(";");
		}

		for (String one : array) {
			String[] split = one.split(MessageConst.KEY_SEPARATOR);
			if (split.length != 3) {
				throw new IllegalArgumentException("parse msgOffsetInfo error, " + msgOffsetInfo);
			}

			String key = split[0] + "@" + split[1];
			if (msgOffsetMap.containsKey(key)) {
				throw new IllegalArgumentException("parse msgOffsetInfo error, " + msgOffsetInfo);
			}

			msgOffsetMap.put(key, new ArrayList<>(8));
			String[] msgOffsets = split[2].split(",");
			for (String msgOffset : msgOffsets) {
				msgOffsetMap.get(key).add(Long.valueOf(msgOffset));
			}
		}

		return msgOffsetMap;
	}

	public static Map<String, Long> parseStartOffsetInfo(String startOffsetInfo) {
		if (startOffsetInfo == null || startOffsetInfo.length() == 0) {
			return null;
		}

		Map<String, Long> startOffsetMap = new HashMap<>(4);
		String[] array;
		if (startOffsetInfo.indexOf(";") < 0) {
			array = new String[]{startOffsetInfo};
		}
		else {
			array = startOffsetInfo.split(";");
		}

		for (String one : array) {
			String[] split = one.split(MessageConst.KEY_SEPARATOR);
			if (split.length != 3) {
				throw new IllegalArgumentException("parse startOffsetInfo error, " + startOffsetInfo);
			}

			String key = split[0] + "@" + split[1];
			if (startOffsetMap.containsKey(key)) {
				throw new IllegalArgumentException("parse startOffsetInfo error, duplicate, " + startOffsetInfo);
			}

			startOffsetMap.put(key, Long.parseLong(split[2]));
		}
		return startOffsetMap;
	}

	public static Map<String, Integer> parseOrderCountInfo(String orderCountInfo) {
		if (orderCountInfo == null || orderCountInfo.length() == 0) {
			return null;
		}

		Map<String, Integer> startOffsetMap = new HashMap<>(4);
		String[] array;
		if (orderCountInfo.indexOf(";") < 0) {
			array = new String[]{orderCountInfo};
		}
		else {
			array = orderCountInfo.split(";");
		}

		for (String one : array) {
			String[] split = one.split(MessageConst.KEY_SEPARATOR);
			if (split.length != 3) {
				throw new IllegalArgumentException("parse orderCountInfo error, " + orderCountInfo);
			}

			String key = split[0] + "@" + split[1];
			if (startOffsetMap.containsKey(key)) {
				throw new IllegalArgumentException("parse orderCountInfo error, duplicate, " + orderCountInfo);
			}

			startOffsetMap.put(key, Integer.valueOf(split[2]));
		}
		return startOffsetMap;
	}

	public static String getStartOffsetInfoMapKey(String topic, long key) {
		return getRetry(topic) + "@" + key;
	}

	public static String getStartOffsetInfoMapKey(String topic, String popCk, long key) {
		return getRetry(topic, popCk) + "@" + key;
	}

	public static String getQueueOffsetMapKey(String topic, long queueId, long queueOffset) {
		return getRetry(topic) + "@" + getQueueOffsetKeyValueKey(queueId, queueOffset);
	}

	public static String getQueueOffsetKeyValueKey(long queueId, long queueOffset) {
		return QUEUE_OFFSET + queueId + "%" + queueOffset;
	}

	public static boolean isOrder(String[] extraInfoStrs) {
		return ExtraInfoUtil.getReviveQid(extraInfoStrs) == KeyBuilder.POP_ORDER_REVIVE_QUEUE;
	}

	private static String getRetry(String topic) {
		String t = NORMAL_TOPIC;
		if (KeyBuilder.isPopRetryTopicV2(t)) {
			t = RETRY_TOPIC_V2;
		}
		else if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
			t = RETRY_TOPIC;
		}
		return t;
	}

	private static String getRetry(String topic, String popCk) {
		if (popCk != null) {
			return getRetry(split(popCk));
		}
		return getRetry(topic);
	}

	private static void assertExtraInfoStrs(String[] extraInfoStrs, int minimumLength, String prefix) {
		if (extraInfoStrs == null || extraInfoStrs.length < minimumLength) {
			throw new IllegalArgumentException(prefix + " fail, extraInfoStrs length " + (extraInfoStrs == null ? 0 : extraInfoStrs.length));
		}
	}
}
