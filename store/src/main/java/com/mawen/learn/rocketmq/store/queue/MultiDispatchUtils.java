package com.mawen.learn.rocketmq.store.queue;

import java.util.Map;

import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.topic.TopicValidator;
import com.mawen.learn.rocketmq.store.DispatchRequest;
import com.mawen.learn.rocketmq.store.config.MessageStoreConfig;
import org.apache.commons.lang3.StringUtils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/16
 */
public class MultiDispatchUtils {

	public static String lmqQueueKey(String queueName) {
		StringBuilder sb = new StringBuilder();
		sb.append(queueName);
		sb.append('-');
		sb.append(0);
		return sb.toString();
	}

	public static boolean isNeedHandleMultiDispatch(MessageStoreConfig messageStoreConfig, String topic) {
		return messageStoreConfig.isEnableMultiDispatch()
				&& !topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)
				&& !topic.startsWith(TopicValidator.SYSTEM_TOPIC_PREFIX)
				&& !topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC);
	}

	public static boolean checkMultiDispatchQueue(MessageStoreConfig messageStoreConfig, DispatchRequest dispatchRequest) {
		if (!isNeedHandleMultiDispatch(messageStoreConfig, dispatchRequest.getTopic())) {
			return false;
		}

		Map<String, String> prop = dispatchRequest.getPropertiesMap();
		if (prop == null || prop.isEmpty()) {
			return false;
		}

		String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
		String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
		return StringUtils.isNotBlank(multiDispatchQueue) && StringUtils.isNotBlank(multiQueueOffset);
	}
}
