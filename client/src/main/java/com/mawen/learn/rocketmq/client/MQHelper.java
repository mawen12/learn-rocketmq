package com.mawen.learn.rocketmq.client;

import java.util.Set;
import java.util.TreeSet;

import com.mawen.learn.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
public class MQHelper {

	private static final Logger log = LoggerFactory.getLogger(MQHelper.class);

	public static void resetOffsetByTimestamp(final MessageModel messageModel, final String consumerGroup, final String topic, final long timestamp) throws MQClientException {
		resetOffsetByTimestamp(messageModel, "DEFAULT", consumerGroup, topic, timestamp);
	}

	public static void resetOffsetByTimestamp(final MessageModel messageModel, final String instanceName, final String consumerGroup, final String topic, final long timestamp) throws MQClientException {
		DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(consumerGroup);
		consumer.setInstanceName(instanceName);
		consumer.setMessageModel(messageModel);
		consumer.start();

		Set<MessageQueue> mqs =null;
		try {
			mqs = consumer.fetchSubscribeMessageQueue(topic);
			if (CollectionUtils.isNotEmpty(mqs)) {
				TreeSet<MessageQueue> mqsNew = new TreeSet<>(mqs);
				for (MessageQueue mq : mqsNew) {
					long offset = consumer.searchOffset(mq, timestamp);
					if (offset >= 0) {
						consumer.updateConsumeOffset(mq, offset);
						log.info("resetOffsetByTimestamp updateConsumeOffset success, {} {} {}", consumerGroup, offset, mq);
					}
				}
			}
		}
		catch (Exception e) {
			log.warn("resetOffsetByTimestamp Exception", e);
			throw e;
		}
		finally {
			if (mqs != null) {
				consumer.getDefaultMQPullConsumerImpl().getOffsetStore().persistAll(mqs);
			}
			consumer.shutdown();
		}
	}
}
