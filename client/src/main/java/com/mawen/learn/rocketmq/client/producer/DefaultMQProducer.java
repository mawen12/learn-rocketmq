package com.mawen.learn.rocketmq.client.producer;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import com.mawen.learn.rocketmq.client.ClientConfig;
import com.mawen.learn.rocketmq.client.trace.TraceDispatcher;
import com.mawen.learn.rocketmq.common.topic.TopicValidator;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public class DefaultMQProducer extends ClientConfig implements MQProducer{

	private final Logger log = LoggerFactory.getLogger(DefaultMQProducer.class);

	protected final transient DefaultMQProducerImpl defaultMQProducerImpl;

	private final Set<Integer> retryResponseCodes = new CopyOnWriteArraySet<>(Arrays.asList(
			ResponseCode.TOPIC_NOT_EXIST,
			ResponseCode.SERVICE_NOT_AVAILABLE,
			ResponseCode.SYSTEM_ERROR,
			ResponseCode.SYSTEM_BUSY,
			ResponseCode.NO_PERMISSION,
			ResponseCode.NO_BUYER_ID,
			ResponseCode.NOT_IN_CURRENT_UNIT
			));

	private String producerGroup;

	private List<String> topics;

	private String createTopicKey = TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC;

	private volatile int defaultTopicQueueNums = 4;

	private int sendMsgTimeout = 3000;

	private int compressMsgBodyOverHowmuch = 4 * 1024;

	private int retryTimesWhenSendFailed = 2;

	private int retryTimesWhenSendAsyncFailed = 2;

	private boolean retryAnotherBrokerWhenNotStoreOK = false;

	private int maxMessageSize = 4 * 1024 * 1024;

	private TraceDispatcher traceDispatcher;

	private boolean authBatch = false;

	private ProduceAccumulator produceAccumulator;

	private boolean enableBackpressureForAsyncMode = false;

	private int backPressureForAsyncSendNum = 10000;

	private int backPressureForAsyncSendSize = 100 * 1024 * 1024;

	private RPCHook rpcHook;



}
