package com.mawen.learn.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import com.mawen.learn.rocketmq.client.hook.EndTransactionHook;
import com.mawen.learn.rocketmq.client.hook.SendMessageHook;
import com.mawen.learn.rocketmq.client.producer.DefaultMQProducer;
import com.mawen.learn.rocketmq.client.producer.TransactionCheckListener;
import com.mawen.learn.rocketmq.client.producer.TransactionListener;
import com.mawen.learn.rocketmq.common.ServiceState;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/18
 */
public class DefaultMQProducerImpl implements MQProducerInner{

	private final Logger log = LoggerFactory.getLogger(DefaultMQProducerImpl.class);

	private final Random random = new Random();
	private final DefaultMQProducer defaultMQProducer;
	private final ConcurrentMap<String, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<>();
	private final List<SendMessageHook> sendMessageHookList = new ArrayList<>();
	private final List<EndTransactionHook> endTransactionHookList = new ArrayList<>();
	private final RPCHook rpcHook;
	private final BlockingQueue<Runnable> asyncSenderThreadPoolQueue;
	private final ExecutorService defaultAsyncSenderExecutor;
	private BlockingQueue<Runnable> checkRequestQueue;
	private ExecutorService checkExecutor;
	private ServiceState serviceState = ServiceState.CREATE_JUST;
	private MQClientInstance mqClientFactory;
	private List<CheckForbiddenHook> checkForbiddenHookList = new ArrayList<>();
	private MQFaultStrategy mqFaultStrategy;
	private ExecutorService asyncSenderExecutor;

	@Override
	public Set<String> getPublishTopicList() {
		return Set.of();
	}

	@Override
	public boolean isPublishTopicNeedUpdate(String topic) {
		return false;
	}

	@Override
	public TransactionCheckListener checkListener() {
		return null;
	}

	@Override
	public TransactionListener getCheckListener() {
		return null;
	}

	@Override
	public void checkTransactionState(String addr, MessageExt msg, CheckTransactionStateRequestHeader checkRequestHeader) {

	}

	@Override
	public boolean isUnitMode() {
		return false;
	}
}
