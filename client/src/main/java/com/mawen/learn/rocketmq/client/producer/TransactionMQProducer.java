package com.mawen.learn.rocketmq.client.producer;

import java.util.List;
import java.util.concurrent.ExecutorService;

import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.exception.RemotingException;
import com.mawen.learn.rocketmq.remoting.protocol.NamespaceUtil;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
@Getter
@Setter
@NoArgsConstructor
public class TransactionMQProducer extends DefaultMQProducer {

	private TransactionCheckListener transactionCheckListener;
	private int checkThreadPoolMinSize = 1;
	private int checkThreadPoolMaxSize = 1;
	private int checkRequestHoldMax = 2000;

	private ExecutorService executorService;

	private TransactionListener transactionListener;

	public TransactionMQProducer(final String producerGroup) {
		super(producerGroup);
	}

	public TransactionMQProducer(final String producerGroup, final List<String> topics) {
		super(producerGroup, null, topics);
	}

	public TransactionMQProducer(final String producerGroup, final RPCHook rpcHook) {
		super(producerGroup, rpcHook, null);
	}

	public TransactionMQProducer(final String producerGroup, final RPCHook rpcHook, final List<String> topics) {
		super(producerGroup, rpcHook, topics);
	}

	public TransactionMQProducer(final String producerGroup, final RPCHook rpcHook, boolean enableMsgTrace, final String customizedTraceTopic) {
		super(producerGroup, rpcHook, enableMsgTrace, customizedTraceTopic);
	}

	public TransactionMQProducer(final String namespace, final String producerGroup) {
		super(namespace, producerGroup);
	}

	public TransactionMQProducer(final String namespace, final String producerGroup, final RPCHook rpcHook, boolean enableMsgTrace, final String customizedTraceTopic) {
		super(namespace, producerGroup, rpcHook, enableMsgTrace, customizedTraceTopic);
	}

	@Override
	public void start() throws MQClientException {
		this.defaultMQProducerImpl.initTransactionEnv();
		super.start();
	}

	@Override
	public void shutdown() {
		super.shutdown();
		this.defaultMQProducerImpl.destroyTransactionEnv();
	}

	@Override
	public TransactionResult sendMessageInTransaction(Message msg, Object arg) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		if (this.transactionListener == null) {
			throw new MQClientException("TransactionListener is null", null);
		}

		msg.setTopic(NamespaceUtil.wrapNamespace(this.getNamespace(), msg.getTopic()));
		return this.defaultMQProducerImpl.sendMessageInTransaction(msg, null, arg);
	}
}
