package com.mawen.learn.rocketmq.client.impl.producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.client.QueryResult;
import com.mawen.learn.rocketmq.client.Validators;
import com.mawen.learn.rocketmq.client.common.ClientErrorCode;
import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.exception.RequestTimeoutException;
import com.mawen.learn.rocketmq.client.hook.CheckForbiddenContext;
import com.mawen.learn.rocketmq.client.hook.CheckForbiddenHook;
import com.mawen.learn.rocketmq.client.hook.EndTransactionContext;
import com.mawen.learn.rocketmq.client.hook.EndTransactionHook;
import com.mawen.learn.rocketmq.client.hook.SendMessageContext;
import com.mawen.learn.rocketmq.client.hook.SendMessageHook;
import com.mawen.learn.rocketmq.client.impl.CommunicationMode;
import com.mawen.learn.rocketmq.client.impl.MQClientManager;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.client.latency.MQFaultStrategy;
import com.mawen.learn.rocketmq.client.latency.ServiceDetector;
import com.mawen.learn.rocketmq.client.producer.DefaultMQProducer;
import com.mawen.learn.rocketmq.client.producer.LocalTransactionState;
import com.mawen.learn.rocketmq.client.producer.MessageQueueSelector;
import com.mawen.learn.rocketmq.client.producer.RequestCallback;
import com.mawen.learn.rocketmq.client.producer.RequestFutureHolder;
import com.mawen.learn.rocketmq.client.producer.RequestResponseFuture;
import com.mawen.learn.rocketmq.client.producer.SendCallback;
import com.mawen.learn.rocketmq.client.producer.SendResult;
import com.mawen.learn.rocketmq.client.producer.SendStatus;
import com.mawen.learn.rocketmq.client.producer.TransactionCheckListener;
import com.mawen.learn.rocketmq.client.producer.TransactionListener;
import com.mawen.learn.rocketmq.client.producer.TransactionMQProducer;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.ServiceState;
import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.compression.CompressionType;
import com.mawen.learn.rocketmq.common.compression.Compressor;
import com.mawen.learn.rocketmq.common.compression.CompressorFactory;
import com.mawen.learn.rocketmq.common.help.FAQUrl;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageAccessor;
import com.mawen.learn.rocketmq.common.message.MessageBatch;
import com.mawen.learn.rocketmq.common.message.MessageClientIDSetter;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.message.MessageDecoder;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.message.MessageType;
import com.mawen.learn.rocketmq.common.sysflag.MessageSysFlag;
import com.mawen.learn.rocketmq.common.utils.CorrelationIdUtil;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingConnectException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingSendRequestException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTimeoutException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.mawen.learn.rocketmq.remoting.protocol.NamespaceUtil;
import com.mawen.learn.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/18
 */
@Setter
@Getter
public class DefaultMQProducerImpl implements MQProducerInner {

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

	private int compressLevel = Integer.parseInt(System.getProperty(MixAll.MESSAGE_COMPRESS_LEVEL, "5"));
	private CompressionType compressType = CompressionType.of(System.getProperty(MixAll.MESSAGE_COMPRESS_TYPE, "ZLIB"));
	private final Compressor compressor = CompressorFactory.getCompressor(compressType);

	private Semaphore semaphoreAsyncSendNum;
	private Semaphore semaphoreAsyncSendSize;

	public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer) {
		this(defaultMQProducer, null);
	}

	public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer, RPCHook rpcHook) {
		this.defaultMQProducer = defaultMQProducer;
		this.rpcHook = rpcHook;

		this.asyncSenderThreadPoolQueue = new LinkedBlockingDeque<>(50000);
		this.defaultAsyncSenderExecutor = new ThreadPoolExecutor(
				Runtime.getRuntime().availableProcessors(),
				Runtime.getRuntime().availableProcessors(),
				60 * 1000,
				TimeUnit.MILLISECONDS,
				this.asyncSenderThreadPoolQueue,
				new ThreadFactoryImpl("AsyncSenderExecutor_")
		);

		if (defaultMQProducer.getBackPressureForAsyncSendNum() > 10) {
			semaphoreAsyncSendNum = new Semaphore(Math.max(defaultMQProducer.getBackPressureForAsyncSendNum(), 10), true);
		}
		else {
			semaphoreAsyncSendNum = new Semaphore(10, true);
			log.info("semaphoreAsyncSendNum can not be smaller than 10.");
		}

		if (defaultMQProducer.getBackPressureForAsyncSendSize() > 1024 * 1024) {
			semaphoreAsyncSendSize = new Semaphore(Math.max(defaultMQProducer.getBackPressureForAsyncSendSize(), 1024 * 1024), true);
		}
		else {
			semaphoreAsyncSendSize = new Semaphore(1024 * 1024, true);
			log.info("semaphoreAsyncSendSize can not be smaller that 1M.");
		}

		ServiceDetector serviceDetector = new ServiceDetector() {
			@Override
			public boolean detect(String endpoint, long timeoutMillis) {
				Optional<String> candidate = pickTopic();
				if (!candidate.isPresent()) {
					return false;
				}

				try {
					MessageQueue mq = new MessageQueue(candidate.get(), null, 0);
					mqClientFactory.getMqClientAPIImpl().getMaxOffset(endpoint, mq, timeoutMillis);
					return true;
				}
				catch (Exception e) {
					return false;
				}
			}
		};

		this.mqFaultStrategy = new MQFaultStrategy(defaultMQProducer.cloneClientConfig(), name -> this.mqClientFactory.findBrokerAddressInPublish(name), serviceDetector);
	}

	public void registerCheckForbiddenHook(CheckForbiddenHook hook) {
		this.checkForbiddenHookList.add(hook);
		log.info("register a new checkForbiddenHook. hookName={}, allHookSize={}", hook.hookName(), checkForbiddenHookList.size());
	}

	public void setSemaphoreAsyncSendNum(int num) {
		this.semaphoreAsyncSendNum = new Semaphore(num, true);
	}

	public void setSemaphoreAsyncSendSize(int size) {
		this.semaphoreAsyncSendSize = new Semaphore(size, true);
	}

	public void initTransactionEnv() {
		TransactionMQProducer producer = (TransactionMQProducer) this.defaultMQProducer;
		if (producer.getExecutorService() != null) {
			this.checkExecutor = producer.getExecutorService();
		}
		else {
			this.checkRequestQueue = new LinkedBlockingDeque<>(producer.getCheckRequestHoldMax());
			this.checkExecutor = new ThreadPoolExecutor(
					producer.getCheckThreadPoolMinSize(),
					producer.getCheckThreadPoolMaxSize(),
					60 * 1000,
					TimeUnit.MILLISECONDS,
					this.checkRequestQueue
			);
		}
	}

	public void destroyTransactionEnv() {
		if (this.checkExecutor != null) {
			this.checkExecutor.shutdown();
		}
	}

	public void registerSendMessageHook(final SendMessageHook hook) {
		this.sendMessageHookList.add(hook);
		log.info("register sendMessage Hook, {}", hook.hookName());
	}

	public void registerEndTransactionHook(final EndTransactionHook hook) {
		this.endTransactionHookList.add(hook);
		log.info("register endTransaction Hook, {}", hook.hookName());
	}

	public void start() throws MQClientException {
		this.start(true);
	}

	public void start(final boolean startFactory) throws MQClientException {
		switch (this.serviceState) {
			case CREATE_JUST:
				this.serviceState = ServiceState.START_FAILED;

				this.checkConfig();

				if (!this.defaultMQProducer.getProducerGroup().contains(MixAll.CLIENT_INNER_PRODUCER_GROUP)) {
					this.defaultMQProducer.changeInstanceNameToPID();
				}

				this.mqClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQProducer, rpcHook);

				boolean registerOK = mqClientFactory.registerProducer(this.defaultMQProducer.getProducerGroup(), this);
				if (!registerOK) {
					this.serviceState = ServiceState.CREATE_JUST;
					throw new MQClientException("The producer group[" + this.defaultMQProducer.getProducerGroup() + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL), null);
				}

				if (startFactory) {
					mqClientFactory.start();
				}

				this.initTopicRoute();

				this.mqFaultStrategy.startDetector();

				log.info("the producer [{}] start OK, sendMessageWithVIPChannel={}", this.defaultMQProducer.getProducerGroup(), this.defaultMQProducer.isSendMessageWithVIPChannel());
				this.serviceState = ServiceState.RUNNING;
				break;
			case RUNNING:
			case START_FAILED:
			case SHUTDOWN_ALREADY:
				throw new MQClientException("The producer service state not OK, maybe started once, " + this.serviceState + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
			default:
				break;
		}
	}

	public void shutdown() {
		this.shutdown(true);
	}

	public void shutdown(final boolean shutdownFactory) {
		switch (this.serviceState) {
			case CREATE_JUST:
				break;
			case RUNNING:
				this.mqClientFactory.unregisterProducer(this.defaultMQProducer.getProducerGroup());
				this.defaultAsyncSenderExecutor.shutdown();
				if (shutdownFactory) {
					this.mqClientFactory.shutdown();
				}

				this.mqFaultStrategy.shutdown();
				RequestFutureHolder.getInstance().shutdown(this);
				log.info("the producer [{}] shutdown OK", this.defaultMQProducer.getProducerGroup());
				this.serviceState = ServiceState.SHUTDOWN_ALREADY;
				break;
			case SHUTDOWN_ALREADY:
				break;
			default:
				break;
		}
	}


	@Override
	public Set<String> getPublishTopicList() {
		return new HashSet<>(this.topicPublishInfoTable.keySet());
	}

	@Override
	public boolean isPublishTopicNeedUpdate(String topic) {
		TopicPublishInfo prev = this.topicPublishInfoTable.get(topic);
		return prev == null || !prev.ok();
	}

	@Override
	public TransactionCheckListener checkListener() {
		if (this.defaultMQProducer instanceof TransactionMQProducer) {
			return ((TransactionMQProducer) defaultMQProducer).getTransactionCheckListener();
		}
		return null;
	}

	@Override
	public TransactionListener getCheckListener() {
		if (this.defaultMQProducer instanceof TransactionMQProducer) {
			return ((TransactionMQProducer) defaultMQProducer).getTransactionListener();
		}
		return null;
	}

	@Override
	public void checkTransactionState(String addr, MessageExt msg, CheckTransactionStateRequestHeader checkRequestHeader) {
		Runnable request = new Runnable() {
			private final String brokerAddr = addr;
			private final MessageExt message = msg;
			private final CheckTransactionStateRequestHeader requestHeader = checkRequestHeader;
			private final String group = DefaultMQProducerImpl.this.defaultMQProducer.getProducerGroup();

			@Override
			public void run() {
				TransactionCheckListener transactionCheckListener = DefaultMQProducerImpl.this.checkListener();
				TransactionListener transactionListener = getCheckListener();

				if (transactionCheckListener != null || transactionListener == null) {
					LocalTransactionState localTransactionState = LocalTransactionState.UNKNOWN;
					Throwable exception = null;
					try {
						if (transactionCheckListener != null) {
							localTransactionState = transactionCheckListener.checkLocalTransactionState(message);
						}
						else {
							log.debug("TransactionCheckListener is null, used new check API, producerGroup={}", group);
							localTransactionState = transactionListener.checkLockTransaction(message);
						}
					}
					catch (Throwable e) {
						log.error("Broker call checkTransactionState, but checkLocalTransactionState exception", e);
						exception = e;
					}

					this.processTransactionState(requestHeader.getTopic(), localTransactionState, group, exception);
				}
				else {
					log.warn("CheckTransactionState, pick transactionCheckListener by group[{}] failed", group);
				}
			}

			private void processTransactionState(final String topic, final LocalTransactionState localTransactionState, final String producerGroup, final Throwable exception) {
				EndTransactionRequestHeader header = new EndTransactionRequestHeader();
				header.setTopic(topic);
				header.setCommitLogOffset(requestHeader.getCommitLogOffset());
				header.setProducerGroup(producerGroup);
				header.setTranStateTableOffset(requestHeader.getTranStateTableOffset());
				header.setFromTransactionCheck(true);
				header.setBrokerName(requestHeader.getBrokerName());

				String uniqKey = message.getProperties().get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
				if (uniqKey == null) {
					uniqKey = message.getMsgId();
				}

				header.setMsgId(uniqKey);
				header.setTransactionId(requestHeader.getTransactionId());
				switch (localTransactionState) {
					case COMMIT_MESSAGE:
						header.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
						break;
					case ROLLBACK_MESSAGE:
						header.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
						log.warn("when broker check, client rollback this transaction, {}", header);
						break;
					case UNKNOWN:
						header.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
						log.warn("when broker check, client does not know this transaction state, {}", header);
						break;
					default:
						break;
				}

				String remark = null;
				if (exception != null) {
					remark = "checkLocalTransactionState Exception: " + UtilAll.exceptionSimpleDesc(exception);
				}
				doExecuteEndTransactionHook(msg, uniqKey, brokerAddr, localTransactionState, true);

				try {
					DefaultMQProducerImpl.this.mqClientFactory.getMqClientAPIImpl().endTransactionOneway(brokerAddr, header, remark, 3000);
				}
				catch (Exception e) {
					log.error("endTransactionOneway exception", e);
				}
			}
		};

		this.checkExecutor.submit(request);
	}

	@Override
	public void updateTopicPublishInfo(String topic, TopicPublishInfo info) {
		if (info != null && topic != null) {
			TopicPublishInfo prev = this.topicPublishInfoTable.put(topic, info);
			if (prev != null) {
				log.info("updateTopicPublishInfo prev is not null, {}", prev);
			}
		}
	}

	@Override
	public boolean isUnitMode() {
		return this.defaultMQProducer.isUnitMode();
	}

	public void sendOneway(Message msg) throws MQClientException, InterruptedException, RemotingTooMuchRequestException {
		try {
			sendDefaultImpl(msg, CommunicationMode.ONEWAY, null, defaultMQProducer.getSendMsgTimeout());
		}
		catch (MQBrokerException e) {
			throw new MQClientException("unknown exception", e);
		}
	}

	public void sendOneway(Message msg, MessageQueue mq) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, InterruptedException, RemotingTooMuchRequestException {
		makeSureStateOK();
		Validators.checkMessage(msg, defaultMQProducer);

		try {
			sendKernelImpl(msg, mq, CommunicationMode.ONEWAY, null, null, defaultMQProducer.getSendMsgTimeout());
		}
		catch (MQBrokerException e) {
			throw new MQClientException("unknown exception", e);
		}
	}

	public void sendOneway(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, InterruptedException, RemotingTooMuchRequestException {
		try {
			sendSelectImpl(msg, selector, arg, CommunicationMode.ONEWAY, null, defaultMQProducer.getSendMsgTimeout());
		}
		catch (MQBrokerException e) {
			throw new MQClientException("unknown exception", e);
		}
	}

	public Message request(Message msg, long timeout) throws MQBrokerException, InterruptedException, MQClientException, RemotingTooMuchRequestException, RequestTimeoutException {
		long begin = System.currentTimeMillis();
		prepareSendRequest(msg, timeout);
		String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

		try {
			RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
			RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

			long cost = System.currentTimeMillis() - begin;
			sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
				@Override
				public void onSuccess(SendResult sendResult) {
					requestResponseFuture.setSendRequestOk(true);
				}

				@Override
				public void onException(Throwable e) {
					requestResponseFuture.setSendRequestOk(false);
					requestResponseFuture.putResponseMessage(null);
					requestResponseFuture.setCause(e);
				}
			}, timeout - cost);

			return waitResponse(msg, timeout, requestResponseFuture, cost);
		}
		finally {
			RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
		}
	}

	public void request(Message msg, RequestCallback requestCallback, long timeout) throws MQBrokerException, InterruptedException, MQClientException, RemotingTooMuchRequestException {
		long begin = System.currentTimeMillis();
		prepareSendRequest(msg, timeout);
		String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

		RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
		RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

		long cost = System.currentTimeMillis() - begin;
		sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
			@Override
			public void onSuccess(SendResult sendResult) {
				requestResponseFuture.setSendRequestOk(true);
				requestResponseFuture.executeRequestCallback();
			}

			@Override
			public void onException(Throwable e) {
				requestResponseFuture.setCause(e);
				requestFail(correlationId);
			}
		}, timeout - cost);
	}

	public Message request(Message msg, MessageQueue mq, final RequestCallback requestCallback, long timeout) {
		long begin = System.currentTimeMillis();
		prepareSendRequest(msg, timeout);
		String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

		RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
		RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

		long cost = System.currentTimeMillis() - begin;
		sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
			@Override
			public void onSuccess(SendResult sendResult) {
				requestResponseFuture.setSendRequestOk(true);
			}

			@Override
			public void onException(Throwable e) {
				requestResponseFuture.setCause(e);
				requestFail(correlationId);
			}
		}, null, timeout - cost);
	}

	public Message request(Message msg, MessageQueueSelector selector, Object arg, long timeout) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQBrokerException, InterruptedException, MQClientException, RemotingTooMuchRequestException, RequestTimeoutException {
		long begin = System.currentTimeMillis();
		prepareSendRequest(msg, timeout);
		String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

		try {
			RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
			RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

			long cost = System.currentTimeMillis() - begin;
			sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
				@Override
				public void onSuccess(SendResult sendResult) {
					requestResponseFuture.setSendRequestOk(true);
				}

				@Override
				public void onException(Throwable e) {
					requestResponseFuture.setSendRequestOk(false);
					requestResponseFuture.putResponseMessage(null);
					requestResponseFuture.setCause(e);
				}
			}, timeout - cost);

			return waitResponse(msg, timeout, requestResponseFuture, cost);
		}
		finally {
			RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
		}
	}

	public Message request(Message msg, MessageQueueSelector selector, Object arg, final RequestCallback requestCallback, final long timeout) throws RequestTimeoutException, InterruptedException, MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQBrokerException, RemotingTooMuchRequestException {
		long begin = System.currentTimeMillis();
		prepareSendRequest(msg, timeout);
		String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

		try {
			RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
			RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

			long cost = System.currentTimeMillis() - begin;
			sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
				@Override
				public void onSuccess(SendResult sendResult) {
					requestResponseFuture.setSendRequestOk(true);
				}

				@Override
				public void onException(Throwable e) {
					requestResponseFuture.setSendRequestOk(false);
					requestResponseFuture.putResponseMessage(null);
					requestResponseFuture.setCause(e);
				}
			}, timeout - cost);

			return waitResponse(msg ,timeout, requestResponseFuture, cost);
		}
		finally {
			RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
		}
	}

	public Message request(final Message msg, final MessageQueue mq, final long timeout) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQBrokerException, InterruptedException, MQClientException, RemotingTooMuchRequestException, RequestTimeoutException {
		long begin = System.currentTimeMillis();
		prepareSendRequest(msg, timeout);
		String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

		try {
			RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
			RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

			long cost = System.currentTimeMillis() - begin;
			sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
				@Override
				public void onSuccess(SendResult sendResult) {
					requestResponseFuture.setSendRequestOk(true);
				}

				@Override
				public void onException(Throwable e) {
					requestResponseFuture.setSendRequestOk(false);
					requestResponseFuture.putResponseMessage(null);
					requestResponseFuture.setCause(e);
				}
			}, null, timeout - cost);

			return waitResponse(msg, timeout, requestResponseFuture, cost);
		}
		finally {
			RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
		}
	}

	public boolean hasCheckForbiddenHook() {
		return !checkForbiddenHookList.isEmpty();
	}

	public void executeCheckForbiddenHook(final CheckForbiddenContext context) throws MQClientException {
		if (hasEndTransactionHook()) {
			for (CheckForbiddenHook hook : checkForbiddenHookList) {
				hook.checkForbidden(context);
			}
		}
	}

	public boolean hasSendMessageHook() {
		return !sendMessageHookList.isEmpty();
	}

	public void executeSendMessageHookBefore(final SendMessageContext context) {
		if (hasSendMessageHook()) {
			for (SendMessageHook hook : sendMessageHookList) {
				try {
					hook.sendMessageBefore(context);
				}
				catch (Throwable e) {
					log.warn("failed to executeSendMessageHookBefore", e);
				}
			}
		}
	}

	public void executeSendMessageHookAfter(final SendMessageContext context) {
		if (hasSendMessageHook()) {
			for (SendMessageHook hook : sendMessageHookList) {
				try {
					hook.sendMessageAfter(context);
				}
				catch (Throwable e) {
					log.warn("failed to executeSendMessageHookAfter", e);
				}
			}
		}
	}

	public boolean hasEndTransactionHook() {
		return !this.endTransactionHookList.isEmpty();
	}

	public void executeEndTransactionHook(final EndTransactionContext context) {
		if (hasEndTransactionHook()) {
			for (EndTransactionHook hook : endTransactionHookList) {
				try {
					hook.endTransaction(context);
				}
				catch (Throwable e) {
					log.warn("failed to executeEndTransactionHook", e);
				}
			}
		}
	}

	public void doExecuteEndTransactionHook(Message msg, String msgId, String brokerAddr, LocalTransactionState state, boolean fromTransactionCheck) {
		if (hasEndTransactionHook()) {
			EndTransactionContext context = new EndTransactionContext();
			context.setProducerGroup(defaultMQProducer.getProducerGroup());
			context.setBrokerAddr(brokerAddr);
			context.setMessage(msg);
			context.setMsgId(msgId);
			context.setTransactionId(msg.getTransactionId());
			context.setTransactionState(state);
			context.setFromTransactionCheck(fromTransactionCheck);

			executeEndTransactionHook(context);
		}
	}

	public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
		createTopic(key, newTopic, queueNum, 0);
	}

	public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
		this.makeSureStateOK();
		Validators.checkTopic(newTopic);
		Validators.isSystemTopic(newTopic);

		this.mqClientFactory.getMqAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag, null);
	}

	public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
		this.makeSureStateOK();
		return this.mqClientFactory.getMqAdminImpl().fetchPublishMessageQueues(topic);
	}

	public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
		this.makeSureStateOK();
		return this.mqClientFactory.getMqAdminImpl().searchOffset(mq, timestamp);
	}

	public long maxOffset(MessageQueue mq) throws MQClientException {
		this.makeSureStateOK();
		return this.mqClientFactory.getMqAdminImpl().maxOffset(mq);
	}

	public long minOffset(MessageQueue mq) throws MQClientException {
		this.makeSureStateOK();
		return this.mqClientFactory.getMqAdminImpl().minOffset(mq);
	}

	public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
		this.makeSureStateOK();
		return this.mqClientFactory.getMqAdminImpl().earliestMsgStoreTime(mq);
	}

	public MessageExt viewMessage(String topic, String msgId) throws MQClientException {
		this.makeSureStateOK();
		return this.mqClientFactory.getMqAdminImpl().viewMessage(topic, msgId);
	}

	public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end) throws InterruptedException, MQClientException {
		this.makeSureStateOK();
		return this.mqClientFactory.getMqAdminImpl().queryMessage(topic, key, maxNum, begin, end);
	}

	public MessageExt queryMessageByUniqKey(String topic, String uniqKey) throws InterruptedException, MQClientException {
		this.makeSureStateOK();
		return this.mqClientFactory.getMqAdminImpl().queryMessageByUniqKey(topic, uniqKey);
	}

	public SendResult send(Message msg) throws MQBrokerException, InterruptedException, MQClientException, RemotingTooMuchRequestException {
		return send(msg, defaultMQProducer.getSendMsgTimeout());
	}

	public SendResult send(Message msg, MessageQueue mq) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQBrokerException, InterruptedException, MQClientException, RemotingTooMuchRequestException {
		return send(msg, mq, defaultMQProducer.getSendMsgTimeout());
	}

	public SendResult send(Message msg, long timeout) throws MQBrokerException, InterruptedException, MQClientException, RemotingTooMuchRequestException {
		return sendDefaultImpl(msg, CommunicationMode.SYNC, null, timeout);
	}

	public SendResult send(Message msg, MessageQueue mq, long timeout) throws MQClientException, RemotingTooMuchRequestException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQBrokerException, InterruptedException {
		long begin = System.currentTimeMillis();
		makeSureStateOK();
		Validators.checkMessage(msg, defaultMQProducer);

		if (!msg.getTopic().equals(mq.getTopic())) {
			throw new MQClientException("message's topic not equal mq's topic", null);
		}

		long cost = System.currentTimeMillis() - begin;
		if (timeout < cost) {
			throw new RemotingTooMuchRequestException("call timeout");
		}

		return sendKernelImpl(msg, mq, CommunicationMode.SYNC, null, null, timeout);
	}

	public SendResult send(Message msg, MessageQueueSelector selector, Object arg) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQBrokerException, InterruptedException, MQClientException, RemotingTooMuchRequestException {
		return send(msg, selector, arg, defaultMQProducer.getSendMsgTimeout());
	}

	public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQBrokerException, InterruptedException, MQClientException, RemotingTooMuchRequestException {
		return sendSelectImpl(msg, selector, arg, CommunicationMode.SYNC, null, timeout);
	}

	private SendResult sendSelectImpl(Message msg, MessageQueueSelector selector, Object arg, CommunicationMode communicationMode, final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingTooMuchRequestException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQBrokerException, InterruptedException {
		long begin = System.currentTimeMillis();
		makeSureStateOK();
		Validators.checkMessage(msg, defaultMQProducer);

		TopicPublishInfo info = tryToFindTopicPublishInfo(msg.getTopic());
		if (info != null && info.ok()) {
			MessageQueue mq = null;
			try {
				List<MessageQueue> mqs = mqClientFactory.getMqAdminImpl().parsePublishMessageQueues(info.getMessageQueueList());
				Message userMessage = MessageAccessor.cloneMessage(msg);
				String userTopic = NamespaceUtil.withoutNamespace(userMessage.getTopic(), mqClientFactory.getClientConfig().getNamespace());
				userMessage.setTopic(userTopic);

				mq = mqClientFactory.getClientConfig().queueWithNamespace(selector.select(mqs, userMessage, arg));
			}
			catch (Throwable e) {
				throw new MQClientException("select message queue threw exception", e);
			}

			long cost = System.currentTimeMillis() - begin;
			if (timeout < cost) {
				throw new RemotingTooMuchRequestException("sendSelectImpl call timeout");
			}
			if (mq != null) {
				return sendKernelImpl(msg, mq, communicationMode, sendCallback, null, timeout - cost);
			}
			else {
				throw new MQClientException("select message queue return null", null);
			}
		}

		validateNameServerSetting();

		throw new MQClientException("No route info for this topic, " + msg.getTopic(), null);
	}

	public void send(Message msg, SendCallback callback) throws InterruptedException, MQClientException {
		send(msg, callback, this.defaultMQProducer.getSendMsgTimeout());
	}

	public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback) {
		send(msg, selector, arg, sendCallback, defaultMQProducer.getSendMsgTimeout());
	}

	public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, final long timeout) {
		BackpressureSendCallback newCallback = new BackpressureSendCallback(sendCallback);
		long begin = System.currentTimeMillis();
		Runnable runnable = () -> {
			long costTime = System.currentTimeMillis() - begin;
			if (timeout > costTime) {
				try {
					try {
						sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, newCallback, timeout - costTime);
					}
					catch (MQBrokerException e) {
						throw new MQClientException("unknown exception", e);
					}
				}
				catch (Exception e) {
					newCallback.onException(e);
				}
			}
			else {
				newCallback.onException(new RemotingTooMuchRequestException("call timeout"));
			}
		};

		executeAsyncMessageSend(runnable, msg, newCallback, timeout, begin);
	}

	public void send(Message msg, MessageQueue mq, SendCallback sendCallback) throws InterruptedException, MQClientException {
		send(msg, mq, sendCallback, defaultMQProducer.getSendMsgTimeout());
	}

	public void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout) throws InterruptedException, MQClientException {
		BackpressureSendCallback newCallback = new BackpressureSendCallback(sendCallback);
		long begin = System.currentTimeMillis();
		Runnable runnable = () -> {
			try {
				makeSureStateOK();
				Validators.checkMessage(msg, defaultMQProducer);

				if (!msg.getTopic().equals(mq.getTopic())) {
					throw new MQClientException("Topic of the message does not match its target message queue", null);
				}

				long cost = System.currentTimeMillis() - begin;
				if (timeout > cost) {
					try {
						sendKernelImpl(msg, mq, CommunicationMode.ASYNC, sendCallback, null, timeout - cost);
					}
					catch (MQBrokerException e) {
						throw new MQClientException("unknown exception", e);
					}
				}
				else {
					newCallback.onException(new RemotingTooMuchRequestException("call timeout"));
				}
			}
			catch (Exception e) {
				newCallback.onException(e);
			}
		};

		executeAsyncMessageSend(runnable, msg, newCallback, timeout, begin);
	}

	public void send(final Message msg, final SendCallback callback, final long timeoutMillis) throws MQClientException, InterruptedException {
		BackpressureSendCallback newCallback = new BackpressureSendCallback(callback);

		final long begin = System.currentTimeMillis();
		Runnable runnable = () -> {
			final long costTime = System.currentTimeMillis() - begin;
			if (timeoutMillis > costTime) {
				try {
					sendDefaultImpl(msg, CommunicationMode.ASYNC, newCallback, timeoutMillis - costTime);
				}
				catch (Exception e) {
					newCallback.onException(e);
				}
			}
			else {
				newCallback.onException(new RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout"));
			}
		};

		executeAsyncMessageSend(runnable, msg, newCallback, timeoutMillis, begin);
	}

	public void executeAsyncMessageSend(Runnable runnable, final Message msg, final BackpressureSendCallback callback, final long timeout, final long begin) throws MQClientException, InterruptedException {
		ExecutorService executor = this.getAsyncSenderExecutor();
		boolean isEnableBackpressureForAsyncMode = this.getDefaultMQProducer().isEnableBackpressureForAsyncMode();
		boolean isSemaphoreAsyncNumAcquired = false;
		boolean isSemaphoreAsyncSizeAcquired = false;
		int msgLen = msg.getBody() == null ? 1 : msg.getBody().length;

		try {
			if (isEnableBackpressureForAsyncMode) {
				long cost = System.currentTimeMillis() - begin;
				isSemaphoreAsyncNumAcquired = timeout - cost > 0 && semaphoreAsyncSendNum.tryAcquire(timeout - cost, TimeUnit.MILLISECONDS);
				if (!isSemaphoreAsyncNumAcquired) {
					callback.onException(new RemotingTooMuchRequestException("send message tryAcquire semaphoreAsyncNum timeout"));
					return;
				}

				cost = System.currentTimeMillis() - begin;
				isSemaphoreAsyncSizeAcquired = timeout - cost > 0 && semaphoreAsyncSendSize.tryAcquire(msgLen, timeout - cost, TimeUnit.MILLISECONDS);
				if (!isSemaphoreAsyncSizeAcquired) {
					callback.onException(new RemotingTooMuchRequestException("send message tryAcquire semaphoreAsyncSize timeout"));
					return;
				}
			}

			callback.isSemaphoreAsyncNumAcquired = isSemaphoreAsyncNumAcquired;
			callback.isSemaphoreAsyncSizeAcquired = isSemaphoreAsyncSizeAcquired;
			callback.msgLen = msgLen;
			executor.submit(runnable);
		}
		catch (RejectedExecutionException e) {
			if (isEnableBackpressureForAsyncMode) {
				runnable.run();
			}
			else {
				throw new MQClientException("executor rejected ", e);
			}
		}
	}

	public MessageQueue invokeMessageQueueSelector(Message msg, MessageQueueSelector selector, Object arg, final long timeout) throws MQClientException, RemotingTooMuchRequestException {
		long begin = System.currentTimeMillis();
		makeSureStateOK();
		Validators.checkMessage(msg, defaultMQProducer);

		TopicPublishInfo info = tryToFindTopicPublishinfo(msg.getTopic());
		if (info != null && info.ok()) {
			MessageQueue mq = null;
			try {
				List<MessageQueue> messageQueueList = mqClientFactory.getMqAdminImpl().parsePublishMessageQueues(info.getMessageQueueList());
				Message userMessage = MessageAccessor.cloneMessage(msg);
				String userTopic = NamespaceUtil.withoutNamespace(userMessage.getTopic(), mqClientFactory.getClientConfig().getNamespace());
				userMessage.setTopic(userTopic);

				mq = mqClientFactory.getClientConfig().queueWithNamespace(selector.select(messageQueueList, userMessage, arg));
			}
			catch (Throwable e) {
				throw new MQClientException("select message queue throw exception.", e);
			}

			long cost = System.currentTimeMillis() - begin;
			if (timeout < cost) {
				throw new RemotingTooMuchRequestException("sendSelectImpl call timeout");
			}
			if (mq != null) {
				return mq;
			}
			else {
				throw new MQClientException("select message queue return null", null);
			}
		}

		validateNameServerSetting();
		throw new MQClientException("No route info for this topic, " + msg.getTopic(), null);
	}

	public MessageQueue selectOneMessageQueue(final TopicPublishInfo info, final String lastBrokerName, final boolean resetIndex) {
		return mqFaultStrategy.selectOneMessageQueue(info, lastBrokerName, resetIndex);
	}

	public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation, boolean reachable) {
		mqFaultStrategy.updateFaultItem(brokerName, currentLatency, isolation, reachable);
	}

	public void setCallbackExecutor(final ExecutorService executor) {
		mqClientFactory.getMqClientAPIImpl().getRemotingClient().setCallbackExecutor(executor);
	}

	public long[] getNotAvailableDuration() {
		return mqFaultStrategy.getNotAvailableDuration();
	}

	public void setNotAvailableDuration(final long[] notAvailableDuration) {
		mqFaultStrategy.setNotAvailableDuration(notAvailableDuration);
	}

	public long[] getLatencyMax() {
		return mqFaultStrategy.getLatencyMax();
	}

	public void setLatencyMax(final long[] latencyMax) {
		mqFaultStrategy.setLatencyMax(latencyMax);
	}

	public boolean isSendLatencyFaultEnable() {
		return mqFaultStrategy.isSendLatencyFaultEnable();
	}

	public void setSendLatencyFaultEnable(boolean sendLatencyFaultEnable) {
		mqFaultStrategy.setSendLatencyFaultEnable(sendLatencyFaultEnable);
	}

	private Message waitResponse(Message msg, long timeout, RequestResponseFuture requestResponseFuture, long cost) throws RequestTimeoutException, InterruptedException, MQClientException {
		Message message = requestResponseFuture.waitResponseMessage(timeout - cost);
		if (message == null) {
			if (requestResponseFuture.isSendRequestOk()) {
				throw new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION, "send request message to <" + msg.getTopic() + "> OK, but wait reply message timeout, " + timeout + "ms");
			}
			else {
				throw new MQClientException("send request message to <" + msg.getTopic() + "> fail", requestResponseFuture.getCause());
			}
		}
		return message;
	}

	private void requestFail(String correlationId) {
		RequestResponseFuture responseFuture = RequestFutureHolder.getInstance().getRequestFutureTable().get(correlationId);
		if (responseFuture != null) {
			responseFuture.setSendRequestOk(false);
			responseFuture.putResponseMessage(null);
			try {
				responseFuture.executeRequestCallback();
			}
			catch (Exception e) {
				log.warn("execute requestCallback in requestFail, and callback throw", e);
			}
		}
	}

	private void prepareSendRequest(final Message msg, long timeout) {
		String correlationId = CorrelationIdUtil.createCorrelationId();
		String requestClientId = getMqClientFactory().getClientId();
		MessageAccessor.putProperty(msg, MessageConst.PROPERTY_CORRELATION_ID, correlationId);
		MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, requestClientId);
		MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_TTL, String.valueOf(timeout));

		boolean hasRouteData = getMqClientFactory().getTopicRouteTable().containsKey(msg.getTopic());
		if (!hasRouteData) {
			long begin = System.currentTimeMillis();
			tryToFindTopicPublishInfo(msg.getTopic());
			getMqClientFactory().sendHeartbeatToAllBrokerWithLock();
			long cost = System.currentTimeMillis() - begin;
			if (cost > 500) {
				log.warn("prepare send request for <{}> cost {}ms", msg.getTopic(), cost);
			}
		}
	}

	private void validateNameServerSetting() throws MQClientException {
		List<String> nsList = getMqClientFactory().getMqClientAPIImpl().getNameServerAddressList();
		if (CollectionUtils.isEmpty(nsList)) {
			throw new MQClientException("No name server address, please set it." + FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL), null);
		}
	}

	private TopicPublishInfo tryToFindTopicPublishinfo(final String topic) {
		TopicPublishInfo info = topicPublishInfoTable.get(topic);
		if (info == null || !info.ok()) {
			topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
			mqClientFactory.updateTopicRouteInfoFromNameServer(topic);
			info = topicPublishInfoTable.get(topic);
		}

		if (info.isHaveTopicRouterInfo() || info.ok()) {
			return info;
		}
		else {
			mqClientFactory.updateTopicRouteInfoFromNameServer(topic, true, defaultMQProducer);
			info = topicPublishInfoTable.get(topic);
			return info;
		}
	}

	private SendResult sendDefaultImpl(Message msg, final CommunicationMode communicationMode, final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingTooMuchRequestException, MQBrokerException, InterruptedException {
		makeSureStateOK();
		Validators.checkMessage(msg, defaultMQProducer);
		long invokeID = random.nextLong();
		long beginFirst = System.currentTimeMillis();
		long beginPrev = beginFirst;
		long end = beginFirst;

		TopicPublishInfo info = tryToFindTopicPublishInfo(msg.getTopic());
		if (info != null && info.ok()) {
			boolean callTimeout = false;
			MessageQueue mq = null;
			Exception exception = null;
			SendResult sendResult = null;
			int timesTotal = communicationMode == CommunicationMode.SYNC ? 1 + defaultMQProducer.getRetryTimesWhenSendFailed() : 1;
			int times = 0;
			String[] brokersSent = new String[timesTotal];
			boolean resetIndex = false;

			for (; times < timesTotal; times++) {
				String lastBrokerName = mq == null ? null : mq.getBrokerName();
				if (times >= 0) {
					resetIndex = true;
				}

				MessageQueue mqSelected = selectOneMessageQueue(info, lastBrokerName, resetIndex);
				if (mqSelected != null) {
					mq = mqSelected;
					brokersSent[times] = mq.getBrokerName();
					try {
						beginPrev = System.currentTimeMillis();
						if (times >= 0) {
							msg.setTopic(defaultMQProducer.withNamespace(msg.getTopic()));
						}
						long cost = beginPrev - beginFirst;
						if (timeout < cost) {
							callTimeout = true;
							break;
						}

						sendResult = sendKernelImpl(msg, mq, communicationMode, sendCallback, info, timeout - cost);
						end = System.currentTimeMillis();
						updateFaultItem(mq.getBrokerName(), end - beginPrev, false, true);
						switch (communicationMode) {
							case ASYNC:
								return null;
							case ONEWAY:
								return null;
							case SYNC:
								if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
									if (defaultMQProducer.isRetryAnotherBrokerWhenNotStoreOK()) {
										continue;
									}
								}

								return sendResult;
							default:
								break;
						}
					}
					catch (MQClientException e) {
						end = System.currentTimeMillis();
						updateFaultItem(mq.getBrokerName(), end - beginPrev, false, true);
						log.warn("sendKernelImpl exception, resend at once, InvokeID: {}, RT: {}ms, Broker: {}",
								invokeID, end - beginPrev, mq, e);
						log.warn(msg.toString());
						exception = e;
						continue;
					}
					catch (RemotingException e) {
						end = System.currentTimeMillis();
						if (mqFaultStrategy.isStartDetectorEnable()) {
							updateFaultItem(mq.getBrokerName(), end - beginPrev, true, false);
						}
						else {
							updateFaultItem(mq.getBrokerName(), end - beginPrev, true, true);
						}

						log.warn("sendKernelImpl exception, resend at once, InovkeID: {}, RT: {}ms, Broker: {}",
								invokeID, end - beginPrev, mq, e);
						if (log.isDebugEnabled()) {
							log.debug(msg.toString());
						}
						exception = e;
						continue;
					}
					catch (MQBrokerException e) {
						end = System.currentTimeMillis();
						updateFaultItem(mq.getBrokerName(), end - beginPrev, true, false);
						log.warn("sendKernelImpl exception, resend at once, InvokeID: {}, RT: {}ms, Broker: {}",
								invokeID, end - beginPrev, mq, e);
						if (log.isDebugEnabled()) {
							log.debug(msg.toString());
						}
						exception = e;
						if (defaultMQProducer.getRetryResponseCodes().contains(e.getResponseCode())) {
							continue;
						}
						else {
							if (sendResult != null) {
								return sendResult;
							}
							throw e;
						}
					}
					catch (InterruptedException e) {
						end = System.currentTimeMillis();
						updateFaultItem(mq.getBrokerName(), end - beginPrev, false, true);
						log.warn("sendKernelImpl exception, throw exception, InvokeID: {}, RT: {}ms, Broker: {}",
								invokeID, end - beginPrev, mq, e);
						if (log.isDebugEnabled()) {
							log.debug(msg.toString());
						}
						throw e;
					}
				}
				else {
					break;
				}
			}

			if (sendResult != null) {
				return sendResult;
			}
			String errMsg = String.format("Send [%s] times, still failed, cost [%d]ms, Topic: %s, BrokerSent: %s",
					times, System.currentTimeMillis() - beginFirst, msg.getTopic(), Arrays.toString(brokersSent));
			errMsg += FAQUrl.suggestTodo(FAQUrl.SEND_MSG_FAILED);

			if (callTimeout) {
				throw new RemotingTooMuchRequestException("sendDefaultImpl call timeout");
			}

			MQClientException mqClientException = new MQClientException(errMsg, exception);

			if (exception instanceof MQBrokerException) {
				mqClientException.setResponseCode(((MQBrokerException) exception).getResponseCode());
			}
			else if (exception instanceof RemotingConnectException) {
				mqClientException.setResponseCode(ClientErrorCode.CONNECT_BROKER_EXCEPTION);
			}
			else if (exception instanceof RemotingTimeoutException) {
				mqClientException.setResponseCode(ClientErrorCode.ACCESS_BROKER_TIMEOUT);
			}
			else if (exception instanceof MQClientException) {
				mqClientException.setResponseCode(ClientErrorCode.BROKER_NOT_EXIST_EXCEPTION);
			}

			throw mqClientException;
		}

		validateNameServerSetting();

		throw new MQClientException(ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION, "No route info of this topic: " + msg.getTopic() + FAQUrl.suggestTodo(FAQUrl.NO_TOPIC_ROUTE_INFO), null);
	}

	private TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
		TopicPublishInfo info = topicPublishInfoTable.get(topic);
		if (info == null || !info.ok()) {
			topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
			mqClientFactory.updateTopicRouteInfoFromNameServer(topic);
			info = topicPublishInfoTable.get(topic);
		}

		if (info.isHaveTopicRouterInfo() || info.ok()) {
			return info;
		}
		else {
			mqClientFactory.updateTopicRouteInfoFromNameServer(topic, true, defaultMQProducer);
			return topicPublishInfoTable.get(topic);
		}
	}

	private SendResult sendKernelImpl(final Message msg, final MessageQueue mq, final CommunicationMode communicationMode, final SendCallback sendCallback, final TopicPublishInfo topicPublishInfo, final long timeout) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQBrokerException, InterruptedException, RemotingTooMuchRequestException {
		long begin = System.currentTimeMillis();
		String brokerName = mqClientFactory.getBrokerNameFromMessageQueue(mq);
		String brokerAddr = mqClientFactory.findBrokerAddressInPublish(brokerName);
		if (brokerAddr == null) {
			tryToFindTopicPublishinfo(mq.getTopic());
			brokerName = mqClientFactory.getBrokerNameFromMessageQueue(mq);
			brokerAddr = mqClientFactory.findBrokerAddressInPublish(brokerName);
		}

		SendMessageContext context = null;
		if (brokerAddr != null) {
			brokerAddr = MixAll.brokerVIPChannel(defaultMQProducer.isSendMessageWithVIPChannel(), brokerAddr);

			byte[] preBody = msg.getBody();
			try {
				if (!(msg instanceof MessageBatch)) {
					MessageClientIDSetter.setUniqID(msg);
				}

				boolean topicWithNamespace = false;
				if (mqClientFactory.getClientConfig().getNamespace() != null) {
					msg.setInstanceId(mqClientFactory.getClientConfig().getNamespace());
					topicWithNamespace = true;
				}

				int sysFlag = 0;
				boolean msgBodyCompressed = false;
				if (tryToCompressMessage(msg)) {
					sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
					sysFlag |= compressType.getCompressionFlag();
					msgBodyCompressed = true;
				}

				String tranMsg = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
				if (Boolean.parseBoolean(tranMsg)) {
					sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
				}

				if (hasCheckForbiddenHook()) {
					CheckForbiddenContext forbiddenContext = new CheckForbiddenContext();
					forbiddenContext.setNameSrvAddr(defaultMQProducer.getNamesrvAddr());
					forbiddenContext.setGroup(defaultMQProducer.getProducerGroup());
					forbiddenContext.setCommunicationMode(communicationMode);
					forbiddenContext.setBrokerAddr(brokerAddr);
					forbiddenContext.setMessage(msg);
					forbiddenContext.setMq(mq);
					forbiddenContext.setUnitMode(isUnitMode());

					executeCheckForbiddenHook(forbiddenContext);
				}

				if (hasSendMessageHook()) {
					context = new SendMessageContext();
					context.setProducer(this);
					context.setProducerGroup(defaultMQProducer.getProducerGroup());
					context.setCommunicationMode(communicationMode);
					context.setBornHost(defaultMQProducer.getClientIP());
					context.setBrokerAddr(brokerAddr);
					context.setMessage(msg);
					context.setMq(mq);
					context.setNamespace(defaultMQProducer.getNamespace());

					if (tranMsg != null && tranMsg.equals("true")) {
						context.setMsgType(MessageType.Trans_Msg_Half);
					}

					if (msg.getProperty("__STARTDELIVERTIME") != null && msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_LEVEL) != null) {
						context.setMsgType(MessageType.Delay_Msg);
					}
					executeSendMessageHookBefore(context);
				}

				SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
				requestHeader.setProducerGroup(defaultMQProducer.getProducerGroup());
				requestHeader.setTopic(msg.getTopic());
				requestHeader.setDefaultTopic(defaultMQProducer.getCreateTopicKey());
				requestHeader.setDefaultTopicQueueNums(defaultMQProducer.getDefaultTopicQueueNums());
				requestHeader.setQueueId(mq.getQueueId());
				requestHeader.setSysFlag(sysFlag);
				requestHeader.setBornTimestamp(System.currentTimeMillis());
				requestHeader.setFlag(msg.getFlag());
				requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));
				requestHeader.setReconsumeTimes(0);
				requestHeader.setUnitMode(isUnitMode());
				requestHeader.setBatch(msg instanceof MessageBatch);
				requestHeader.setBrokerName(brokerName);

				if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
					String reconsumeTime = MessageAccessor.getReconsumeTime(msg);
					if (reconsumeTime != null) {
						requestHeader.setReconsumeTimes(Integer.valueOf(reconsumeTime));
						MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME);
					}

					String maxReconsumeTimes = MessageAccessor.getMaxReconsumeTimes(msg);
					if (maxReconsumeTimes != null) {
						requestHeader.setMaxReconsumeTimes(Integer.valueOf(maxReconsumeTimes));
						MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
					}
				}

				SendResult sendResult = null;
				switch (communicationMode) {
					case ASYNC: {
						Message tmpMessage = msg;
						boolean messageCloned = false;
						if (msgBodyCompressed) {
							tmpMessage = MessageAccessor.cloneMessage(msg);
							messageCloned = true;
							msg.setBody(preBody);
						}

						if (topicWithNamespace) {
							if (!messageCloned) {
								tmpMessage = MessageAccessor.cloneMessage(msg);
								messageCloned = true;
							}
							msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), defaultMQProducer.getNamespace()));
						}

						long cost = System.currentTimeMillis() - begin;
						if (timeout < cost) {
							throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
						}

						sendResult = mqClientFactory.getMqClientAPIImpl().sendMessage(brokerAddr, brokerName, tmpMessage, requestHeader, timeout - cost,
								communicationMode, sendCallback, topicPublishInfo, mqClientFactory, defaultMQProducer.getRetryTimesWhenSendAsyncFailed(), context, this);
						break;
					}
					case ONEWAY:
					case SYNC: {
						long cost = System.currentTimeMillis() - begin;
						if (timeout < cost) {
							throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
						}

						sendResult = mqClientFactory.getMqClientAPIImpl().sendMessage(brokerAddr, brokerName, msg, requestHeader, timeout - cost,
								communicationMode, context, this);
						break;
					}
					default:
						assert false;
						break;
				}

				if (hasSendMessageHook()) {
					context.setSendResult(sendResult);
					executeSendMessageHookAfter(context);
				}

				return sendResult;
			}
			catch (RemotingException | InterruptedException | MQBrokerException e) {
				if (hasSendMessageHook()) {
					context.setException(e);
					executeSendMessageHookAfter(context);
				}
				throw e;
			}
			finally {
				msg.setBody(preBody);
				msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), defaultMQProducer.getNamespace()));
			}
		}

		throw new MQClientException("The broker[" + brokerName + "] not exist", null);
	}

	private boolean tryToCompressMessage(final Message msg) {
		if (msg instanceof MessageBatch) {
			return false;
		}

		byte[] body = msg.getBody();
		if (body != null) {
			if (body.length > defaultMQProducer.getCompressMsgBodyOverHowmuch()) {
				try {
					byte[] data = compressor.compress(body, compressLevel);
					if (data != null) {
						msg.setBody(data);
						return true;
					}
				}
				catch (IOException e) {
					log.error("tryToCompressMessage exception", e);
					if (log.isDebugEnabled()) {
						log.debug(msg.toString());
					}
				}
			}
		}

		return false;
	}

	private Optional<String> pickTopic() {
		if (topicPublishInfoTable.isEmpty()) {
			return Optional.empty();
		}
		return Optional.of(topicPublishInfoTable.keySet().iterator().next());
	}

	private void checkConfig() throws MQClientException {
		Validators.checkGroup(this.defaultMQProducer.getProducerGroup());

		if (this.defaultMQProducer.getProducerGroup().equals(MixAll.DEFAULT_PRODUCER_GROUP)) {
			throw new MQClientException("producerGroup can not equal " + MixAll.DEFAULT_PRODUCER_GROUP + ", please specify another one.", null);
		}
	}

	private void makeSureStateOK() throws MQClientException {
		if (this.serviceState != ServiceState.RUNNING) {
			throw new MQClientException("The producer service state not OK, " + this.serviceState + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
		}
	}

	private void initTopicRoute() {
		List<String> topics = defaultMQProducer.getTopics();
		if (CollectionUtils.isNotEmpty(topics)) {
			topics.forEach(topic -> {
				String newTopic = NamespaceUtil.wrapNamespace(defaultMQProducer.getNamespace(), topic);
				TopicPublishInfo info = tryToFindTopicPublishInfo(topic);
				if (info == null || !info.ok()) {
					log.warn("No route info of this topic: {}{}", topic, FAQUrl.suggestTodo(FAQUrl.NO_TOPIC_ROUTE_INFO));
				}
			});
		}
	}

	@RequiredArgsConstructor
	class BackpressureSendCallback implements SendCallback {
		public boolean isSemaphoreAsyncSizeAcquired = false;
		public boolean isSemaphoreAsyncNumAcquired = false;
		public int msgLen;
		public final SendCallback delegate;

		@Override
		public void onSuccess(SendResult sendResult) {
			if (isSemaphoreAsyncSizeAcquired) {
				semaphoreAsyncSendSize.release(msgLen);
			}
			if (isSemaphoreAsyncNumAcquired) {
				semaphoreAsyncSendNum.release();
			}
			delegate.onSuccess(sendResult);
		}

		@Override
		public void onException(Throwable e) {
			if (isSemaphoreAsyncSizeAcquired) {
				semaphoreAsyncSendSize.release(msgLen);
			}
			if (isSemaphoreAsyncNumAcquired) {
				semaphoreAsyncSendNum.release();
			}
			delegate.onException(e);
		}
	}
}
