package com.mawen.learn.rocketmq.client.impl.producer;

import java.util.ArrayList;
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
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.hook.CheckForbiddenHook;
import com.mawen.learn.rocketmq.client.hook.EndTransactionHook;
import com.mawen.learn.rocketmq.client.hook.SendMessageHook;
import com.mawen.learn.rocketmq.client.impl.CommunicationMode;
import com.mawen.learn.rocketmq.client.impl.MQClientManager;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.client.latency.MQFaultStrategy;
import com.mawen.learn.rocketmq.client.latency.Resolver;
import com.mawen.learn.rocketmq.client.latency.ServiceDetector;
import com.mawen.learn.rocketmq.client.producer.DefaultMQProducer;
import com.mawen.learn.rocketmq.client.producer.LocalTransactionState;
import com.mawen.learn.rocketmq.client.producer.MessageQueueSelector;
import com.mawen.learn.rocketmq.client.producer.RequestFutureHolder;
import com.mawen.learn.rocketmq.client.producer.SendCallback;
import com.mawen.learn.rocketmq.client.producer.SendResult;
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
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.sysflag.MessageSysFlag;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.mawen.learn.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/18
 */
@Getter
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
			private final String group = DefaultMQProducerImpl.this.defaultMQProducer.getProducerGruop();

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

	public void send(Message msg, SendCallback callback) {
		send(msg, callback, this.defaultMQProducer.getSendMsgTimeout());
	}

	public void send(final Message msg, final SendCallback callback, final long timeoutMillis) {
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

	public void executeAsyncMessageSend(Runnable runnable, final Message msg, final BackpressureSendCallback callback, final long timeout, final long begin) throws MQClientException {
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

	public MessageQueue invokeMessageQueueSelector(Message msg, MessageQueueSelector selector, Object arg, final long timeout) {
		long begin = System.currentTimeMillis();

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
			throw new MQClientException("producerGroup can not equal " + MixAll.DEFAULT_PRODUCER_GROUP + ", please specify another one.", null)
		}
	}

	private void makeSureStateOK() throws MQClientException {
		if (this.serviceState != ServiceState.RUNNING) {
			throw new MQClientException("The producer service state not OK, " + this.serviceState + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
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
