package com.mawen.learn.rocketmq.controller;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.common.ControllerConfig;
import com.mawen.learn.rocketmq.common.Pair;
import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.utils.ThreadUtils;
import com.mawen.learn.rocketmq.remoting.Configuration;
import com.mawen.learn.rocketmq.remoting.RemotingClient;
import com.mawen.learn.rocketmq.remoting.netty.NettyClientConfig;
import com.mawen.learn.rocketmq.remoting.netty.NettyRemotingClient;
import com.mawen.learn.rocketmq.remoting.netty.NettyServerConfig;
import com.mawen.learn.rocketmq.remoting.protocol.body.RoleChangeNotifyEntry;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/26
 */
@Getter
public class ControllerManager {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_CONSOLE_NAME);

	private final ControllerConfig controllerConfig;
	private final NettyServerConfig nettyServerConfig;
	private final NettyClientConfig nettyClientConfig;
	private final BrokerHousekeepingService brokerHousekeepingService;
	private final Configuration configuration;
	private final RemotingClient remotingClient;
	private Controller controller;
	private final BrokerHeartbeatManger heartbeatManger;
	private ExecutorService controllerRequestExecutor;
	private BlockingQueue<Runnable> controllerRequestThreadPoolQueue;
	private final NotifyService notifyService;
	private ControllerMetricsManager controllerMetricsManager;

	public ControllerManager(ControllerConfig controllerConfig, NettyServerConfig nettyServerConfig, NettyClientConfig nettyClientConfig) {
		this.controllerConfig = controllerConfig;
		this.nettyServerConfig = nettyServerConfig;
		this.nettyClientConfig = nettyClientConfig;
		this.brokerHousekeepingService = new BrokerHousekeepingService(this);
		this.configuration = new Configuration(log, controllerConfig, nettyClientConfig);
		this.configuration.setStorePathFromConfig(controllerConfig, "configStorePath");
		this.remotingClient = new NettyRemotingClient(nettyClientConfig);
		this.heartbeatManger = BrokerHeartbeatManger.newBrokerHeartbeatManager(controllerConfig);
		this.notifyService = new NotifyService();
	}

	public boolean initialize() {
		this.controllerRequestThreadPoolQueue = new LinkedBlockingQueue<>(controllerConfig.getControllerRequestsThreadPoolQueueCapacity());
		this.controllerRequestExecutor = ThreadUtils.newThreadPoolExecutor(
				controllerConfig.getControllerThreadPoolNums(),
				controllerConfig.getControllerThreadPoolNums(),
				60 * 1000,
				TimeUnit.MILLISECONDS,
				controllerRequestThreadPoolQueue,
				new ThreadFactoryImpl("ControllerRequestExecutorThread_"));

		this.notifyService.initialize();

		if (controllerConfig.getControllerType().equals(ControllerConfig.JRAFT_CONTROLLER)) {
			if (StringUtils.isEmpty(controllerConfig.getJraftConfig().getjRaftInitConf())) {
				throw new IllegalArgumentException("Attribute value jRaftInitConf of ControllerConfig is null or empty");
			}
			if (StringUtils.isEmpty(controllerConfig.getJraftConfig().getjRaftServerId())) {
				throw new IllegalArgumentException("Attribute value jRaftServerId of ControllerConfig is null or empty");
			}
			try {
				this.controller = new JRaftController(controllerConfig, brokerHousekeepingService);
				((RaftBrokerHeartBeatManager)this).heartbeatManger.setController((JRaftController)controller);
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		else {
			if (StringUtils.isEmpty(controllerConfig.getControllerDLegerPeers())) {
				throw new IllegalArgumentException("Attribute value controllerDLegerPeers of ControllerConfig is null or empty");
			}
			if (StringUtils.isEmpty(controllerConfig.getControllerDLegerSelfId())) {
				throw new IllegalArgumentException("Attribute value controllerDLegerSelfId of ControllerConfig is null or empty");
			}
			this.controller = new DLedgerController(controllerConfig, heartbeatManger::isBrokerActive,
					nettyServerConfig, nettyClientConfig, brokerHousekeepingService,
					new DefaultElectPolicy(heartbeatManger::isBrokerActive, heartbeatManger::getBrokerLiveInfo));
		}

		this.heartbeatManger.initialize();

		this.heartbeatManger.registerBrokerLifecycleListener(this::onBrokerInactive);
		this.controller.registerBrokerLifecycleListener(this::onBrokerInactive);

		registerProcessor();

		this.controllerMetricsManager = ControllerMetricsManager.getInstance(this);
		return true;
	}

	class NotifyService {
		private ExecutorService executorService;

		private Map<String, NotifyTask> currentNotifyFutures;

		public NotifyService() {}

		public void initialize() {
			this.executorService = Executors.newFixedThreadPool(3, new ThreadFactoryImpl("ControllerManager_NotifyService_"));
			this.currentNotifyFutures = new ConcurrentHashMap<>();
		}

		public void notifyBroker(String brokerAddress, RoleChangeNotifyEntry entry) {
			int masterEpoch = entry.getMasterEpoch();
			NotifyTask oldTask = currentNotifyFutures.get(brokerAddress);
			if (oldTask != null && masterEpoch > oldTask.getMasterEpoch()) {
				Future oldFuture = oldTask.getFuture();
				if (oldFuture != null && !oldFuture.isDone()) {
					oldFuture.cancel(true);
				}
			}

			NotifyTask task = new NotifyTask(masterEpoch, null);
			Runnable runnable = () -> {
				doNotifyBrokerRoleChanged(brokerAddress, entry);
				currentNotifyFutures.remove(brokerAddress, task);
			};
			currentNotifyFutures.put(brokerAddress, task);
			Future<?> future = executorService.submit(runnable);
			task.setFuture(future);
		}

		public void shutdown() {
			if (!executorService.isShutdown()) {
				executorService.shutdown();
			}
		}

		class NotifyTask extends Pair<Integer, Future> {
			public NotifyTask(Integer masterEpoch, Future future) {
				super(masterEpoch, future);
			}

			public Integer getMasterEpoch() {
				return super.getObject1();
			}

			public Future getFuture() {
				return super.getObject2();
			}

			public void setFuture(Future future) {
				super.setObject2(future);
			}

			@Override
			public int hashCode() {
				return Objects.hashCode(super.getObject1());
			}

			@Override
			public boolean equals(Object obj) {
				if (this == obj) {
					return true;
				}
				if (!(obj instanceof NotifyTask)) {
					return false;
				}
				NotifyTask task = (NotifyTask) obj;
				return super.getObject1().equals(task.getObject1());
			}
		}
	}
}
