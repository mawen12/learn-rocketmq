package com.mawen.learn.rocketmq.store.stats;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import com.mawen.learn.rocketmq.common.BrokerConfig;
import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.statistics.StatisticsItemFormatter;
import com.mawen.learn.rocketmq.common.statistics.StatisticsItemPrinter;
import com.mawen.learn.rocketmq.common.statistics.StatisticsItemScheduledIncrementPrinter;
import com.mawen.learn.rocketmq.common.statistics.StatisticsItemScheduledPrinter;
import com.mawen.learn.rocketmq.common.statistics.StatisticsKindMeta;
import com.mawen.learn.rocketmq.common.statistics.StatisticsManager;
import com.mawen.learn.rocketmq.common.stats.MomentStatsItemSet;
import com.mawen.learn.rocketmq.common.stats.Stats;
import com.mawen.learn.rocketmq.common.stats.StatsItem;
import com.mawen.learn.rocketmq.common.stats.StatsItemSet;
import com.mawen.learn.rocketmq.common.topic.TopicValidator;
import com.mawen.learn.rocketmq.common.utils.ThreadUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
@Getter
@Setter
public class BrokerStatsManager {

	public static final String QUEUE_PUT_NUMS = Stats.QUEUE_PUT_NUMS;
	public static final String QUEUE_PUT_SIZE = Stats.QUEUE_PUT_SIZE;

	public static final String QUEUE_GET_NUMS = Stats.QUEUE_GET_NUMS;
	public static final String QUEUE_GET_SIZE = Stats.QUEUE_GET_SIZE;

	public static final String TOPIC_PUT_NUMS = Stats.TOPIC_PUT_NUMS;
	public static final String TOPIC_PUT_SIZE = Stats.TOPIC_PUT_SIZE;

	public static final String GROUP_GET_NUMS = Stats.GROUP_GET_NUMS;
	public static final String GROUP_GET_SIZE = Stats.GROUP_GET_SIZE;
	public static final String GROUP_GET_FROM_DISK_NUMS = Stats.GROUP_GET_FROM_DISK_NUMS;
	public static final String GROUP_GET_FROM_DISK_SIZE = Stats.GROUP_GET_FROM_DISK_SIZE;

	public static final String SNDBCK_PUT_NUMS = Stats.SNDBCK_PUT_NUMS;

	public static final String BROKER_PUT_NUMS = Stats.BROKER_PUT_NUMS;
	public static final String BROKER_GET_NUMS = Stats.BROKER_GET_NUMS;
	public static final String BROKER_GET_FROM_DISK_NUMS = Stats.BROKER_GET_FROM_DISK_NUMS;
	public static final String BROKER_GET_FROM_DISK_SIZE = Stats.BROKER_GET_FROM_DISK_SIZE;

	public static final String COMMERCIAL_SEND_TIMES = Stats.COMMERCIAL_SEND_TIMES;
	public static final String COMMERCIAL_SNDBCK_TIMES = Stats.COMMERCIAL_SNDBCK_TIMES;
	public static final String COMMERCIAL_RCV_TIMES = Stats.COMMERCIAL_RCV_TIMES;
	public static final String COMMERCIAL_RCV_EPOLLS = Stats.COMMERCIAL_RCV_EPOLLS;
	public static final String COMMERCIAL_SEND_SIZE = Stats.COMMERCIAL_SEND_SIZE;
	public static final String COMMERCIAL_RCV_SIZE = Stats.COMMERCIAL_RCV_SIZE;
	public static final String COMMERCIAL_PERM_FAILURES = Stats.COMMERCIAL_PERM_FAILURES;

	public static final String TOPIC_PUT_LATENCY = "TOPIC_PUT_LATENCY";
	public static final String GROUP_ACK_NUMS = "GROUP_ACK_NUMS";
	public static final String GROUP_CK_NUMS = "GROUP_CK_NUMS";
	public static final String DLQ_PUT_NUMS = "DLQ_PUT_NUMS";
	public static final String BROKER_ACK_NUMS = "BROKER_ACK_NUMS";
	public static final String BROKER_CK_NUMS = "BROKER_CK_NUMS";
	public static final String BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC = "BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC";
	public static final String BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC = "BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC";
	public static final String SNDBCK2DLQ_TIMES = "SNDBCK2DLQ_TIMES";
	public static final String COMMERCIAL_OWNER = "COMMERCIAL_OWNER";

	public static final String ACCOUNT_OWNER_PARENT = "OWNER_PARENT";
	public static final String ACCOUNT_OWNER_SELF = "OWNER_SELF";
	public static final long ACCOUNT_STAT_INTERVAL = 60 * 1000;
	public static final String ACCOUNT_AUTH_TYPE = "AUTH_TYPE";

	public static final String ACCOUNT_SEND = "SEND";
	public static final String ACCOUNT_RCV = "RCV";
	public static final String ACCOUNT_SEND_BACK = "SEND_BACK";
	public static final String ACCOUNT_SEND_BACK_TO_DLQ = "SEND_BACK_TO_DLQ";
	public static final String ACCOUNT_AUTH_FAILED = "AUTH_FAILED";
	public static final String ACCOUNT_SEND_REJ = "SEND_REJ";
	public static final String ACCOUNT_REV_REJ = "REV_REJ";

	public static final String MSG_NUM = "MSG_NUM";
	public static final String MSG_SIZE = "MSG_SIZE";
	public static final String SUCCESS_MSG_NUM = "SUCCESS_MSG_NUM";
	public static final String FAILURE_MSG_NUM = "FAILURE_MSG_NUM";
	public static final String COMMERCIAL_MSG_NUM = "COMMERCIAL_MSG_NUM";
	public static final String SUCCESS_REQ_NUM = "SUCCESS_REQ_NUM";
	public static final String FAILURE_REQ_NUM = "FAILURE_REQ_NUM";
	public static final String SUCCESS_MSG_SIZE = "SUCCESS_MSG_SIZE";
	public static final String FAILURE_MSG_SIZE = "FAILURE_MSG_SIZE";
	public static final String RT = "RT";
	public static final String INNER_RT = "INNER_RT";

	public static final String GROUP_GET_FALL_SIZE = Stats.GROUP_GET_FALL_SIZE;
	public static final String GROUP_GET_FALL_TIME = Stats.GROUP_GET_FALL_TIME;
	public static final String GROUP_GET_LATENCY = Stats.GROUP_GET_LATENCY;

	public static final String PRODUCER_REGISTER_TIME = "PRODUCER_REGISTER_TIME";
	public static final String CONSUMER_REGISTER_TIME = "CONSUMER_REGISTER_TIME";
	public static final String CHANNEL_ACTIVITY = "CHANNEL_ACTIVITY";
	public static final String CHANNEL_ACTIVITY_CONNECT = "CHANNEL_ACTIVITY_CONNECT";
	public static final String CHANNEL_ACTIVITY_IDLE = "CHANNEL_ACTIVITY_IDLE";
	public static final String CHANNEL_ACTIVITY_EXCEPTION = "CHANNEL_ACTIVITY_EXCEPTION";
	public static final String CHANNEL_ACTIVITY_CLOSE = "CHANNEL_ACTIVITY_CLOSE";

	private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_STATS_LOGGER_NAME);
	private static final Logger COMMERCIAL_LOG = LoggerFactory.getLogger(LoggerName.COMMERCIAL_LOGGER_NAME);
	private static final Logger ACCOUNT_LOG = LoggerFactory.getLogger(LoggerName.ACCOUNT_LOGGER_NAME);
	private static final Logger DLQ_STAT_LOG = LoggerFactory.getLogger(LoggerName.DLQ_STATS_LOGGER_NAME);

	private ScheduledExecutorService scheduledExecutorService;
	private ScheduledExecutorService commercialExecutor;
	private ScheduledExecutorService accountExecutor;

	private final Map<String, StatsItemSet> statsTable = new HashMap<>();
	private final String clusterName;
	private final boolean enableQueueStat;
	private MomentStatsItemSet momentStatsItemSetFallSize;
	private MomentStatsItemSet momentStatsItemSetFallTime;

	private final StatisticsManager accountStatManager = new StatisticsManager();
	private StateGetter producerStateGetter;
	private StateGetter consumerStateGetter;

	private BrokerConfig brokerConfig;

	public BrokerStatsManager(BrokerConfig brokerConfig) {
		this.brokerConfig = brokerConfig;
		this.enableQueueStat = brokerConfig.isEnableDetailStat();
		this.clusterName = brokerConfig.getBrokerClusterName();
		initScheduleService();
		init();
	}

	public BrokerStatsManager(String clusterName, boolean enableQueueStat) {
		this.clusterName = clusterName;
		this.enableQueueStat = enableQueueStat;
		initScheduleService();
		init();
	}

	private void initScheduleService() {
		this.scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("BrokerStatsThread", true, brokerConfig));
		this.commercialExecutor = ThreadUtils.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CommercialStatsThread", true, brokerConfig));
		this.accountExecutor = ThreadUtils.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("AccountStatsThread", true, brokerConfig));
	}

	public void init() {
		this.momentStatsItemSetFallSize = new MomentStatsItemSet(GROUP_GET_FALL_SIZE, scheduledExecutorService, log);
		this.momentStatsItemSetFallTime = new MomentStatsItemSet(GROUP_GET_FALL_TIME, scheduledExecutorService, log);

		if (enableQueueStat) {
			statsTable.put(Stats.QUEUE_PUT_NUMS, new StatsItemSet(Stats.QUEUE_PUT_NUMS, scheduledExecutorService, log));
			statsTable.put(Stats.QUEUE_PUT_SIZE, new StatsItemSet(Stats.QUEUE_PUT_SIZE, scheduledExecutorService, log));
			statsTable.put(Stats.QUEUE_GET_NUMS, new StatsItemSet(Stats.QUEUE_PUT_NUMS, scheduledExecutorService, log));
			statsTable.put(Stats.QUEUE_GET_SIZE, new StatsItemSet(Stats.QUEUE_GET_SIZE, scheduledExecutorService, log));
		}

		statsTable.put(Stats.TOPIC_PUT_NUMS, new StatsItemSet(Stats.TOPIC_PUT_NUMS, scheduledExecutorService, log));
		statsTable.put(Stats.TOPIC_PUT_SIZE, new StatsItemSet(Stats.TOPIC_PUT_SIZE, scheduledExecutorService, log));
		statsTable.put(Stats.GROUP_GET_NUMS, new StatsItemSet(Stats.GROUP_GET_NUMS, scheduledExecutorService, log));
		statsTable.put(Stats.GROUP_GET_SIZE, new StatsItemSet(Stats.GROUP_GET_SIZE, scheduledExecutorService, log));

		statsTable.put(GROUP_ACK_NUMS, new StatsItemSet(GROUP_ACK_NUMS, scheduledExecutorService, log));
		statsTable.put(GROUP_CK_NUMS, new StatsItemSet(GROUP_CK_NUMS, scheduledExecutorService, log));
		statsTable.put(Stats.GROUP_GET_LATENCY, new StatsItemSet(Stats.GROUP_GET_LATENCY, scheduledExecutorService, log));
		statsTable.put(TOPIC_PUT_LATENCY, new StatsItemSet(TOPIC_PUT_LATENCY, scheduledExecutorService, log));
		statsTable.put(Stats.SNDBCK_PUT_NUMS, new StatsItemSet(Stats.SNDBCK_PUT_NUMS, scheduledExecutorService, log));
		statsTable.put(DLQ_PUT_NUMS, new StatsItemSet(DLQ_PUT_NUMS, scheduledExecutorService, log));
		statsTable.put(Stats.BROKER_PUT_NUMS, new StatsItemSet(Stats.BROKER_PUT_NUMS, scheduledExecutorService, log));
		statsTable.put(Stats.BROKER_GET_NUMS, new StatsItemSet(Stats.BROKER_GET_NUMS, scheduledExecutorService, log));

		statsTable.put(BROKER_ACK_NUMS, new StatsItemSet(BROKER_ACK_NUMS, scheduledExecutorService, log));
		statsTable.put(BROKER_CK_NUMS, new StatsItemSet(BROKER_CK_NUMS, scheduledExecutorService, log));
		statsTable.put(BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC, new StatsItemSet(BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC, scheduledExecutorService, log));
		statsTable.put(BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC, new StatsItemSet(BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC, scheduledExecutorService, log));
		statsTable.put(Stats.GROUP_GET_FROM_DISK_NUMS, new StatsItemSet(Stats.GROUP_GET_FROM_DISK_NUMS, scheduledExecutorService, log));
		statsTable.put(Stats.GROUP_GET_FROM_DISK_SIZE, new StatsItemSet(Stats.GROUP_GET_FROM_DISK_SIZE, scheduledExecutorService, log));
		statsTable.put(Stats.BROKER_GET_FROM_DISK_NUMS, new StatsItemSet(Stats.BROKER_GET_FROM_DISK_NUMS, scheduledExecutorService, log));
		statsTable.put(Stats.BROKER_GET_FROM_DISK_SIZE, new StatsItemSet(Stats.BROKER_GET_FROM_DISK_SIZE, scheduledExecutorService, log));
		statsTable.put(SNDBCK2DLQ_TIMES, new StatsItemSet(SNDBCK2DLQ_TIMES, scheduledExecutorService, log));

		statsTable.put(COMMERCIAL_SEND_TIMES, new StatsItemSet(COMMERCIAL_SEND_TIMES, commercialExecutor, COMMERCIAL_LOG));
		statsTable.put(COMMERCIAL_RCV_TIMES, new StatsItemSet(COMMERCIAL_RCV_TIMES, commercialExecutor, COMMERCIAL_LOG));
		statsTable.put(COMMERCIAL_SEND_SIZE, new StatsItemSet(COMMERCIAL_SEND_SIZE, commercialExecutor, COMMERCIAL_LOG));
		statsTable.put(COMMERCIAL_RCV_SIZE, new StatsItemSet(COMMERCIAL_RCV_SIZE, commercialExecutor, COMMERCIAL_LOG));
		statsTable.put(COMMERCIAL_RCV_EPOLLS, new StatsItemSet(COMMERCIAL_RCV_EPOLLS, commercialExecutor, COMMERCIAL_LOG));
		statsTable.put(COMMERCIAL_SNDBCK_TIMES, new StatsItemSet(COMMERCIAL_SNDBCK_TIMES, commercialExecutor, COMMERCIAL_LOG));
		statsTable.put(COMMERCIAL_PERM_FAILURES, new StatsItemSet(COMMERCIAL_PERM_FAILURES, commercialExecutor, COMMERCIAL_LOG));

		statsTable.put(CONSUMER_REGISTER_TIME, new StatsItemSet(CONSUMER_REGISTER_TIME, scheduledExecutorService, log));
		statsTable.put(PRODUCER_REGISTER_TIME, new StatsItemSet(PRODUCER_REGISTER_TIME, scheduledExecutorService, log));

		statsTable.put(CHANNEL_ACTIVITY, new StatsItemSet(CHANNEL_ACTIVITY, scheduledExecutorService, log));

		StatisticsItemFormatter formatter = new StatisticsItemFormatter();
		accountStatManager.setBriefMetas(new Pair[]{
				Pair.of(RT, new long[][]{{50, 50}, {100, 10}, {1000, 10}}),
				Pair.of(INNER_RT, new long[][]{{10, 10}, {100, 10}, {1000, 10}})
		});
		String[] itemNames = new String[]{
				MSG_NUM, SUCCESS_MSG_NUM, FAILURE_MSG_NUM, COMMERCIAL_MSG_NUM,
				SUCCESS_REQ_NUM, FAILURE_REQ_NUM,
				MSG_SIZE, SUCCESS_MSG_SIZE, FAILURE_MSG_SIZE,
				RT, INNER_RT
		};
		accountStatManager.addStatisticsKindMeta(createStatisticsKindMeta(ACCOUNT_SEND, itemNames, accountExecutor, formatter, ACCOUNT_LOG, ACCOUNT_STAT_INTERVAL));
		accountStatManager.addStatisticsKindMeta(createStatisticsKindMeta(ACCOUNT_RCV, itemNames, accountExecutor, formatter, ACCOUNT_LOG, ACCOUNT_STAT_INTERVAL));
		accountStatManager.addStatisticsKindMeta(createStatisticsKindMeta(ACCOUNT_SEND_BACK, itemNames, accountExecutor, formatter, ACCOUNT_LOG, ACCOUNT_STAT_INTERVAL));
		accountStatManager.addStatisticsKindMeta(createStatisticsKindMeta(ACCOUNT_SEND_BACK_TO_DLQ, itemNames, accountExecutor, formatter, ACCOUNT_LOG, ACCOUNT_STAT_INTERVAL));
		accountStatManager.addStatisticsKindMeta(createStatisticsKindMeta(ACCOUNT_SEND_REJ, itemNames, accountExecutor, formatter, ACCOUNT_LOG, ACCOUNT_STAT_INTERVAL));
		accountStatManager.addStatisticsKindMeta(createStatisticsKindMeta(ACCOUNT_REV_REJ, itemNames, accountExecutor, formatter, ACCOUNT_LOG, ACCOUNT_STAT_INTERVAL));
		accountStatManager.setStatisticsItemStateGetter(item -> {
			String[] strArr = null;
			try {
				strArr = splitAccountStatKey(item.getStatObject());
			}
			catch (Exception e) {
				log.warn("parse account stat key failed, key: {}", item.getStatObject());
				return false;
			}

			if (strArr == null || strArr.length < 4) {
				return false;
			}

			String instanceId = strArr[1];
			String topic = strArr[2];
			String group = strArr[3];

			String kind = item.getStatKind();
			if (ACCOUNT_SEND.equals(kind) || ACCOUNT_SEND_REJ.equals(kind)) {
				return producerStateGetter.online(instanceId, group, topic);
			}
			else if (ACCOUNT_RCV.equals(kind) || ACCOUNT_SEND_BACK.equals(kind) || ACCOUNT_SEND_BACK_TO_DLQ.equals(kind)) {
				return consumerStateGetter.online(instanceId, group, topic);
			}
			return false;
		});
	}

	public void start() {
	}

	public void shutdown() {
		scheduledExecutorService.shutdown();
		commercialExecutor.shutdown();
	}

	public StatsItem getStatsItem(final String statsName, final String statsKey) {
		try {
			return statsTable.get(statsName).getStatsItem(statsKey);
		}
		catch (Exception e) {}

		return null;
	}

	public void onTopicDeleted(final String topic) {
		statsTable.get(Stats.TOPIC_PUT_NUMS).delValue(topic);
		statsTable.get(Stats.TOPIC_PUT_SIZE).delValue(topic);
		if (enableQueueStat) {
			statsTable.get(Stats.QUEUE_PUT_NUMS).delValueByPrefixKey(topic, "@");
			statsTable.get(Stats.QUEUE_PUT_SIZE).delValueByPrefixKey(topic, "@");
		}
		statsTable.get(Stats.GROUP_GET_NUMS).delValueByPrefixKey(topic, "@");
		statsTable.get(Stats.GROUP_GET_SIZE).delValueByPrefixKey(topic, "@");
		statsTable.get(Stats.QUEUE_GET_NUMS).delValueByPrefixKey(topic, "@");
		statsTable.get(Stats.QUEUE_GET_SIZE).delValueByPrefixKey(topic, "@");
		statsTable.get(Stats.SNDBCK_PUT_NUMS).delValueByPrefixKey(topic, "@");
		statsTable.get(Stats.GROUP_GET_LATENCY).delValueByPrefixKey(topic, "@");

		momentStatsItemSetFallSize.delValueByInfixKey(topic, "@");
		momentStatsItemSetFallTime.delValueByPrefixKey(topic, "@");
	}

	public void onGroupDeleted(final String group) {
		statsTable.get(Stats.GROUP_GET_NUMS).delValueBySuffixKey(group, "@");
		statsTable.get(Stats.GROUP_GET_SIZE).delValueByPrefixKey(group, "@");
		if (enableQueueStat) {
			statsTable.get(Stats.QUEUE_GET_NUMS).delValueBySuffixKey(group, "@");
			statsTable.get(Stats.QUEUE_GET_SIZE).delValueBySuffixKey(group, "@");
		}
		statsTable.get(Stats.SNDBCK_PUT_NUMS).delValueBySuffixKey(group, "@");
		statsTable.get(Stats.GROUP_GET_LATENCY).delValueBySuffixKey(group, "@");

		momentStatsItemSetFallSize.delValueBySuffixKey(group, "@");
		momentStatsItemSetFallTime.delValueBySuffixKey(group, "@");
	}

	public void incQueuePutNums(final String topic, final Integer queueId) {
		incQueuePutNums(topic, queueId, 1, 1 );
	}

	public void incQueuePutNums(final String topic, final Integer queueId, int num, int times) {
		if (enableQueueStat) {
			statsTable.get(Stats.QUEUE_PUT_NUMS).addValue(buildStatsKey(topic, queueId), num, times);
		}
	}

	public void incQueuePutSize(final String topic, final Integer queueId, final int size) {
		if (enableQueueStat) {
			statsTable.get(Stats.QUEUE_PUT_SIZE).addValue(buildStatsKey(topic, queueId), size, 1);
		}
	}

	public void incQueueGetNums(final String group, final String topic, final Integer queueId, final int incValue) {
		if (enableQueueStat) {
			statsTable.get(Stats.QUEUE_GET_NUMS).addValue(buildStatsKey(topic,queueId, group), incValue, 1);
		}
	}

	public void incQueueGetSize(final String group, final String topic, final Integer queueId, final int incValue) {
		if (enableQueueStat) {
			statsTable.get(Stats.QUEUE_GET_SIZE).addValue(buildStatsKey(topic, queueId, group), incValue, 1);
		}
	}

	public void incConsumerRegisterTime(final int incValue) {
		statsTable.get(CONSUMER_REGISTER_TIME).addValue(clusterName, incValue, 1);
	}

	public void incProducerRegisterTime(final int incValue) {
		statsTable.get(PRODUCER_REGISTER_TIME).addValue(clusterName, incValue,1);
	}

	public void incChannelConnectNum() {
		statsTable.get(CHANNEL_ACTIVITY).addValue(CHANNEL_ACTIVITY_CONNECT, 1, 1);
	}

	public void incChannelCloseNum() {
		statsTable.get(CHANNEL_ACTIVITY).addValue(CHANNEL_ACTIVITY_CLOSE, 1, 1);
	}

	public void incChannelExceptionNum() {
		statsTable.get(CHANNEL_ACTIVITY).addValue(CHANNEL_ACTIVITY_EXCEPTION, 1, 1);
	}

	public void incChannelIdleNum() {
		statsTable.get(CHANNEL_ACTIVITY).addValue(CHANNEL_ACTIVITY_IDLE, 1, 1);
	}

	public void incTopicPutNums(final String topic) {
		statsTable.get(TOPIC_PUT_NUMS).addValue(topic, 1, 1);
	}

	public void incTopicPutSIze(final String topic, final int size) {
		statsTable.get(TOPIC_PUT_SIZE).addValue(topic, size, 1);
	}

	public void incGroupGetNums(final String group, final String topic, final int incValue) {
		statsTable.get(GROUP_GET_NUMS).addValue(buildStatsKey(group, topic), incValue, 1);
	}

	public void incGroupCkNums(final String group, final String topic, final int incValue) {
		statsTable.get(GROUP_CK_NUMS).addValue(buildStatsKey(group, topic), incValue,1);
	}

	public void incGroupAckNums(final String group, final String topic, final int incValue) {
		statsTable.get(GROUP_ACK_NUMS).addValue(buildStatsKey(group, topic), incValue,1);
	}

	public void incGroupGetSize(final String group, final String topic, final int incValue) {
		statsTable.get(GROUP_GET_SIZE).addValue(buildStatsKey(group, topic), incValue, 1);
	}

	public void incGroupGetLatency(final String group, final String topic, final int queueId, final int incValue) {
		String statsKey;
		if (enableQueueStat) {
			statsKey = buildStatsKey(queueId, topic, group);
		}
		else {
			statsKey = buildStatsKey(topic, group);
		}

		statsTable.get(GROUP_GET_LATENCY).addRTValue(statsKey, incValue, 1);
	}

	public void incTopicPutLatency(final String topic, final int queueId, final int incValue) {
		StringBuilder sb;
		if (topic != null) {
			sb = new StringBuilder(topic.length() + 6);
		}
		else {
			sb = new StringBuilder(6);
		}
		sb.append(queueId).append("@").append(topic);

		statsTable.get(TOPIC_PUT_LATENCY).addValue(sb.toString(), incValue, 1);
	}

	public void incBrokerPutNums() {
		statsTable.get(BROKER_PUT_NUMS).getAndCreateStatsItem(clusterName).getValue().add(1);
	}

	public void incBrokerPutNums(final String topic, final int incValue) {
		statsTable.get(BROKER_PUT_NUMS).getAndCreateStatsItem(clusterName).getValue().add(incValue);
		incBrokerPutNumsWithoutSystemTopic(topic, incValue);
	}

	public void incBrokerGetNums(final String topic, final int incValue) {
		statsTable.get(BROKER_GET_NUMS).getAndCreateStatsItem(clusterName).getValue().add(incValue);
		incBrokerGetNumsWithoutSystemTopic(topic, incValue);
	}

	public void incBrokerAckNums(final int incValue) {
		statsTable.get(BROKER_ACK_NUMS).getAndCreateStatsItem(clusterName).getValue().add(incValue);
	}

	public void incBrokerCkNums(final int incValue) {
		statsTable.get(BROKER_CK_NUMS).getAndCreateStatsItem(clusterName).getValue().add(incValue);
	}

	public void incBrokerGetNumsWithoutSystemTopic(final String topic, final int incValue) {
		if (TopicValidator.isSystemTopic(topic)) {
			return;
		}
		statsTable.get(BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC).getAndCreateStatsItem(clusterName).getValue().add(incValue);
	}

	public void incBrokerPutNumsWithoutSystemTopic(final String topic, final int incValue) {
		if (TopicValidator.isSystemTopic(topic)) {
			return;
		}
		statsTable.get(BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC).getAndCreateStatsItem(clusterName).getValue().add(incValue);
	}

	public long getBrokerGetNumsWithoutSystemTopic() {
		StatsItemSet statsItemSet = statsTable.get(BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC);
		if (statsItemSet == null) {
			return 0;
		}

		StatsItem statsItem = statsItemSet.getStatsItem(clusterName);
		if (statsItem == null) {
			return 0;
		}

		return statsItem.getValue().longValue();
	}

	public long getBrokerPutNumsWithoutSystemTopic() {
		StatsItemSet statsItemSet = statsTable.get(BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC);
		if (statsItemSet == null) {
			return 0;
		}
		StatsItem statsItem = statsItemSet.getStatsItem(clusterName);
		if (statsItem == null) {
			return 0;
		}
		return statsItem.getValue().longValue();
	}

	public void incSendBackNums(final String group, final String topic) {
		statsTable.get(SNDBCK_PUT_NUMS).addValue(buildStatsKey(topic, group), 1, 1);
	}

	public double tpsGroupGetNums(final String group, final String topic) {
		return statsTable.get(GROUP_GET_NUMS).getStatsDataInMinute(buildStatsKey(topic, group)).getTps();
	}

	public void recordDiskFallBehindTime(final String group, final String topic, final int queueId, final long fallBehind) {
		momentStatsItemSetFallTime.getAndCreateStatsItem(buildStatsKey(queueId, topic, group)).getValue().set(fallBehind);
	}

	public void recordDiskFallBehindSize(final String group, final String topic, final int queueId, final long fallBehind) {
		momentStatsItemSetFallSize.getAndCreateStatsItem(buildStatsKey(queueId, topic, group)).getValue().set(fallBehind);
	}

	public void incDLQStatValue(final String key, final String owner, final String group, final String topic, final String type, final int incValue) {
		String statsKey = buildCommercialStatsKey(owner, topic, group, type);
		statsTable.get(key).addValue(statsKey, incValue, 1);
	}

	public void incCommercialValue(final String key, final String owner, final String group, final String topic, final String type, final int incValue) {
		String statsKey = buildCommercialStatsKey(owner, topic, group, type);
		statsTable.get(key).addValue(statsKey, incValue, 1);
	}

	public void incAccountValue(final String key, final String accountOwnerParent, final String accountOwnerSelf, final String instanceId, final String group, final String topic, final String msgType, final int incValue) {
		String statsKey = buildAccountStatsKey(accountOwnerParent, accountOwnerSelf, instanceId, topic, group, msgType);
		statsTable.get(key).addValue(statsKey, incValue, 1);
	}

	public String buildStatsKey(String topic, String group) {
		StringBuilder sb;
		if (topic != null && group != null) {
			sb = new StringBuilder(topic.length() + 1 + group.length());
		}
		else {
			sb = new StringBuilder();
		}

		sb.append(topic).append("@").append(group.length());
		return sb.toString();
	}

	public String buildStatsKey(String topic, int queueId) {
		StringBuilder sb;
		if (topic != null) {
			sb = new StringBuilder(topic.length() + 5);
		}
		else {
			sb = new StringBuilder();
		}

		sb.append(topic).append("@").append(queueId);
		return sb.toString();
	}

	public String buildStatsKey(String topic, int queueId, String group) {
		StringBuilder sb;
		if (topic != null && group != null) {
			sb = new StringBuilder(topic.length() + 1 + 5 + group.length());
		}
		else {
			sb = new StringBuilder();
		}

		sb.append(topic).append("@").append(queueId).append("@").append(group);
		return sb.toString();
	}

	public String buildStatsKey(int queueId, String topic, String group) {
		StringBuilder sb;
		if (topic != null && group != null) {
			sb = new StringBuilder(2 + 4 + topic.length() + group.length());
		}
		else {
			sb = new StringBuilder();
		}

		sb.append(queueId).append("@").append(topic).append("@").append(group);
		return sb.toString();
	}

	public String buildCommercialStatsKey(String owner, String topic, String group, String type) {
		return Arrays.asList(owner, topic, group, topic).stream().collect(Collectors.joining("@"));
	}

	public String buildAccountStatsKey(String accountOwnerParent, String accountOwnerSelf, String instanceId, String topic, String group, String type) {
		return Arrays.asList(accountOwnerParent, accountOwnerSelf, instanceId, topic, group, type)
				.stream()
				.collect(Collectors.joining("@"));
	}

	public String[] splitAccountStatKey(final String accountStatKey) {
		return accountStatKey.split("\\|");
	}

	private StatisticsKindMeta createStatisticsKindMeta(String name, String[] itemNames, ScheduledExecutorService executorService, StatisticsItemFormatter formatter, Logger log, long interval) {
		final BrokerConfig brokerConfig = this.brokerConfig;
		StatisticsItemPrinter printer = new StatisticsItemPrinter(formatter, log);
		StatisticsKindMeta kindMeta = new StatisticsKindMeta();
		kindMeta.setName(name);
		kindMeta.setItemNames(itemNames);
		kindMeta.setScheduledPrinter(new StatisticsItemScheduledIncrementPrinter(
				"Stat In One Minute:",
				printer,
				executorService,
				() -> Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()),
				interval,
				new String[]{MSG_NUM},
				new StatisticsItemScheduledPrinter.Valve() {
					@Override
					public boolean enabled() {
						return brokerConfig != null ? brokerConfig.isAccountStatsEnable() : true;
					}

					@Override
					public boolean printZeroLine() {
						return brokerConfig != null ? brokerConfig.isAccountStatsPrintZeroValues() : true;
					}
				}
		));

		return kindMeta;
	}

	public interface StateGetter {
		boolean online(String instanceId, String group, String topic);
	}

	public enum StatsType {
		SEND_SUCCESS,
		SEND_FAILURE,

		RCV_SUCCESS,
		RCV_EPOLLS,
		SEND_BACK,
		SEND_BACK_TO_DLQ,

		SEND_ERROR,
		SEND_TIMER,
		SEND_TRANSACTION,

		PERM_FAILURE
	}

}
