package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import com.mawen.learn.rocketmq.remoting.protocol.admin.ConsumeStats;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class ConsumerRunningInfo extends RemotingSerializable {

	public static final String PROP_NAMESERVER_ADDR = "PROP_NAMESERVER_ADDR";

	public static final String PROP_THREADPOOL_CORE_SIZE = "PROP_THEADPOOL_CODE_SIZE";

	public static final String PROP_CONSUME_ORDERLY = "PROP_CONSUME_ORDERLY";

	public static final String PROP_CONSUME_TYPE = "PROP_CONSUME_TYPE";

	public static final String PROP_CLIENT_VERSION = "PROP_CLIENT_VERSION";

	public static final String PROP_CONSUMER_START_TIMESTAMP = "PROP_CONSUMER_START_TIMESTAMP";

	private Properties properties = new Properties();

	private TreeSet<SubscriptionData> subscriptionSet = new TreeSet<>();

	private TreeMap<MessageQueue, ProcessQueueInfo> mqTable = new TreeMap<>();

	private TreeMap<MessageQueue, PopProcessQueueInfo> mqPopTable = new TreeMap<>();

	private TreeMap<String, ConsumeStats> statusTable = new TreeMap<>();

	private TreeMap<String, String> userConsumeInfo = new TreeMap<>();

	private String jstack;

	public static boolean analyzeSubscription(TreeMap<String, ConsumerRunningInfo> criTable) {
		ConsumerRunningInfo prev = criTable.firstEntry().getValue();
		boolean push = isPushType(prev);
		boolean startForWhile = false;

		String property = prev.getProperties().getProperty(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP);
		if (property == null) {
			property = String.valueOf(prev.getProperties().get(ConsumerRunningInfo.PROP_CONSUME_ORDERLY));
		}
		startForWhile = (System.currentTimeMillis() - Long.parseLong(property)) > (2 * 60 * 1000);

		if (push && startForWhile) {
			Iterator<Map.Entry<String, ConsumerRunningInfo>> it = criTable.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, ConsumerRunningInfo> next = it.next();
				ConsumerRunningInfo current = next.getValue();

				boolean equals = current.getSubscriptionSet().equals(prev.getSubscriptionSet());
				if (!equals) {
					return false;
				}

				prev = next.getValue();
			}
		}

		return true;
	}

	public static boolean isPushType(ConsumerRunningInfo consumerRunningInfo) {
		String property = consumerRunningInfo.getProperties().getProperty(ConsumerRunningInfo.PROP_CONSUME_TYPE);

		if (property == null) {
			property = ((ConsumeType) consumerRunningInfo.getProperties().get(ConsumerRunningInfo.PROP_CONSUME_TYPE)).name();
		}
		return ConsumeType.valueOf(property) == ConsumeType.CONSUME_PASSIVELY;
	}

	public static boolean analyzeRebalance(TreeMap<String, ConsumerRunningInfo> criTable) {
		return true;
	}

	public static String analyzeProcessQueue(String clientId, ConsumerRunningInfo info) {
		StringBuilder sb = new StringBuilder();
		boolean push = false;

		String property = info.getProperties().getProperty(ConsumerRunningInfo.PROP_CONSUME_TYPE);
		if (property == null) {
			property = ((ConsumeType) info.getProperties().get(ConsumerRunningInfo.PROP_CONSUME_TYPE)).name();
		}
		push = ConsumeType.valueOf(property) == ConsumeType.CONSUME_PASSIVELY;

		property = info.getProperties().getProperty(ConsumerRunningInfo.PROP_CONSUME_ORDERLY);
		boolean orderMsg = Boolean.parseBoolean(property);

		if (push) {
			Iterator<Map.Entry<MessageQueue, ProcessQueueInfo>> it = info.getMqTable().entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<MessageQueue, ProcessQueueInfo> next = it.next();
				MessageQueue mq = next.getKey();
				ProcessQueueInfo pq = next.getValue();

				if (orderMsg) {
					if (!pq.isLocked()) {
						sb.append(String.format("%s %s can't lock for a while, %dms%n", clientId, mq, System.currentTimeMillis() - pq.getLastLockTimestamp()));
					}
					else {
						if (pq.isDroped() && pq.getTryUnlockTimes() > 0) {
							sb.append(String.format("%s %s unlock %d times, still failed%n", clientId, mq, pq.getTryUnlockTimes()));
						}
					}
				}
				else {
					long diff = System.currentTimeMillis() - pq.getLastConsumeTimestamp();

					if (diff > (60 * 1000) && pq.getCachedMsgCount() > 0) {
						sb.append(String.format("%s %s can't consume for a while, maybe blocked, %dms%n", clientId, mq, diff));
					}
				}
			}
		}

		return sb.toString();
	}


	public String formatString() {
		StringBuilder sb = new StringBuilder();

		sb.append("#Consumer properties#\n");
		for (Map.Entry<Object, Object> entry : this.properties.entrySet()) {
			String item = String.format("%-40s: %s%n", entry.getKey().toString(), entry.getValue().toString());
			sb.append(item);
		}

		sb.append("\n\n#Consumer Subscription#\n");
		int i = 0;
		Iterator<SubscriptionData> iter = this.subscriptionSet.iterator();
		while (iter.hasNext()) {
			SubscriptionData next = iter.next();
			String item = String.format("%03d Topic: %-40s ClassFilter: %-8s SubExpression: %s%n", ++i, next.getTopic(), next.isClassFilterMode(), next.getSubString());
			sb.append(item);
		}

		sb.append("\n\n#Consumer Offset#\n");
		sb.append(String.format("%-64s  %-32s  %-4s  %-20s%n",
				"#Topic",
				"#Broker Name",
				"#QID",
				"#Consumer Offset"
		));

		for (Map.Entry<MessageQueue, ProcessQueueInfo> entry : this.mqTable.entrySet()) {
			String item = String.format("%-64s  %-32s  %4d %-20s%n",
					entry.getKey().getTopic(),
					entry.getKey().getBrokerName(),
					entry.getKey().getQueueId(),
					entry.getValue().getCommitOffset());
			sb.append(item);
		}

		sb.append("\n\n#Consumer MQ Detail#\n");
		sb.append(String.format("%-64s  %-32s  %-4s  %-20s%n",
				"#Topic",
				"#Broker Name",
				"#QID",
				"#ProcessQueueInfo"
		));

		for (Map.Entry<MessageQueue, ProcessQueueInfo> entry : this.mqTable.entrySet()) {
			String item = String.format("%-64s  %-32s  %4d %-20s%n",
					entry.getKey().getTopic(),
					entry.getKey().getBrokerName(),
					entry.getKey().getQueueId(),
					entry.getValue().toString());
			sb.append(item);
		}

		sb.append("\n\n#Consumer Pop Detail#\n");
		sb.append(String.format("%-32s  %-32s  %-4s  %-20s\n",
				"#Topic",
				"#Broker Name",
				"#QID",
				"#ProcessQueueInfo"
		));

		for (Map.Entry<MessageQueue, PopProcessQueueInfo> entry : this.mqPopTable.entrySet()) {
			String item = String.format("%-32s  %-32s  %-4d  %s%n",
					entry.getKey().getTopic(),
					entry.getKey().getBrokerName(),
					entry.getKey().getQueueId(),
					entry.getValue().toString());
			sb.append(item);
		}


		sb.append("\n\n#Consumer RT&TPS#\n");
		sb.append(String.format("%-64s  %14s %14s %14s %14s %18s %25s%n",
				"#Topic",
				"#Pull RT",
				"#Pull TPS",
				"#Consume RT",
				"#ConsumeOK TPS",
				"#ConsumeFailed TPS",
				"#ConsumeFailedMsgsInHour"
		));

		for (Map.Entry<String, ConsumeStats> entry : this.statusTable.entrySet()) {
			String item = String.format("%-32s  %14.2f %14.2f %14.2f %14.2f %18.2f %25d%n",
					entry.getKey(),
					entry.getValue().getPullRT(),
					entry.getValue().getPullTPS(),
					entry.getValue().getConsumeRT(),
					entry.getValue().getConsumeOKTPS(),
					entry.getValue().getConsumeFailedTPS(),
					entry.getValue().getConsumeFailedMsgs());
			sb.append(item);
		}

		if (this.userConsumeInfo != null) {
			sb.append("\n\n#User Consume Info#\n");
			for (Map.Entry<String, String> entry : this.userConsumeInfo.entrySet()) {
				String item = String.format("%-40s: %s%n", entry.getKey(), entry.getValue());
				sb.append(item);
			}
		}

		if (this.jstack != null) {
			sb.append("\n\n#Consumer jstack#\n");
			sb.append(this.jstack);
		}

		return sb.toString();
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public TreeSet<SubscriptionData> getSubscriptionSet() {
		return subscriptionSet;
	}

	public void setSubscriptionSet(TreeSet<SubscriptionData> subscriptionSet) {
		this.subscriptionSet = subscriptionSet;
	}

	public TreeMap<MessageQueue,ProcessQueueInfo> getMqTable() {
		return mqTable;
	}

	public void setMqTable(TreeMap<MessageQueue,ProcessQueueInfo> mqTable) {
		this.mqTable = mqTable;
	}

	public TreeMap<MessageQueue,PopProcessQueueInfo> getMqPopTable() {
		return mqPopTable;
	}

	public void setMqPopTable(TreeMap<MessageQueue,PopProcessQueueInfo> mqPopTable) {
		this.mqPopTable = mqPopTable;
	}

	public TreeMap<String, ConsumeStats> getStatusTable() {
		return statusTable;
	}

	public void setStatusTable(TreeMap<String, ConsumeStats> statusTable) {
		this.statusTable = statusTable;
	}

	public TreeMap<String, String> getUserConsumeInfo() {
		return userConsumeInfo;
	}

	public void setUserConsumeInfo(TreeMap<String, String> userConsumeInfo) {
		this.userConsumeInfo = userConsumeInfo;
	}

	public String getJstack() {
		return jstack;
	}

	public void setJstack(String jstack) {
		this.jstack = jstack;
	}
}
