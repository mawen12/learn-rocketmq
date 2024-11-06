package com.mawen.learn.rocketmq.store;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

import com.mawen.learn.rocketmq.common.BrokerIdentity;
import com.mawen.learn.rocketmq.common.ServiceThread;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
@Getter
public class StoreStatService extends ServiceThread {

	private static final Logger log = LoggerFactory.getLogger(StoreStatService.class);

	private static final int FREQUENCY_OF_SAMPLING = 1000;
	private static final int MAX_RECORDS_OF_SAMPLING = 10 * 60;
	private static final String[] PUT_MESSAGE_ENTIRE_TIME_MAX_DESC = new String[]{
			"[<=0ms]", "[0~10ms]", "[10~50ms]", "[50~100ms]", "[100~200ms]", "[200~500ms]", "[500ms~1s]", "[1~2s]", "[2~3s]", "[3~4s]", "[4~5s]", "[5~10s]", "[10s~]",
	};
	private static final Map<Integer, Integer> PUT_MESSAGE_ENTIRE_TIME_BUCKETS = new TreeMap<>();
	private static int printTPSInterval = 1 * 60;

	private TreeMap<Long, LongAdder> buckets = new TreeMap<>();
	private Map<Long, LongAdder> lastBuckets = new TreeMap<>();

	private final LongAdder putMessageFailedTimes = new LongAdder();

	private final ConcurrentMap<String, LongAdder> putMessageTopicTimesTotal = new ConcurrentHashMap<>(128);
	private final ConcurrentMap<String, LongAdder> putMessageTopicSizeTotal = new ConcurrentHashMap<>(128);

	private final LongAdder getMessageTimesTotalFound = new LongAdder();
	private final LongAdder getMessageTransferredMsgCount = new LongAdder();
	private final LongAdder getMessageTimesTotalMiss = new LongAdder();
	private final LinkedList<CallSnapshot> putTimesList = new LinkedList<>();

	private final LinkedList<CallSnapshot> getTimesFoundList = new LinkedList<>();
	private final LinkedList<CallSnapshot> getTimesMissList = new LinkedList<>();
	private final LinkedList<CallSnapshot> transferredMsgCountList = new LinkedList<>();
	private volatile LongAdder[] putMessageDistributeTime;
	private volatile LongAdder[] lastPutMessageDistributeTime;
	private long messageStoreBootTimestamp = System.currentTimeMillis();
	private volatile long putMessageEntireTimeMax = 0;
	private volatile long getMessageEntireTimeMax = 0;

	private ReentrantLock putLock = new ReentrantLock();
	private ReentrantLock getLock = new ReentrantLock();

	private volatile long dispatchMaxBuffer = 0;

	private ReentrantLock samplingLock = new ReentrantLock();

	private long lastPrintTimestamp = System.currentTimeMillis();

	private BrokerIdentity brokerIdentity;

	public StoreStatService(BrokerIdentity brokerIdentity) {
		this();
		this.brokerIdentity = brokerIdentity;
	}

	public StoreStatService() {
		PUT_MESSAGE_ENTIRE_TIME_BUCKETS.put(1, 20);
		PUT_MESSAGE_ENTIRE_TIME_BUCKETS.put(2, 15);
		PUT_MESSAGE_ENTIRE_TIME_BUCKETS.put(5, 10);
		PUT_MESSAGE_ENTIRE_TIME_BUCKETS.put(10, 10);
		PUT_MESSAGE_ENTIRE_TIME_BUCKETS.put(50, 6);
		PUT_MESSAGE_ENTIRE_TIME_BUCKETS.put(100, 5);
		PUT_MESSAGE_ENTIRE_TIME_BUCKETS.put(1000, 9);

		resetPutMessageTimeBuckets();
		resetPutMessageDistributeTime();
	}

	private void resetPutMessageTimeBuckets() {
		TreeMap<Long, LongAdder> nextBuckets = new TreeMap<>();
		AtomicLong index = new AtomicLong(0);
		PUT_MESSAGE_ENTIRE_TIME_BUCKETS.forEach((interval, times) -> {
			for (int i = 0; i < times; i++) {
				nextBuckets.put(index.addAndGet(interval), new LongAdder());
			}
		});
		nextBuckets.put(Long.MAX_VALUE, new LongAdder());

		lastBuckets = buckets;
		buckets = nextBuckets;
	}

	private LongAdder[] resetPutMessageDistributeTime() {
		LongAdder[] next = new LongAdder[13];
		for (int i = 0; i < next.length; i++) {
			next[i] = new LongAdder();
		}

		lastPutMessageDistributeTime = putMessageDistributeTime;
		putMessageDistributeTime = next;
		return lastPutMessageDistributeTime;
	}

	public void incPutMessageEntireTime(long value) {
		Map.Entry<Long, LongAdder> targetBucket = buckets.ceilingEntry(value);
		if (targetBucket != null) {
			targetBucket.getValue().add(1);
		}
	}

	public double findPutMessageEntireTimePX(double px) {
		Map<Long, LongAdder> lastBuckets = this.lastBuckets;
		long start = System.currentTimeMillis();

		double result = 0.0;
		long totalResult = lastBuckets.values().stream().mapToLong(LongAdder::longValue).sum();
		long pxIndex = (long) (totalResult * px);
		long passCount = 0;
		List<Long> bucketValue = new ArrayList<>(lastBuckets.keySet());
		for (int i = 0; i < bucketValue.size(); i++) {
			long count = lastBuckets.get(bucketValue.get(i)).longValue();
			if (pxIndex <= passCount + count) {
				long relativeIndex = pxIndex - passCount;
				if (i == 0) {
					result = count == 0 ? 0 : bucketValue.get(i) * relativeIndex / (double) count;
				}
				else {
					long lastBucket = bucketValue.get(i - 1);
					result = lastBucket + (count == 0 ? 0 : (bucketValue.get(i) - lastBucket) * relativeIndex / (double) count);
				}
				break;
			}
			else {
				passCount += count;
			}
		}
		log.info("findPutMessageEntireTimePX {}={}ms cost {}ms", px, String.format("%.2f", result), System.currentTimeMillis() - start);
		return result;
	}

	public void setPutMessageEntireTimeMax(long value) {
		incPutMessageEntireTime(value);
		LongAdder[] times = putMessageDistributeTime;

		if (times == null) {
			return;
		}

		if (value <= 0) {
			times[0].add(1);
		}
		else if (value < 10) {
			times[1].add(1);
		}
		else if (value < 50) {
			times[2].add(1);
		}
		else if (value < 100) {
			times[3].add(1);
		}
		else if (value < 200) {
			times[4].add(1);
		}
		else if (value < 500) {
			times[5].add(1);
		}
		else if (value < 1000) {
			times[6].add(1);
		}

		else if (value < 2000) {
			times[7].add(1);
		}
		else if (value < 3000) {
			times[8].add(1);
		}
		else if (value < 4000) {
			times[9].add(1);
		}
		else if (value < 5000) {
			times[10].add(1);
		}
		else if (value < 10000) {
			times[11].add(1);
		}
		else {
			times[12].add(1);
		}

		if (value > putMessageEntireTimeMax) {
			putLock.lock();
			putMessageEntireTimeMax = Math.max(value, putMessageEntireTimeMax);
			putLock.unlock();
		}
	}

	public void setGetMessageEntireTimeMax(long value) {
		if (value > getMessageEntireTimeMax) {
			getLock.lock();
			getMessageEntireTimeMax = Math.max(value, getMessageEntireTimeMax);
			getLock.unlock();
		}
	}

	public void setDispatchMaxBuffer(long value) {
		dispatchMaxBuffer = Math.max(value, dispatchMaxBuffer);
	}

	@Override
	public String getServiceName() {
		if (brokerIdentity != null && brokerIdentity.isInBrokerContainer()) {
			return brokerIdentity.getIdentifier() + StoreStatService.class.getSimpleName();
		}
		return StoreStatService.class.getSimpleName();
	}

	@Override
	public void run() {
		String serviceName = getServiceName();
		log.info("{} service started.", serviceName);

		while (!isStopped()) {
			try {
				waitForRunning(FREQUENCY_OF_SAMPLING);

				sampling();

				printTps();
			}
			catch (Exception e) {
				log.warn("{} service has exception.", serviceName, e);
			}
		}

		log.info("{} service end.", serviceName);
	}

	private void sampling() {
		samplingLock.lock();

		try {
			putTimesList.add(new CallSnapshot(System.currentTimeMillis(), getPutMessageTimesTotal()));
			if (putTimesList.size() > MAX_RECORDS_OF_SAMPLING + 1) {
				putTimesList.removeFirst();
			}

			getTimesFoundList.add(new CallSnapshot(System.currentTimeMillis(), getMessageTimesTotalFound.longValue()));
			if (getTimesFoundList.size() > MAX_RECORDS_OF_SAMPLING + 1) {
				getTimesFoundList.removeFirst();
			}

			getTimesMissList.add(new CallSnapshot(System.currentTimeMillis(), getMessageTimesTotalMiss.longValue()));
			if (getTimesMissList.size() > MAX_RECORDS_OF_SAMPLING + 1) {
				getTimesMissList.removeFirst();
			}

			transferredMsgCountList.add(new CallSnapshot(System.currentTimeMillis(), getMessageTransferredMsgCount.longValue()));
			if (transferredMsgCountList.size() > MAX_RECORDS_OF_SAMPLING + 1) {
				transferredMsgCountList.removeFirst();
			}
		}
		finally {
			samplingLock.unlock();
		}
	}

	private void printTps() {
		if (System.currentTimeMillis() > lastPrintTimestamp + printTPSInterval * 1000) {
			lastPrintTimestamp = System.currentTimeMillis();

			log.info("[STORETPS] put_tps {} get_found_tps {} get_miss_tps {} get_transferred_tps {}",
					getPutTps(printTPSInterval), getGetFoundTps(printTPSInterval), getGetMissTps(printTPSInterval), getGetTransferredTps(printTPSInterval));

			LongAdder[] times = resetPutMessageDistributeTime();
			if (times == null) {
				return;
			}

			StringBuilder sb = new StringBuilder();
			long totalPut = 0;
			for (int i = 0; i < times.length; i++) {
				long value = times[i].longValue();
				totalPut += value;
				sb.append(String.format("%s:%d", PUT_MESSAGE_ENTIRE_TIME_MAX_DESC[i], value));
				sb.append(" ");
			}
			resetPutMessageTimeBuckets();
			findPutMessageEntireTimePX(0.99);
			findPutMessageEntireTimePX(0.999);
			log.info("[PAGECACHERT] TotalPut {} PutMessageDistributeTime {}", totalPut, sb.toString());
		}
	}

	public long getPutMessageTimesTotal() {
		return putMessageTopicTimesTotal
				.values()
				.parallelStream()
				.mapToLong(LongAdder::longValue)
				.sum();
	}

	private String getFormatRuntime() {
		final long millisecond = 1;
		final long second = 1000 * millisecond;
		final long minute = 60 * second;
		final long hour = 60 * minute;
		final long day = 24 * hour;
		final MessageFormat messageFormat = new MessageFormat("[ {0} days, {1} hours, {2} minutes, {3} seconds ]");

		long time = System.currentTimeMillis() - this.messageStoreBootTimestamp;
		long days = time / day;
		long hours = (time % day) / hour;
		long minutes = (time % hour) / minute;
		long seconds = (time % minute) / second;
		return messageFormat.format(new Long[] {days, hours, minutes, seconds});
	}

	public long getPutMessageSizeTotal() {
		return putMessageTopicSizeTotal
				.values()
				.parallelStream()
				.mapToLong(LongAdder::longValue)
				.sum();
	}

	private String getPutMessageDistributeTimeStringInfo(Long total) {
		return putMEssageDistributeTimeToString();
	}

	private String getPutTps() {
		return new StringBuilder()
				.append(getPutTps(10))
				.append(" ")
				.append(getPutTps(60))
				.append(" ")
				.append(getPutTps(600))
				.toString();
	}

	private String getGetFoundTps() {
		return new StringBuilder()
				.append(getGetFoundTps(10))
				.append(" ")
				.append(getGetFoundTps(60))
				.append(" ")
				.append(getGetFoundTps(600))
				.toString();
	}

	private String getGetMissTps() {
		return new StringBuilder()
				.append(getGetMissTps(10))
				.append(" ")
				.append(getGetMissTps(60))
				.append(" ")
				.append(getGetMissTps(600))
				.toString();
	}

	private String getGetTotalTps() {
		return new StringBuilder()
				.append(getGetTotalTps(10))
				.append(" ")
				.append(getGetTotalTps(60))
				.append(" ")
				.append(getGetTotalTps(600))
				.toString();
	}

	private String getGetTransferredTps() {
		return new StringBuilder()
				.append(getGetTransferredTps(10))
				.append(" ")
				.append(getGetTransferredTps(60))
				.append(" ")
				.append(getGetTransferredTps(600))
				.toString();
	}

	private String putMEssageDistributeTimeToString() {
		if (lastPutMessageDistributeTime == null) {
			return null;
		}

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < lastPutMessageDistributeTime.length; i++) {
			long value = lastPutMessageDistributeTime[i].longValue();
			sb.append(String.format("%s:%d", PUT_MESSAGE_ENTIRE_TIME_MAX_DESC[i], value));
			sb.append(" ");
		}

		return sb.toString();
	}

	private String getPutTps(int time) {
		String result = "";
		samplingLock.lock();

		try {
			CallSnapshot last = putTimesList.getLast();

			if (putTimesList.size() > time) {
				CallSnapshot lastBefore = putTimesList.get(putTimesList.size() - (time + 1));
				result += CallSnapshot.getTPS(lastBefore, last);
			}
		}
		finally {
			samplingLock.unlock();
		}

		return result;
	}

	private String getGetFoundTps(int time) {
		String result = "";
		samplingLock.lock();

		try {
			CallSnapshot last = getTimesFoundList.getLast();

			if (getTimesFoundList.size() > time) {
				CallSnapshot lastBefore = getTimesFoundList.get(getTimesFoundList.size() - (time + 1));
				result += CallSnapshot.getTPS(lastBefore, last);
			}
		}
		finally {
			samplingLock.unlock();
		}
		return result;
	}

	private String getGetMissTps(int time) {
		String result = "";
		samplingLock.lock();

		try {
			CallSnapshot last = getTimesMissList.getLast();

			if (getTimesMissList.size() > time) {
				CallSnapshot lastBefore = getTimesMissList.get(getTimesMissList.size() - (time + 1));
				result += CallSnapshot.getTPS(lastBefore, last);
			}
		}
		finally {
			samplingLock.unlock();
		}
		return result;
	}

	private String getGetTotalTps(int time) {
		samplingLock.lock();
		double found = 0, miss = 0;

		try {
			CallSnapshot last = getTimesFoundList.getLast();
			if (getTimesFoundList.size() > time) {
				CallSnapshot lastBefore = getTimesFoundList.get(getTimesFoundList.size() - (time + 1));
				found = CallSnapshot.getTPS(lastBefore, last);
			}

			last = getTimesMissList.getLast();
			if (getTimesMissList.size() > time) {
				CallSnapshot lastBefore = getTimesMissList.get(getTimesMissList.size() - (time + 1));
				miss = CallSnapshot.getTPS(lastBefore, last);
			}
		}
		finally {
			samplingLock.unlock();
		}
		return Double.toString(found + miss);
	}

	private String getGetTransferredTps(int time) {
		String result = "";
		samplingLock.lock();

		try {
			CallSnapshot last = transferredMsgCountList.getLast();

			if (transferredMsgCountList.size() > time) {
				CallSnapshot lastBefore = transferredMsgCountList.get(transferredMsgCountList.size() - (time + 1));
				result += CallSnapshot.getTPS(lastBefore, last);
			}
		}
		finally {
			samplingLock.unlock();
		}
		return result;
	}

	public HashMap<String, String> getRuntimeInfo() {
		HashMap<String, String> result = new HashMap<>(64);

		Long totalTimes = getPutMessageTimesTotal();
		if (totalTimes == 0) {
			totalTimes = 1L;
		}

		result.put("bootTimestamp", String.valueOf(messageStoreBootTimestamp));
		result.put("runtime", getFormatRuntime());

		result.put("putMessageEntireTimeMax", String.valueOf(putMessageEntireTimeMax));
		result.put("putMessageTimesTotal", String.valueOf(totalTimes));
		result.put("putMessageFailedTimes", String.valueOf(putMessageFailedTimes));
		result.put("putMessageSizeTotal", String.valueOf(getPutMessageSizeTotal()));
		result.put("putMessageDistributionTime", String.valueOf(getPutMessageDistributeTimeStringInfo(totalTimes)));
		result.put("putMessageAverageSize", String.valueOf(getPutMessageSizeTotal() / totalTimes.doubleValue()));

		result.put("dispatchMaxBuffer", String.valueOf(dispatchMaxBuffer));
		result.put("getMessageEntireTimeMax", String.valueOf(getMessageEntireTimeMax));
		result.put("putTps", getPutTps());
		result.put("getFoundTps", getGetFoundTps());
		result.put("getMissTps", getGetMissTps());
		result.put("getTotalTps", getGetTotalTps());
		result.put("getTransferredTps", getGetTransferredTps());
		result.put("putLatency99", String.format("%.2f", findPutMessageEntireTimePX(0.99)));
		result.put("putLatency999", String.format("%.2f", findPutMessageEntireTimePX(0.999)));

		return result;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(1024);
		Long totalTimes = getPutMessageTimesTotal();
		if (0 == totalTimes) {
			totalTimes = 1L;
		}

		sb.append("\truntime: " + this.getFormatRuntime() + "\r\n");
		sb.append("\tputMessageEntireTimeMax: " + this.putMessageEntireTimeMax + "\r\n");
		sb.append("\tputMessageTimesTotal: " + totalTimes + "\r\n");
		sb.append("\tgetPutMessageFailedTimes: " + this.getPutMessageFailedTimes() + "\r\n");
		sb.append("\tputMessageSizeTotal: " + this.getPutMessageSizeTotal() + "\r\n");
		sb.append("\tputMessageDistributeTime: " + this.getPutMessageDistributeTimeStringInfo(totalTimes)
				+ "\r\n");
		sb.append("\tputMessageAverageSize: " + (this.getPutMessageSizeTotal() / totalTimes.doubleValue())
				+ "\r\n");
		sb.append("\tdispatchMaxBuffer: " + this.dispatchMaxBuffer + "\r\n");
		sb.append("\tgetMessageEntireTimeMax: " + this.getMessageEntireTimeMax + "\r\n");
		sb.append("\tputTps: " + this.getPutTps() + "\r\n");
		sb.append("\tgetFoundTps: " + this.getGetFoundTps() + "\r\n");
		sb.append("\tgetMissTps: " + this.getGetMissTps() + "\r\n");
		sb.append("\tgetTotalTps: " + this.getGetTotalTps() + "\r\n");
		sb.append("\tgetTransferredTps: " + this.getGetTransferredTps() + "\r\n");
		return sb.toString();
	}

	@AllArgsConstructor
	static class CallSnapshot {
		public final long timestamp;
		public final long callTimesTotal;

		public static double getTPS(final CallSnapshot begin, final CallSnapshot end) {
			long total = end.callTimesTotal - begin.callTimesTotal;
			Long time = end.timestamp - begin.timestamp;

			double tps = total / time.doubleValue();

			return tps * 1000;
		}
	}
}
