package com.mawen.learn.rocketmq.store.timer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.io.Files;
import com.mawen.learn.rocketmq.common.ConfigManager;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.topic.TopicValidator;
import com.mawen.learn.rocketmq.remoting.protocol.DataVersion;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/16
 */
@Getter
@RequiredArgsConstructor
public class TimerMetrics extends ConfigManager {

	private static final Logger log = LoggerFactory.getLogger(TimerMetrics.class);
	private static final long LOCK_TIMEOUT_MILLIS = 3000;
	private transient final Lock lock = new ReentrantLock();

	private final ConcurrentMap<String, Metric> timingCount = new ConcurrentHashMap<>(1024);
	private final ConcurrentMap<Integer, Metric> timingDistribution = new ConcurrentHashMap<>(1024);

	public List<Integer> timerDist = Arrays.asList(5, 60, 300, 900, 3600, 14400, 28800, 86400);

	private final DataVersion dataVersion = new DataVersion();

	private final String configPath;

	public long updateDistPair(int period, int value) {
		Metric distPair = getDistPair(period);
		return distPair.getCount().addAndGet(value);
	}

	public long addAndGet(MessageExt msg, int value) {
		String topic = msg.getProperty(MessageConst.PROPERTY_REAL_TOPIC);
		Metric pair = getTopicPair(topic);
		getDataVersion().nextVersion();
		pair.setTimeStamp(System.currentTimeMillis());
		return pair.getCount().addAndGet(value);
	}

	public Metric getDistPair(Integer period) {
		return timingDistribution.computeIfAbsent(period, k -> new Metric());
	}

	public Metric getTopicPair(String topic) {
		return timingCount.computeIfAbsent(topic, k -> new Metric());
	}

	public long getTimingCount(String topic) {
		Metric pair = timingCount.get(topic);
		return pair != null ? pair.getCount().get() : 0;
	}

	protected void write0(Writer writer) {
		TimerMetricsSerializeWrapper wrapper = new TimerMetricsSerializeWrapper();
		wrapper.setTimingCount(timingCount);
		wrapper.setDataVersion(dataVersion);
		JSON.writeJSONString(writer, wrapper, SerializerFeature.BrowserCompatible);
	}

	@Override
	public String encode() {
		return encode(false);
	}

	@Override
	public String encode(boolean prettyFormat) {
		TimerMetricsSerializeWrapper wrapper = new TimerMetricsSerializeWrapper();
		wrapper.setDataVersion(dataVersion);
		wrapper.setTimingCount(timingCount);
		return wrapper.toJson(prettyFormat);
	}

	@Override
	public String configFilePath() {
		return configPath;
	}

	@Override
	public void decode(String jsonString) {
		if (jsonString != null) {
			TimerMetricsSerializeWrapper wrapper = TimerMetricsSerializeWrapper.fromJson(jsonString, TimerMetricsSerializeWrapper.class);
			if (wrapper != null) {
				timingCount.putAll(wrapper.getTimingCount());
				dataVersion.assignNewOne(wrapper.getDataVersion());
			}
		}
	}

	public void cleanMetrics(Set<String> topics) {
		if (topics == null || topics.isEmpty()) {
			return;
		}

		Iterator<Map.Entry<String, Metric>> iterator = timingCount.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, Metric> entry = iterator.next();
			String topic = entry.getKey();
			if (topic.startsWith(TopicValidator.SYSTEM_TOPIC_PREFIX)) {
				continue;
			}
			if (topics.contains(topic)) {
				continue;
			}

			iterator.remove();
			log.info("clean timer metrics, because not in topic config, {}", topic);
		}
	}

	@Override
	public synchronized void persist() {
		String config = configFilePath();
		String tmp = config + ".tmp";
		String backup = config + ".bak";
		BufferedWriter bufferedWriter = null;

		try {
			File tmpFile = new File(tmp);
			File parentDir = tmpFile.getParentFile();
			if (!parentDir.exists()) {
				if (!parentDir.mkdirs()) {
					log.error("Failed to create directory: {}", parentDir.getCanonicalPath());
					return;
				}
			}

			if (!tmpFile.exists()) {
				if (!tmpFile.createNewFile()) {
					log.error("Failed to create file: {}", tmpFile.getCanonicalPath());
					return;
				}
			}

			bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpFile, false), StandardCharsets.UTF_8));
			write0(bufferedWriter);
			bufferedWriter.flush();
			bufferedWriter.close();

			if (log.isDebugEnabled()) {
				log.debug("Finished writing tmp file: {}", tmp);
			}

			File configFile = new File(config);
			if (configFile.exists()) {
				Files.copy(configFile, new File(backup));
				configFile.delete();
			}

			tmpFile.renameTo(configFile);
		}
		catch (IOException e) {
			log.error("Failed to persist {}", tmp, e);
		}
		finally {
			if (bufferedWriter != null) {
				try {
					bufferedWriter.close();
				}
				catch (IOException ignored) {
				}
			}
		}
	}

	@Setter
	@Getter
	public static class TimerMetricsSerializeWrapper extends RemotingSerializable {
		private ConcurrentMap<String, Metric> timingCount = new ConcurrentHashMap<>(1024);
		private DataVersion dataVersion = new DataVersion();
	}

	@Getter
	@Setter
	public static class Metric {
		private AtomicLong count;
		private long timeStamp;

		public Metric() {
			count = new AtomicLong(0);
			timeStamp = System.currentTimeMillis();
		}

		@Override
		public String toString() {
			return "Metric{" +
			       "count=" + count.get() +
			       ", timeStamp=" + timeStamp +
			       '}';
		}
	}
}
