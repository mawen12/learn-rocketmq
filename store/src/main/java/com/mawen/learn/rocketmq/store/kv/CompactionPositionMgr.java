package com.mawen.learn.rocketmq.store.kv;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

import com.mawen.learn.rocketmq.common.ConfigManager;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
@Getter
@Setter
public class CompactionPositionMgr extends ConfigManager {
	public static final String CHECKPOINT_FILE = "position-checkpoint";

	private transient String compactionPath;
	private transient String checkpointFileName;

	private ConcurrentHashMap<String, Long> queueOffsetMap = new ConcurrentHashMap<>();

	public CompactionPositionMgr(String compactionPath) {
		this.compactionPath = compactionPath;
		this.checkpointFileName = compactionPath + File.separator + CHECKPOINT_FILE;
		this.load();
	}

	public void setOffset(String topic, int queueId, long offset) {
		queueOffsetMap.put(topic + "_" + queueId, offset);
	}

	public long getOffset(String topic, int queueId) {
		return queueOffsetMap.getOrDefault(topic + "_" + queueId, -1L);
	}

	public boolean isEmpty() {
		return queueOffsetMap.isEmpty();
	}

	public boolean isCompaction(String topic, int queueId, long offset) {
		return getOffset(topic, queueId) > offset;
	}

	@Override
	public String configFilePath() {
		return checkpointFileName;
	}

	@Override
	public String encode() {
		return this.encode(false);
	}

	@Override
	public String encode(boolean prettyFormat) {
		return RemotingSerializable.toJson(this, prettyFormat);
	}

	@Override
	public void decode(String jsonString) {
		if (jsonString != null) {
			CompactionPositionMgr obj = RemotingSerializable.fromJson(jsonString, CompactionPositionMgr.class);
			if (obj != null) {
				queueOffsetMap = obj.queueOffsetMap;
			}
		}
	}
}
