package com.mawen.learn.rocketmq.store.kv;

import java.util.Objects;
import java.util.Optional;

import com.mawen.learn.rocketmq.common.TopicConfig;
import com.mawen.learn.rocketmq.common.attribute.CleanupPolicy;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.utils.CleanupPolicyUtils;
import com.mawen.learn.rocketmq.store.CommitLog;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import com.mawen.learn.rocketmq.store.DispatchRequest;
import com.mawen.learn.rocketmq.store.SelectMappedBufferResult;
import lombok.AllArgsConstructor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
@AllArgsConstructor
public class CompactionService {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	private final CommitLog commitLog;
	private final DefaultMessageStore defaultMessageStore;
	private final CompactionStore compactionStore;

	public void putRequest(DispatchRequest request) {
		if (request == null) {
			return;
		}

		String topic = request.getTopic();
		Optional<TopicConfig> topicConfig = defaultMessageStore.getTopicConfig(topic);
		CleanupPolicy policy = CleanupPolicyUtils.getDeletePolicy(topicConfig);

		if (Objects.equals(policy, CleanupPolicy.COMPACTION)) {
			SelectMappedBufferResult result = null;
			try {
				result = commitLog.getData(request.getCommitLogOffset());
				if (result != null) {
					compactionStore.doDispatch(request, result);
				}
			}
			catch (Exception e) {
				log.error("putMessage into {}:{} compactionLog exception: ", request.getTopic(), request.getQueueId(), e);
			}
			finally {
				if (request != null) {
					result.release();
				}
			}
		}
	}



	public boolean load(boolean exitOK) {
		try {
			compactionStore.load(exitOK);
			return true;
		}
		catch (Exception e) {
			log.error("load compaction store error", e);
			return false;
		}
	}

	public void shutdown() {
		compactionStore.shutdown();
	}

	public void updateMaterAddress(String addr) {
		compactionStore.updateMasterAddress(addr);
	}
}
