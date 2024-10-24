package com.mawen.learn.rocketmq.client.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.mawen.learn.rocketmq.client.ClientConfig;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.client.producer.ProduceAccumulator;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MQClientManager {

	private static final Logger log = LoggerFactory.getLogger(MQClientManager.class);

	private static MQClientManager instance = new MQClientManager();
	private AtomicInteger factoryIndexGenerator = new AtomicInteger();
	private ConcurrentMap<String, MQClientInstance> factoryTable = new ConcurrentHashMap<>();
	private ConcurrentMap<String, ProduceAccumulator> accumulatorTable = new ConcurrentHashMap<>();

	public static MQClientManager getInstance() {
		return instance;
	}

	public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig) {
		return getOrCreateMQClientInstance(clientConfig, null);
	}

	public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {
		String clientId = clientConfig.buildMQClientId();
		MQClientInstance instance = this.factoryTable.get(clientId);

		if (instance == null) {
			instance = new MQClientInstance(clientConfig.cloneClientConfig(), this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);

			MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
			if (prev != null) {
				instance = prev;
				log.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
			}
			else {
				log.info("Created new MQClientInstance for clientId:[{}]", clientId);
			}
		}

		return instance;
	}

	public ProduceAccumulator getOrCreateProduceAccumulator(final ClientConfig clientConfig) {
		String clientId = clientConfig.buildMQClientId();
		ProduceAccumulator accumulator = this.accumulatorTable.get(clientId);
		if (accumulator == null) {
			accumulator = new ProduceAccumulator(clientId);
			ProduceAccumulator prev = this.accumulatorTable.putIfAbsent(clientId, accumulator);
			if (prev != null) {
				accumulator = prev;
				log.warn("Returned Previous ProduceAccumulator for clientId: {}", clientId);
			}
			else {
				log.info("Created new ProduceAccumulator for clientId: {}", clientConfig);
			}
		}
		return accumulator;
	}

	public void removeClientFactory(final String clientId) {
		this.factoryTable.remove(clientId);
	}
}
