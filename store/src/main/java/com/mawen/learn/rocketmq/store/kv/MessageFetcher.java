package com.mawen.learn.rocketmq.store.kv;

import java.util.function.BiFunction;

import com.google.common.collect.Lists;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.remoting.RemotingClient;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.netty.NettyClientConfig;
import com.mawen.learn.rocketmq.remoting.netty.NettyRemotingClient;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.store.MessageFilter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
public class MessageFetcher implements AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	private final RemotingClient client;

	public MessageFetcher() {
		NettyClientConfig nettyClientConfig = new NettyClientConfig();
		nettyClientConfig.setUseTLS(false);
		this.client = new NettyRemotingClient(nettyClientConfig);
		this.client.start();
	}

	@Override
	public void close() throws Exception {
		client.shutdown();
	}

	public void pullMessageFromMaster(String topic, int queueId, long endOffset, String masterAddr, BiFunction<Long, RemotingCommand, Boolean> responseHandler) throws RemotingCommandException {
		long currentPullOffset = 0;

		try {
			long subVersion = System.currentTimeMillis();
			String groupName =  getConsumerGroup(topic, queueId);
			if (!prepare(masterAddr, topic, groupName, subVersion)) {
				log.error("{}:{} prepare to {} pull message failed", topic, queueId, masterAddr);
				throw new RemotingCommandException(topic + ":" + queueId + " prepare to " + masterAddr + " pull message failed");
			}

			boolean noNewMsg = false;
			boolean keepPull = true;
			while (!stopFull(currentPullOffset, endOffset)) {
				createPullMessageRequest(topic, queueId, currentPullOffset, subVersion);
			}
		}
		finally {
			if (client != null) {
				client.closeChannels(Lists.newArrayList(masterAddr));
			}
		}
	}
}
