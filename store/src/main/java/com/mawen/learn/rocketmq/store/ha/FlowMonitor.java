package com.mawen.learn.rocketmq.store.ha;

import java.util.concurrent.atomic.AtomicLong;

import com.mawen.learn.rocketmq.common.ServiceThread;
import com.mawen.learn.rocketmq.store.config.MessageStoreConfig;
import lombok.Getter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/11
 */
@Getter
public class FlowMonitor extends ServiceThread {

	private final AtomicLong transferByte = new AtomicLong(0L);
	private volatile long transferredByteInSecond;
	private MessageStoreConfig messageStoreConfig;

	public FlowMonitor(MessageStoreConfig messageStoreConfig) {
		this.messageStoreConfig = messageStoreConfig;
	}

	@Override
	public String getServiceName() {
		return FlowMonitor.class.getSimpleName();
	}

	@Override
	public void run() {
		while (!isStopped()) {
			waitForRunning(1000);
			calculateSpeed();
		}
	}

	public void calculateSpeed() {
		transferredByteInSecond = transferByte.get();
		transferByte.set(0);
	}

	public int canTransferMaxByteNum() {
		if (isFlowControlEnable()) {
			long res = Math.max(maxTransferByteInSecond() - transferByte.get(), 0);
			return res < Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) res;
		}
		return Integer.MAX_VALUE;
	}

	public void addByteCountTransferred(long count) {
		transferByte.addAndGet(count);
	}

	public boolean isFlowControlEnable() {
		return messageStoreConfig.isHaFlowControlEnable();
	}

	public long maxTransferByteInSecond() {
		return messageStoreConfig.getMaxHaTransferByteInSecond();
	}
}
