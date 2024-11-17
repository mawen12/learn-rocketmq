package com.mawen.learn.rocketmq.store.ha.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import com.mawen.learn.rocketmq.common.constant.LoggerName;
import io.netty.buffer.ByteBuf;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
public abstract class AbstractHAReader {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	protected final List<HAReadHook> readHookList = new ArrayList<>();

	public boolean read(SocketChannel socketChannel, ByteBuffer buffer) {
		int readSizeZeroTimes = 0;

		while (buffer.hasRemaining()) {
			try {
				int readSize = socketChannel.read(buffer);
				readHookList.forEach(hook -> hook.afterRead(readSize));
				if (readSize > 0) {
					readSizeZeroTimes = 0;
					boolean result = processReadResult(buffer);
					if (!result) {
						log.error("Process read result failed");
						return false;
					}
				}
				else if (readSize == 0) {
					if (++readSizeZeroTimes >= 3) {
						break;
					}
				}
				else {
					log.info("Read socket < 0");
					return false;
				}
			}
			catch (IOException e) {
				log.info("Read socket exception", e);
				return false;
			}
		}

		return true;
	}

	public void registerHook(HAReadHook readHook) {
		readHookList.add(readHook);
	}

	public void clearHook() {
		readHookList.clear();
	}

	protected abstract boolean processReadResult(ByteBuffer buffer);
}
