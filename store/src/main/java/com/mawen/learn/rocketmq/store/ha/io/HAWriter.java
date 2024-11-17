package com.mawen.learn.rocketmq.store.ha.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import com.mawen.learn.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
public class HAWriter {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	protected final List<HAWriteHook> writeHookList = new ArrayList<>();

	public boolean write(SocketChannel socketChannel, ByteBuffer buffer) throws IOException {
		int writeSizeZeroTimes = 0;

		while (buffer.hasRemaining()) {
			int writeSize = socketChannel.write(buffer);
			writeHookList.forEach(hook -> hook.afterWrite(writeSize));
			if (writeSize > 0) {
				writeSizeZeroTimes = 0;
			}
			else if (writeSize == 0) {
				if (++writeSizeZeroTimes >= 3) {
					break;
				}
			}
			else {
				log.info("Write socket < 0");
			}
		}

		return !buffer.hasRemaining();
	}

	public void registerHook(HAWriteHook writeHook) {
		writeHookList.add(writeHook);
	}

	public void cleanHook() {
		writeHookList.clear();
	}
}
