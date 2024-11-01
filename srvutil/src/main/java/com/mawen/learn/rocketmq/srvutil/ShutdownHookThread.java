package com.mawen.learn.rocketmq.srvutil;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.logging.org.slf4j.Logger;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
public class ShutdownHookThread extends Thread {

	private volatile boolean hasShutdown = false;
	private AtomicInteger shutdownTimes = new AtomicInteger(0);
	private final Logger log;
	private final Callable callback;

	public ShutdownHookThread(Logger log, Callable callback) {
		super("ShutdownHook");
		this.log = log;
		this.callback = callback;
	}

	@Override
	public void run() {
		synchronized (this) {
			log.info("shutdown hook was invoked, {} times", shutdownTimes.incrementAndGet());
			if (!hasShutdown) {
				hasShutdown = true;
				long begin = System.currentTimeMillis();
				try {
					callback.call();
				}
				catch (Exception e) {
					log.error("shutdown hook callback invoked failure,", e);
				}

				long cost = System.currentTimeMillis() - begin;
				log.info("shutdown hook done, consuming time total(ms): {}", cost);
			}
		}
	}
}
