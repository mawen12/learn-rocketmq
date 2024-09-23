package com.mawen.learn.rocketmq.common.utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public class ThreadUtils {

	private static final Logger log = LoggerFactory.getLogger(ThreadUtils.class);

	public static ExecutorService newSingleThreadExecutor(String processName, boolean isDaemon) {
		return ThreadUtils.newSingleThreadExecutor(newThreadFactory(processName, isDaemon));
	}

	public static ExecutorService newSingleThreadExecutor(ThreadFactory threadFactory) {
		return ThreadUtils.newThreadPoolExecutor(1, threadFactory);
	}

	public static ExecutorService newThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory) {
		return ThreadUtils.newThreadPoolExecutor(corePoolSize, corePoolSize, 0L, TimeUnit.MICROSECONDS, new LinkedBlockingDeque<>(), threadFactory);
	}

	public static ExecutorService newThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, String processName, boolean isDaemon) {
		return ThreadUtils.newThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, new ThreadFactory(processName, isDaemon));
	}

	public static ExecutorService newThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
		return ThreadUtils.newThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, new ThreadPoolExecutor.AbortPolicy());
	}

	public static ExecutorService newThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
		return new FutureTaskExtThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
	}

	public static ScheduledExecutorService newSingleThreadScheduledExecutor(String processName, boolean isDaemon) {
		return ThreadUtils.newScheduledThreadPool(1, processName, isDaemon);
	}

	public static ScheduledExecutorService newSingleThreadScheduledExecutor(ThreadFactory threadFactory) {
		return ThreadUtils.newScheduledThreadPool(1, threadFactory);
	}

	public static ScheduledExecutorService newScheduledThreadPool(int corePoolSize) {
		return ThreadUtils.newScheduledThreadPool(corePoolSize, Executors.defaultThreadFactory());
	}

	public static ScheduledExecutorService newScheduledThreadPool(int corePoolSize, String processName, boolean isDaemon) {
		return ThreadUtils.newScheduledThreadPool(corePoolSize, newThreadFactory(processName, isDaemon));
	}

	public static ScheduledExecutorService newScheduledThreadPool(int corePoolSize, ThreadFactory threadFactory) {
		return ThreadUtils.newScheduledThreadPool(corePoolSize, threadFactory, new ThreadPoolExecutor.AbortPolicy());
	}

	public static ScheduledExecutorService newScheduledThreadPool(int corePoolSize, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
		return new ScheduledThreadPoolExecutor(corePoolSize, threadFactory, handler);
	}

	public static ThreadFactory newThreadFactory(String processName, boolean isDaemon) {
		return newGenericThreadFactory("ThreadUtils-" + processName, isDaemon);
	}

	public static ThreadFactory newGenericThreadFactory(String processName) {
		return newGenericThreadFactory(processName, false);
	}

	public static ThreadFactory newGenericThreadFactory(String processName, int threads) {
		return newGenericThreadFactory(processName, threads, false);
	}

	public static ThreadFactory newGenericThreadFactory(String processName, boolean isDaemon) {
		return new ThreadFactoryImpl(processName + "_", isDaemon);
	}

	public static ThreadFactory newGenericThreadFactory(String processName, int threads, boolean isDaemon) {
		return new ThreadFactoryImpl(String.format("%s_%d_", processName, threads), isDaemon);
	}

	public static Thread newThread(String name, Runnable runnable, boolean daemon) {
		Thread thread = new Thread(runnable, name);
		thread.setDaemon(daemon);
		thread.setUncaughtExceptionHandler((t, e) -> {
			log.error("Uncaught exception in thread '" + t.getName() + "':", e);
		});
		return thread;
	}

	public static void shutdownGracefully(Thread t) {
		shutdownGracefully(t, 0);
	}

	public static void shutdownGracefully(Thread t, long millis) {
		if (t == null) {
			return;
		}

		while (t.isAlive()) {
			try {
				t.interrupt();
				t.join(millis);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	public static void shutdownGracefully(ExecutorService executor, long timeout, TimeUnit unit) {
		executor.shutdown();

		try {
			if (!executor.awaitTermination(timeout, unit)) {
				executor.shutdownNow();
				if (!executor.awaitTermination(timeout, unit)) {
					log.warn("{} didn't terminate!", executor);
				}
			}
		}
		catch (InterruptedException e) {
			executor.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}

	public static void shutdown(ExecutorService executorService) {
		if (executorService != null) {
			executorService.shutdown();
		}
	}

	private ThreadUtils() {}
}
