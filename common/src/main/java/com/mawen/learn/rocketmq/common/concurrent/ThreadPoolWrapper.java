package com.mawen.learn.rocketmq.common.concurrent;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/9
 */
public class ThreadPoolWrapper {

	private String name;
	private ThreadPoolExecutor threadPoolExecutor;
	private List<ThreadPoolStatusMonitor> statusPrinters;

	public ThreadPoolWrapper(String name, ThreadPoolExecutor threadPoolExecutor, List<ThreadPoolStatusMonitor> statusPrinters) {
		this.name = name;
		this.threadPoolExecutor = threadPoolExecutor;
		this.statusPrinters = statusPrinters;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public ThreadPoolExecutor getThreadPoolExecutor() {
		return threadPoolExecutor;
	}

	public void setThreadPoolExecutor(ThreadPoolExecutor threadPoolExecutor) {
		this.threadPoolExecutor = threadPoolExecutor;
	}

	public List<ThreadPoolStatusMonitor> getStatusPrinters() {
		return statusPrinters;
	}

	public void setStatusPrinters(List<ThreadPoolStatusMonitor> statusPrinters) {
		this.statusPrinters = statusPrinters;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		ThreadPoolWrapper wrapper = (ThreadPoolWrapper) o;
		return Objects.equal(name, wrapper.name) && Objects.equal(threadPoolExecutor, wrapper.threadPoolExecutor) && Objects.equal(statusPrinters, wrapper.statusPrinters);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(name, threadPoolExecutor, statusPrinters);
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("name", name)
				.add("threadPoolExecutor", threadPoolExecutor)
				.add("statusPrinters", statusPrinters)
				.toString();
	}

	public static class ThreadPoolWrapperBuilder {
		private String name;
		private ThreadPoolExecutor threadPoolExecutor;
		private List<ThreadPoolStatusMonitor> statusPrinters;

		ThreadPoolWrapperBuilder() {}

		public ThreadPoolWrapper.ThreadPoolWrapperBuilder name(final String name) {
			this.name = name;
			return this;
		}

		public ThreadPoolWrapper.ThreadPoolWrapperBuilder threadPoolExecutor(final ThreadPoolExecutor threadPoolExecutor) {
			this.threadPoolExecutor = threadPoolExecutor;
			return this;
		}

		public ThreadPoolWrapper.ThreadPoolWrapperBuilder statusPrinters(final List<ThreadPoolStatusMonitor> statusPrinters) {
			this.statusPrinters = statusPrinters;
			return this;
		}

		public ThreadPoolWrapper build() {
			return new ThreadPoolWrapper(name, threadPoolExecutor, statusPrinters);
		}

		@Override
		public String toString() {
			return "ThreadPoolWrapper.ThreadPoolWrapperBuilder(name=" + this.name + ", threadPoolExecutor=" + this.threadPoolExecutor + ", statusPrinters=" + this.statusPrinters + ")";
		}
	}

	public static ThreadPoolWrapper.ThreadPoolWrapperBuilder builder() {
		return new ThreadPoolWrapperBuilder();
	}
}
