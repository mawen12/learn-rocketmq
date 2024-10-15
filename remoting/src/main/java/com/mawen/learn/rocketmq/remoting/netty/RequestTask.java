package com.mawen.learn.rocketmq.remoting.netty;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/15
 */
public class RequestTask implements Runnable {
	private final Runnable runnable;

	private final long createTimestamp = System.currentTimeMillis();

	private final Channel channel;

	private final RemotingCommand request;

	private volatile boolean stopRun = false;

	public RequestTask(Runnable runnable, Channel channel, RemotingCommand request) {
		this.runnable = runnable;
		this.channel = channel;
		this.request = request;
	}

	@Override
	public void run() {
		if (!this.stopRun) {
			this.runnable.run();
		}
	}

	public void returnResponse(int code, String remark) {
		RemotingCommand response = RemotingCommand.createResponseCommand(code, remark);
		response.setOpaque(request.getOpaque());
		this.channel.writeAndFlush(response);
	}

	public Runnable getRunnable() {
		return runnable;
	}

	public long getCreateTimestamp() {
		return createTimestamp;
	}

	public Channel getChannel() {
		return channel;
	}

	public RemotingCommand getRequest() {
		return request;
	}

	public boolean isStopRun() {
		return stopRun;
	}

	public void setStopRun(boolean stopRun) {
		this.stopRun = stopRun;
	}

	@Override
	public int hashCode() {
		int result = runnable != null ? runnable.hashCode() : 0;
		result = 31 * result + (int) (getCreateTimestamp() ^ (getCreateTimestamp() >>> 32));
		result = 31 * result + (channel != null ? channel.hashCode() : 0);
		result = 31 * result + (request != null ? request.hashCode() : 0);
		result = 31 * result + (isStopRun() ? 1 : 0);
		return result;
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o)
			return true;
		if (!(o instanceof RequestTask))
			return false;

		final RequestTask that = (RequestTask) o;

		if (getCreateTimestamp() != that.getCreateTimestamp())
			return false;
		if (isStopRun() != that.isStopRun())
			return false;
		if (channel != null ? !channel.equals(that.channel) : that.channel != null)
			return false;
		return request != null ? request.getOpaque() == that.request.getOpaque() : that.request == null;

	}
}
