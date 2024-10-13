package com.mawen.learn.rocketmq.remoting.protocol.body;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class ConsumeStats {

	private double pullRT;

	private double pullTPS;

	private double consumeRT;

	private double consumeOKTPS;

	private double consumeFailedTPS;

	private long consumeFailedMsgs;

	public double getPullRT() {
		return pullRT;
	}

	public void setPullRT(double pullRT) {
		this.pullRT = pullRT;
	}

	public double getPullTPS() {
		return pullTPS;
	}

	public void setPullTPS(double pullTPS) {
		this.pullTPS = pullTPS;
	}

	public double getConsumeRT() {
		return consumeRT;
	}

	public void setConsumeRT(double consumeRT) {
		this.consumeRT = consumeRT;
	}

	public double getConsumeOKTPS() {
		return consumeOKTPS;
	}

	public void setConsumeOKTPS(double consumeOKTPS) {
		this.consumeOKTPS = consumeOKTPS;
	}

	public double getConsumeFailedTPS() {
		return consumeFailedTPS;
	}

	public void setConsumeFailedTPS(double consumeFailedTPS) {
		this.consumeFailedTPS = consumeFailedTPS;
	}

	public long getConsumeFailedMsgs() {
		return consumeFailedMsgs;
	}

	public void setConsumeFailedMsgs(long consumeFailedMsgs) {
		this.consumeFailedMsgs = consumeFailedMsgs;
	}
}
