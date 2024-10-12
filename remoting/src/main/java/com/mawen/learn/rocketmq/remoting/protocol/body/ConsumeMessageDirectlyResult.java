package com.mawen.learn.rocketmq.remoting.protocol.body;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class ConsumeMessageDirectlyResult extends RemotingSerializable {

	private boolean order = false;

	private boolean autoCommit = true;

	private CMResult consumeResult;

	private String remark;

	private long spentTimeMillis;

	public boolean isOrder() {
		return order;
	}

	public void setOrder(boolean order) {
		this.order = order;
	}

	public boolean isAutoCommit() {
		return autoCommit;
	}

	public void setAutoCommit(boolean autoCommit) {
		this.autoCommit = autoCommit;
	}

	public CMResult getConsumeResult() {
		return consumeResult;
	}

	public void setConsumeResult(CMResult consumeResult) {
		this.consumeResult = consumeResult;
	}

	public String getRemark() {
		return remark;
	}

	public void setRemark(String remark) {
		this.remark = remark;
	}

	public long getSpentTimeMillis() {
		return spentTimeMillis;
	}

	public void setSpentTimeMillis(long spentTimeMillis) {
		this.spentTimeMillis = spentTimeMillis;
	}

	@Override
	public String toString() {
		return "ConsumeMessageDirectlyResult{" +
				"order=" + order +
				", autoCommit=" + autoCommit +
				", consumeResult=" + consumeResult +
				", remark='" + remark + '\'' +
				", spentTimeMillis=" + spentTimeMillis +
				"} " + super.toString();
	}
}
