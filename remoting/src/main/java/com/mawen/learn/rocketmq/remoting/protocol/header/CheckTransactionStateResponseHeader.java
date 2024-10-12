package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.sysflag.MessageSysFlag;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class CheckTransactionStateResponseHeader implements CommandCustomHeader {

	@CFNotNull
	private String producerGroup;

	@CFNotNull
	private Long tranStateTableOffset;

	@CFNotNull
	private Long commitLogOffset;

	@CFNotNull
	private Integer commitOrRollback;

	@Override
	public void checkFields() throws RemotingCommandException {
		if (MessageSysFlag.TRANSACTION_COMMIT_TYPE == this.commitOrRollback) {
			return;
		}

		if (MessageSysFlag.TRANSACTION_ROLLBACK_TYPE == this.commitOrRollback) {
			return;
		}

		throw new RemotingCommandException("commitOrRollback field wrong");
	}

	public String getProducerGroup() {
		return producerGroup;
	}

	public void setProducerGroup(String producerGroup) {
		this.producerGroup = producerGroup;
	}

	public Long getTranStateTableOffset() {
		return tranStateTableOffset;
	}

	public void setTranStateTableOffset(Long tranStateTableOffset) {
		this.tranStateTableOffset = tranStateTableOffset;
	}

	public Long getCommitLogOffset() {
		return commitLogOffset;
	}

	public void setCommitLogOffset(Long commitLogOffset) {
		this.commitLogOffset = commitLogOffset;
	}

	public Integer getCommitOrRollback() {
		return commitOrRollback;
	}

	public void setCommitOrRollback(Integer commitOrRollback) {
		this.commitOrRollback = commitOrRollback;
	}
}
