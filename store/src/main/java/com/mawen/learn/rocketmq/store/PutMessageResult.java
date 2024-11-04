package com.mawen.learn.rocketmq.store;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/4
 */
@Getter
@Setter
@ToString
@AllArgsConstructor
public class PutMessageResult {
	private PutMessageStatus putMessageStatus;
	private AppendMessageResult appendMessageResult;
	private boolean remotePut = false;

	public PutMessageResult(PutMessageStatus putMessageStatus, AppendMessageResult appendMessageResult) {
		this.putMessageStatus = putMessageStatus;
		this.appendMessageResult = appendMessageResult;
	}

	public boolean isOk() {
		if (remotePut) {
			return putMessageStatus == PutMessageStatus.PUT_OK
					|| putMessageStatus == PutMessageStatus.FLUSH_DISK_TIMEOUT
					|| putMessageStatus == PutMessageStatus.FLUSH_SLAVE_TIMEOUT
					|| putMessageStatus == PutMessageStatus.SLAVE_NOT_AVAILABLE;
		}
		else {
			return appendMessageResult != null && appendMessageResult.isOk();
		}
	}
}
