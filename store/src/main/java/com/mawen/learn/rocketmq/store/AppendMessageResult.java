package com.mawen.learn.rocketmq.store;

import java.util.function.Supplier;

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
public class AppendMessageResult {

	private AppendMessageStatus status;

	private long wroteOffset;

	private int wroteBytes;

	private String msgId;

	private Supplier<String> msgIdSupplier;

	private long storeTimestamp;

	private long logicsOffset;

	private long pagecacheRT = 0;

	private int msgNum = 1;

	public AppendMessageResult(AppendMessageStatus status) {
		this(status, 0, 0, "", 0, 0, 0);
	}

	public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, String msgId,
			long storeTimestamp, long logicsOffset, long pagecacheRT) {
		this.status = status;
		this.wroteOffset = wroteOffset;
		this.wroteBytes = wroteBytes;
		this.msgId = msgId;
		this.storeTimestamp = storeTimestamp;
		this.logicsOffset = logicsOffset;
		this.pagecacheRT = pagecacheRT;
	}

	public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, long storeTimestamp) {
		this.status = status;
		this.wroteOffset = wroteOffset;
		this.wroteBytes = wroteBytes;
		this.storeTimestamp = storeTimestamp;
	}

	public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, Supplier<String> msgIdSupplier,
			long storeTimestamp, long logicsOffset, long pagecacheRT) {
		this.status = status;
		this.wroteOffset = wroteOffset;
		this.wroteBytes = wroteBytes;
		this.msgIdSupplier = msgIdSupplier;
		this.storeTimestamp = storeTimestamp;
		this.logicsOffset = logicsOffset;
		this.pagecacheRT = pagecacheRT;
	}

	public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, Supplier<String> msgIdSupplier,
			long storeTimestamp, long logicsOffset, long pagecacheRT, int msgNum) {
		this.status = status;
		this.wroteOffset = wroteOffset;
		this.wroteBytes = wroteBytes;
		this.msgIdSupplier = msgIdSupplier;
		this.storeTimestamp = storeTimestamp;
		this.logicsOffset = logicsOffset;
		this.pagecacheRT = pagecacheRT;
		this.msgNum = msgNum;
	}

	public boolean isOk() {
		return status == AppendMessageStatus.PUT_OK;
	}

	public String getMsgId() {
		if (msgId == null && msgIdSupplier != null) {
			msgId = msgIdSupplier.get();
		}
		return msgId;
	}
}
