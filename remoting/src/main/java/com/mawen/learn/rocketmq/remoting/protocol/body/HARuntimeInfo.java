package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.ArrayList;
import java.util.List;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
@Setter
@Getter
public class HARuntimeInfo extends RemotingSerializable {

	private boolean master;

	private long masterCommitLogMaxOffset;

	private int inSyncSlaveNums;

	private List<HAConnectionRuntimeInfo> haConnectionInfo = new ArrayList<>();

	private HAClientRuntimeInfo haClientRuntimeInfo = new HAClientRuntimeInfo();

	@Setter
	@Getter
	public static class HAConnectionRuntimeInfo extends RemotingSerializable {
		private String addr;

		private long slaveAckOffset;

		private long diff;

		private boolean inSync;

		private long transferredByteInSecond;

		private long transferFromWhere;
	}

	@Setter
	@Getter
	public static class HAClientRuntimeInfo extends RemotingSerializable {
		private String masterAddr;

		private long transferredByteInSecond;

		private long maxOffset;

		private long lastReadTimestamp;

		private long lastWriteTimestamp;

		private long masterFlushOffset;

		private boolean isActivated = false;
	}
}
