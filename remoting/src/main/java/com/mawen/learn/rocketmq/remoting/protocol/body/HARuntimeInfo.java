package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.ArrayList;
import java.util.List;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class HARuntimeInfo extends RemotingSerializable {

	private boolean master;

	private long masterCommitLogMaxOffset;

	private int inSyncSlaveNums;

	private List<HAConnectionRuntimeInfo> haConnectionInfo = new ArrayList<>();

	private HAClientRuntimeInfo haClientRuntimeInfo = new HAClientRuntimeInfo();

	public HAClientRuntimeInfo getHaClientRuntimeInfo() {
		return haClientRuntimeInfo;
	}

	public void setHaClientRuntimeInfo(HAClientRuntimeInfo haClientRuntimeInfo) {
		this.haClientRuntimeInfo = haClientRuntimeInfo;
	}

	public List<HAConnectionRuntimeInfo> getHaConnectionInfo() {
		return haConnectionInfo;
	}

	public void setHaConnectionInfo(List<HAConnectionRuntimeInfo> haConnectionInfo) {
		this.haConnectionInfo = haConnectionInfo;
	}

	public int getInSyncSlaveNums() {
		return inSyncSlaveNums;
	}

	public void setInSyncSlaveNums(int inSyncSlaveNums) {
		this.inSyncSlaveNums = inSyncSlaveNums;
	}

	public long getMasterCommitLogMaxOffset() {
		return masterCommitLogMaxOffset;
	}

	public void setMasterCommitLogMaxOffset(long masterCommitLogMaxOffset) {
		this.masterCommitLogMaxOffset = masterCommitLogMaxOffset;
	}

	public boolean isMaster() {
		return master;
	}

	public void setMaster(boolean master) {
		this.master = master;
	}

	public static class HAConnectionRuntimeInfo extends RemotingSerializable {
		private String addr;

		private long slaveAckOffset;

		private long diff;

		private boolean inSync;

		private long transferredByteInSecond;

		private long transferFromWhere;

		public String getAddr() {
			return addr;
		}

		public void setAddr(String addr) {
			this.addr = addr;
		}

		public long getSlaveAckOffset() {
			return slaveAckOffset;
		}

		public void setSlaveAckOffset(long slaveAckOffset) {
			this.slaveAckOffset = slaveAckOffset;
		}

		public long getDiff() {
			return diff;
		}

		public void setDiff(long diff) {
			this.diff = diff;
		}

		public boolean isInSync() {
			return inSync;
		}

		public void setInSync(boolean inSync) {
			this.inSync = inSync;
		}

		public long getTransferredByteInSecond() {
			return transferredByteInSecond;
		}

		public void setTransferredByteInSecond(long transferredByteInSecond) {
			this.transferredByteInSecond = transferredByteInSecond;
		}

		public long getTransferFromWhere() {
			return transferFromWhere;
		}

		public void setTransferFromWhere(long transferFromWhere) {
			this.transferFromWhere = transferFromWhere;
		}
	}


	public static class HAClientRuntimeInfo extends RemotingSerializable {
		private String masterAddr;

		private long transferredByteInSecond;

		private long maxOffset;

		private long lastReadTimestamp;

		private long lastWriteTimestamp;

		private long masterFlushOffset;

		private boolean isActivated = false;

		public String getMasterAddr() {
			return masterAddr;
		}

		public void setMasterAddr(String masterAddr) {
			this.masterAddr = masterAddr;
		}

		public long getTransferredByteInSecond() {
			return transferredByteInSecond;
		}

		public void setTransferredByteInSecond(long transferredByteInSecond) {
			this.transferredByteInSecond = transferredByteInSecond;
		}

		public long getMaxOffset() {
			return maxOffset;
		}

		public void setMaxOffset(long maxOffset) {
			this.maxOffset = maxOffset;
		}

		public long getLastReadTimestamp() {
			return lastReadTimestamp;
		}

		public void setLastReadTimestamp(long lastReadTimestamp) {
			this.lastReadTimestamp = lastReadTimestamp;
		}

		public long getLastWriteTimestamp() {
			return lastWriteTimestamp;
		}

		public void setLastWriteTimestamp(long lastWriteTimestamp) {
			this.lastWriteTimestamp = lastWriteTimestamp;
		}

		public long getMasterFlushOffset() {
			return masterFlushOffset;
		}

		public void setMasterFlushOffset(long masterFlushOffset) {
			this.masterFlushOffset = masterFlushOffset;
		}

		public boolean isActivated() {
			return isActivated;
		}

		public void setActivated(boolean activated) {
			isActivated = activated;
		}
	}
}
