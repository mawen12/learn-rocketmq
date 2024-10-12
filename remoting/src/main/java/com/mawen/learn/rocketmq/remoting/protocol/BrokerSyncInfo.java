package com.mawen.learn.rocketmq.remoting.protocol;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class BrokerSyncInfo extends RemotingSerializable {

	private String masterHaAddress;

	private long masterFlushOffset;

	private String masterAddress;

	public BrokerSyncInfo(String masterHaAddress, long masterFlushOffset, String masterAddress) {
		this.masterHaAddress = masterHaAddress;
		this.masterFlushOffset = masterFlushOffset;
		this.masterAddress = masterAddress;
	}

	public String getMasterHaAddress() {
		return masterHaAddress;
	}

	public void setMasterHaAddress(String masterHaAddress) {
		this.masterHaAddress = masterHaAddress;
	}

	public long getMasterFlushOffset() {
		return masterFlushOffset;
	}

	public void setMasterFlushOffset(long masterFlushOffset) {
		this.masterFlushOffset = masterFlushOffset;
	}

	public String getMasterAddress() {
		return masterAddress;
	}

	public void setMasterAddress(String masterAddress) {
		this.masterAddress = masterAddress;
	}

	@Override
	public String toString() {
		return "BrokerSyncInfo{" +
				"masterHaAddress='" + masterHaAddress + '\'' +
				", masterFlushOffset=" + masterFlushOffset +
				", masterAddress=" + masterAddress +
				'}';
	}
}
