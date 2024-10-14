package com.mawen.learn.rocketmq.remoting.protocol.route;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class QueueData implements Comparable<QueueData> {

	private String brokerName;

	private int readQueueNums;

	private int writeQueueNums;

	private int perm;

	private int topicSysFlag;

	public QueueData() {
	}

	public QueueData(QueueData queueData) {
		this.brokerName = queueData.brokerName;
		this.readQueueNums = queueData.readQueueNums;
		this.writeQueueNums = queueData.writeQueueNums;
		this.perm = queueData.perm;
		this.topicSysFlag = queueData.topicSysFlag;
	}

	public String getBrokerName() {
		return brokerName;
	}

	public void setBrokerName(String brokerName) {
		this.brokerName = brokerName;
	}

	public int getReadQueueNums() {
		return readQueueNums;
	}

	public void setReadQueueNums(int readQueueNums) {
		this.readQueueNums = readQueueNums;
	}

	public int getWriteQueueNums() {
		return writeQueueNums;
	}

	public void setWriteQueueNums(int writeQueueNums) {
		this.writeQueueNums = writeQueueNums;
	}

	public int getPerm() {
		return perm;
	}

	public void setPerm(int perm) {
		this.perm = perm;
	}

	public int getTopicSysFlag() {
		return topicSysFlag;
	}

	public void setTopicSysFlag(int topicSysFlag) {
		this.topicSysFlag = topicSysFlag;
	}

	@Override
	public int compareTo(QueueData o) {
		return this.brokerName.compareTo(o.getBrokerName());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
		result = prime * result + perm;
		result = prime * result + readQueueNums;
		result = prime * result + writeQueueNums;
		result = prime * result + topicSysFlag;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QueueData other = (QueueData) obj;
		if (brokerName == null) {
			if (other.brokerName != null)
				return false;
		} else if (!brokerName.equals(other.brokerName))
			return false;
		if (perm != other.perm)
			return false;
		if (readQueueNums != other.readQueueNums)
			return false;
		if (writeQueueNums != other.writeQueueNums)
			return false;
		return topicSysFlag == other.topicSysFlag;
	}

	@Override
	public String toString() {
		return "QueueData [brokerName=" + brokerName + ", readQueueNums=" + readQueueNums
				+ ", writeQueueNums=" + writeQueueNums + ", perm=" + perm + ", topicSysFlag=" + topicSysFlag
				+ "]";
	}
}
