package com.mawen.learn.rocketmq.common.message;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import com.mawen.learn.rocketmq.common.TopicFilterType;
import com.mawen.learn.rocketmq.common.sysflag.MessageSysFlag;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/1
 */
public class MessageExt extends Message {

	private static final long serialVersionUID = 3710643140064807714L;

	private String brokerName;
	private int queueId;
	private int storeSize;
	private long queueOffset;
	private int sysFlag;
	private long bornTimestamp;
	private SocketAddress bornHost;

	private long storeTimestamp;
	private SocketAddress storeHost;
	private String msgId;
	private long commitLogOffset;
	private int bodyCRC;
	private int reconsumeTimes;

	private long preparedTransactionOffset;

	public MessageExt() {
	}

	public MessageExt(int queueId, long bornTimestamp, SocketAddress bornHost, long storeTimestamp, SocketAddress storeHost, String msgId) {
		this.queueId = queueId;
		this.bornTimestamp = bornTimestamp;
		this.bornHost = bornHost;
		this.storeTimestamp = storeTimestamp;
		this.storeHost = storeHost;
		this.msgId = msgId;
	}

	public static TopicFilterType parseTopicFilterType(final int sysFlag) {
		if ((sysFlag & MessageSysFlag.MULTI_TAGS_FLAG) == MessageSysFlag.MULTI_TAGS_FLAG) {
			return TopicFilterType.MULTI_FLAG;
		}
		return TopicFilterType.SINGLE_FLAG;
	}

	public static ByteBuffer socketAddress2ByteBuffer(final SocketAddress socketAddress, final ByteBuffer byteBuffer) {
		InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
		InetAddress address = inetSocketAddress.getAddress();
		if (address instanceof Inet4Address) {
			byteBuffer.put(inetSocketAddress.getAddress().getAddress(), 0, 4);
		}
		else {
			byteBuffer.put(inetSocketAddress.getAddress().getAddress(), 0, 16);
		}

		byteBuffer.putInt(inetSocketAddress.getPort());
		byteBuffer.flip();
		return byteBuffer;
	}

	public static ByteBuffer socketAddress2ByteBuffer(SocketAddress socketAddress) {
		InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
		InetAddress address = inetSocketAddress.getAddress();
		ByteBuffer byteBuffer;
		if (address instanceof Inet4Address) {
			byteBuffer = ByteBuffer.allocate(4 + 4);
		}
		else {
			byteBuffer = ByteBuffer.allocate(16 + 4);
		}
		return socketAddress2ByteBuffer(socketAddress, byteBuffer);
	}

	public ByteBuffer getBornHostBytes() {
		return socketAddress2ByteBuffer(this.bornHost);
	}

	public ByteBuffer getBornHostBytes(ByteBuffer buffer) {
		return socketAddress2ByteBuffer(this.bornHost, buffer);
	}

	public ByteBuffer getStoreHostBytes() {
		return socketAddress2ByteBuffer(this.storeHost);
	}

	public ByteBuffer getStoreHostBytes(ByteBuffer byteBuffer) {
		return socketAddress2ByteBuffer(this.storeHost, byteBuffer);
	}

	public void setStoreHostAddressV6Flag() {
		this.sysFlag = this.sysFlag | MessageSysFlag.STOREHOSTADDRESS_V6_FLAG;
	}

	public void setBornHostV6Flag() {
		this.sysFlag = this.sysFlag | MessageSysFlag.BORNHOST_V6_FLAG;
	}

	public String getBornHostString() {
		if (this.bornHost != null) {
			InetAddress inetAddress = ((InetSocketAddress) this.bornHost).getAddress();

			return inetAddress != null ? inetAddress.getHostAddress() : null;
		}
		return null;
	}

	public String getBornHostNameString() {
		if (this.bornHost != null) {
			if (this.bornHost instanceof InetSocketAddress) {
				return ((InetSocketAddress)bornHost).getHostString();
			}

			InetAddress inetAddress = ((InetSocketAddress) this.bornHost).getAddress();

			return inetAddress != null ? inetAddress.getHostName() : null;
		}
		return null;
	}

	public String getBrokerName() {
		return brokerName;
	}

	public void setBrokerName(String brokerName) {
		this.brokerName = brokerName;
	}

	public int getQueueId() {
		return queueId;
	}

	public void setQueueId(int queueId) {
		this.queueId = queueId;
	}

	public long getPreparedTransactionOffset() {
		return preparedTransactionOffset;
	}

	public void setPreparedTransactionOffset(long preparedTransactionOffset) {
		this.preparedTransactionOffset = preparedTransactionOffset;
	}

	public int getReconsumeTimes() {
		return reconsumeTimes;
	}

	public void setReconsumeTimes(int reconsumeTimes) {
		this.reconsumeTimes = reconsumeTimes;
	}

	public int getBodyCRC() {
		return bodyCRC;
	}

	public void setBodyCRC(int bodyCRC) {
		this.bodyCRC = bodyCRC;
	}

	public long getCommitLogOffset() {
		return commitLogOffset;
	}

	public void setCommitLogOffset(long commitLogOffset) {
		this.commitLogOffset = commitLogOffset;
	}

	public String getMsgId() {
		return msgId;
	}

	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}

	public SocketAddress getStoreHost() {
		return storeHost;
	}

	public void setStoreHost(SocketAddress storeHost) {
		this.storeHost = storeHost;
	}

	public long getStoreTimestamp() {
		return storeTimestamp;
	}

	public void setStoreTimestamp(long storeTimestamp) {
		this.storeTimestamp = storeTimestamp;
	}

	public SocketAddress getBornHost() {
		return bornHost;
	}

	public void setBornHost(SocketAddress bornHost) {
		this.bornHost = bornHost;
	}

	public long getBornTimestamp() {
		return bornTimestamp;
	}

	public void setBornTimestamp(long bornTimestamp) {
		this.bornTimestamp = bornTimestamp;
	}

	public int getSysFlag() {
		return sysFlag;
	}

	public void setSysFlag(int sysFlag) {
		this.sysFlag = sysFlag;
	}

	public long getQueueOffset() {
		return queueOffset;
	}

	public void setQueueOffset(long queueOffset) {
		this.queueOffset = queueOffset;
	}

	public int getStoreSize() {
		return storeSize;
	}

	public void setStoreSize(int storeSize) {
		this.storeSize = storeSize;
	}

	public Integer getTopicSysFlag() {
		String topicSysFlagString = getProperty(MessageConst.PROPERTY_TRANSIENT_TOPIC_CONFIG);
		if (topicSysFlagString != null && !topicSysFlagString.isEmpty()) {
			return Integer.valueOf(topicSysFlagString);
		}
		return null;
	}

	public void setTopicSysFlag(Integer topicSysFlag) {
		if (topicSysFlag == null) {
			clearProperty(MessageConst.PROPERTY_TRANSIENT_TOPIC_CONFIG);
		}
		else {
			putProperty(MessageConst.PROPERTY_TRANSIENT_TOPIC_CONFIG, String.valueOf(topicSysFlag));
		}
	}

	public Integer getGroupSysFlag() {
		String groupSysFlagString = getProperty(MessageConst.PROPERTY_TRANSIENT_GROUP_CONFIG);
		if (groupSysFlagString != null && !groupSysFlagString.isEmpty()) {
			return Integer.valueOf(groupSysFlagString);
		}
		return null;
	}

	public void setGroupSysFlag(Integer groupSysFlag) {
		if (groupSysFlag == null) {
			clearProperty(MessageConst.PROPERTY_TRANSIENT_GROUP_CONFIG);
		}
		else {
			putProperty(MessageConst.PROPERTY_TRANSIENT_GROUP_CONFIG,String.valueOf(groupSysFlag));
		}
	}

	@Override
	public String toString() {
		return "MessageExt [brokerName=" + brokerName + ", queueId=" + queueId + ", storeSize=" + storeSize + ", queueOffset=" + queueOffset
		       + ", sysFlag=" + sysFlag + ", bornTimestamp=" + bornTimestamp + ", bornHost=" + bornHost
		       + ", storeTimestamp=" + storeTimestamp + ", storeHost=" + storeHost + ", msgId=" + msgId
		       + ", commitLogOffset=" + commitLogOffset + ", bodyCRC=" + bodyCRC + ", reconsumeTimes="
		       + reconsumeTimes + ", preparedTransactionOffset=" + preparedTransactionOffset
		       + ", toString()=" + super.toString() + "]";
	}
}
