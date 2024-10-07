package com.mawen.learn.rocketmq.common.message;

import java.nio.ByteBuffer;

import com.mawen.learn.rocketmq.common.TopicFilterType;
import com.mawen.learn.rocketmq.common.utils.MessageUtils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/6
 */
public class MessageExtBrokerInner extends MessageExt{

	private static final long serialVersionUID = -3875078715479058614L;

	private String propertiesString;
	private long tagCode;
	private ByteBuffer encodedBuffer;
	private volatile boolean encodeCompleted;
	private MessageVersion version = MessageVersion.MESSAGE_VERSION_V1;

	public static long tagsString2tagsCode(TopicFilterType filter, String tags) {
		if (null == tags || tags.length() == 0) {
			return 0;
		}

		return tags.hashCode();
	}

	public static long tagsString2tagsCode(String tags) {
		return tagsString2tagsCode(null, tags);
	}

	public String getPropertiesString() {
		return propertiesString;
	}

	public void setPropertiesString(String propertiesString) {
		this.propertiesString = propertiesString;
	}

	public ByteBuffer getEncodedBuffer() {
		return encodedBuffer;
	}

	public void setEncodedBuffer(ByteBuffer encodedBuffer) {
		this.encodedBuffer = encodedBuffer;
	}

	public long getTagCode() {
		return tagCode;
	}

	public void setTagCode(long tagCode) {
		this.tagCode = tagCode;
	}

	public MessageVersion getVersion() {
		return version;
	}

	public void setVersion(MessageVersion version) {
		this.version = version;
	}

	public boolean isEncodeCompleted() {
		return encodeCompleted;
	}

	public void setEncodeCompleted(boolean encodeCompleted) {
		this.encodeCompleted = encodeCompleted;
	}

	public void deleteProperty(String name) {
		super.clearProperty(name);

		if (propertiesString != null) {
			this.setPropertiesString(MessageUtils.deleteProperty(propertiesString, name));
		}
	}

	public void removeWaitStorePropertyString() {
		if (this.getProperties().containsKey(MessageConst.PROPERTY_WAIT_STORE_MSG_OK)) {
			String waitStoreMsgOKValue = this.getProperties().remove(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
			this.setPropertiesString(MessageDecoder.messageProperties2String(this.getProperties()));
			this.getProperties().put(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, waitStoreMsgOKValue);
		}
		else {
			this.setPropertiesString(MessageDecoder.messageProperties2String(this.getProperties()));
		}
	}
}
