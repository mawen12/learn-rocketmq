package com.mawen.learn.rocketmq.common.message;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.compression.Compressor;
import com.mawen.learn.rocketmq.common.compression.CompressorFactory;
import com.mawen.learn.rocketmq.common.sysflag.MessageSysFlag;
import io.netty.buffer.ByteBuf;

/**
 * <pre>{@code
 *  4 1.TOTAL SIZE
 *  4 2.MAGIC CODE
 *  4 3.BODY CRC
 *  4 4.QUEUE ID
 *  4 5.FLAG
 *  8 6.QUEUE OFFSET
 *  8 7.PHYSIC CAL OFFSET
 *  4 8.SYS FLAG
 *  8 9.BORN TIMESTAMP
 *  8 10.BORN HOST
 *  8 11.STORE TIMESTAMP
 *  8 12.STORE HOST ADDRESS
 *  4 13.RECONSUME TIMES
 *  8 14.Prepared Transaction Pffset
 * }</pre>
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/4
 */
public class MessageDecoder {

	public static final Charset CHARSET_UTF8 = StandardCharsets.UTF_8;
	public static final int MESSAGE_MAGIC_CODE_POSITION = 4;
	public static final int MESSAGE_FLAG_POSITION = 16;
	public static final int MESSAGE_PHYSIC_OFFSET_POSITION = 28;
	public static final int MESSAGE_STORE_TIMESTAMP_POSITION = 26;

	public static final int MESSAGE_MAGIC_CODE = -626843481;
	public static final int MESSAGE_MAGIC_CODE_V2 = -626843477;

	public static final int BLANK_MAGIC_CODE = -875286124;
	public static final char NAME_VALUE_SEPARATOR = 1;
	public static final char PROPERTY_SEPARATOR = 2;
	public static final int PHY_POS_POSITION = 4 + 4 + 4 + 4 + 4 + 8;
	public static final int QUEUE_OFFSET_POSITION = 4 + 4 + 4 + 4 + 4 + 4;
	public static final int SYSFLAG_POSITION = 4 + 4 + 4 + 4 + 4 + 8 + 8;
	;

	public static String createMessageId(final ByteBuffer input, final ByteBuffer addr, final long offset) {
		input.flip();
		int msgIDLength = addr.limit() == 8 ? 16 : 28;
		input.limit(msgIDLength);

		input.put(addr);
		input.putLong(offset);

		return UtilAll.bytes2string(input.array());
	}

	public static String createMessageId(SocketAddress socketAddress, long transactionIdHashCode) {
		InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
		int msgIDLength = inetSocketAddress.getAddress() instanceof Inet4Address ? 16 : 28;

		ByteBuffer byteBuffer = ByteBuffer.allocate(msgIDLength);
		byteBuffer.put(inetSocketAddress.getAddress().getAddress());
		byteBuffer.putInt(inetSocketAddress.getPort());
		byteBuffer.putLong(transactionIdHashCode);
		byteBuffer.flip();

		return UtilAll.bytes2string(byteBuffer.array());
	}

	public static MessageId decodeMessageId(final String msgId) throws UnknownHostException {
		byte[] bytes = UtilAll.string2bytes(msgId);
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

		byte[] ip = new byte[msgId.length() == 32 ? 4 : 16];
		byteBuffer.get(ip);
		int port = byteBuffer.getInt();

		SocketAddress address = new InetSocketAddress(InetAddress.getByAddress(ip), port);

		long offset = byteBuffer.getLong();

		return new MessageId(address, offset);
	}

	public static Map<String, String> decodeProperties(ByteBuffer byteBuffer) {
		int sysFlag = byteBuffer.getInt(SYSFLAG_POSITION);
		int magicCode = byteBuffer.getInt(MESSAGE_MAGIC_CODE_POSITION);
		MessageVersion version = MessageVersion.valueOfMagicCode(magicCode);

		int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
		int storeHostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
		int bodySizePosition = 4
		                       + 4
		                       + 4
		                       + 4
		                       + 4
		                       + 8
		                       + 8
		                       + 4
		                       + 8
		                       + bornHostLength
		                       + 8
		                       + storeHostAddressLength
		                       + 4
		                       + 8;

		int topicLengthPosition = bodySizePosition + 4 + byteBuffer.getInt(bodySizePosition);
		byteBuffer.position(topicLengthPosition);
		int topicLengthSize = version.getTopicLengthSize();
		int topicLength = version.getTopicLength(byteBuffer);

		int propertiesPosition = topicLengthPosition + topicLengthSize + topicLength;
		short propertiesLength = byteBuffer.getShort(propertiesPosition);
		byteBuffer.position(propertiesPosition + 2);

		if (propertiesLength > 0) {
			byte[] properties = new byte[propertiesLength];
			byteBuffer.get(properties);
			String propertiesString = new String(properties, CHARSET_UTF8);
			return string2messageProperties(propertiesString);
		}
		return null;
	}

	public static void createCrc32(final ByteBuffer input, int crc32) {
		input.put(MessageConst.PROPERTY_CRC32.getBytes(StandardCharsets.UTF_8));
		input.put((byte) NAME_VALUE_SEPARATOR);
		for (int i = 0; i < 10; i++) {
			byte b = '0';
			if (crc32 > 0) {
				b += (byte) (crc32 % 10);
				crc32 /= 10;
			}
			input.put(b);
		}
		input.put((byte) PROPERTY_SEPARATOR);
	}

	public static void createCrc32(final ByteBuf input, int crc32) {
		input.writeBytes(MessageConst.PROPERTY_CRC32.getBytes(StandardCharsets.UTF_8));
		input.writeByte((byte) NAME_VALUE_SEPARATOR);
		for (int i = 0; i < 10; i++) {
			byte b = '0';
			if (crc32 > 0) {
				b += (byte) (crc32 % 10);
				crc32 /= 10;
			}
			input.writeByte(b);
		}
		input.writeByte((byte) PROPERTY_SEPARATOR);
	}

	public static MessageExt clientDecode(ByteBuffer byteBuffer, final boolean readBody) {
		return decode(byteBuffer, readBody, true);
	}

	public static MessageExt decode(ByteBuffer byteBuffer) {
		return decode(byteBuffer, true, true, false);
	}

	public static MessageExt decode(ByteBuffer byteBuffer, final boolean readBody) {
		return decode(byteBuffer, readBody, true, false);
	}

	public static MessageExt decode(ByteBuffer byteBuffer, final boolean readBody, final boolean deCompressBody) {
		return decode(byteBuffer, readBody, deCompressBody, false);
	}

	public static MessageExt decode(ByteBuffer byteBuffer, final boolean readBody, final boolean deCompressBody, final boolean isClient) {
		return decode(byteBuffer, readBody, deCompressBody, isClient, false, false);
	}

	public static MessageExt decode(ByteBuffer byteBuffer, final boolean readBody, final boolean deCompressBody, final boolean isClient, final boolean isSetPropertiesString) {
		return decode(byteBuffer, readBody, deCompressBody, isClient, isSetPropertiesString, false);
	}

	public static MessageExt decode(ByteBuffer byteBuffer, final boolean readBody, final boolean deCompressBody, final boolean isClient, final boolean isSetPropertiesString, final boolean checkCRC) {
		try {
			MessageExt msgExt;
			if (isClient) {
				msgExt = new MessageClientExt();
			}
			else {
				msgExt = new MessageExt();
			}

			int totalSize = byteBuffer.getInt();
			msgExt.setStoreSize(totalSize);

			int magicCode = byteBuffer.getInt();
			MessageVersion version = MessageVersion.valueOfMagicCode(magicCode);

			int bodyCRC = byteBuffer.getInt();
			msgExt.setBodyCRC(bodyCRC);

			int queueId = byteBuffer.getInt();
			msgExt.setQueueId(queueId);

			int flag = byteBuffer.getInt();
			msgExt.setFlag(flag);

			long queueOffset = byteBuffer.getLong();
			msgExt.setQueueOffset(queueOffset);

			long physicOffset = byteBuffer.getLong();
			msgExt.setCommitLogOffset(physicOffset);

			int sysFlag = byteBuffer.getInt();
			msgExt.setSysFlag(sysFlag);

			long bornTimestamp = byteBuffer.getLong();
			msgExt.setBornTimestamp(bornTimestamp);

			int bornHostIPLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 : 16;
			byte[] bornHost = new byte[bornHostIPLength];
			byteBuffer.get(bornHost, 0, bornHostIPLength);
			int port = byteBuffer.getInt();
			msgExt.setBornHost(new InetSocketAddress(InetAddress.getByAddress(bornHost), port));

			long storeTimestamp = byteBuffer.getLong();
			msgExt.setStoreTimestamp(storeTimestamp);

			int storeHostIPLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 : 16;
			byte[] storeHost = new byte[storeHostIPLength];
			byteBuffer.get(storeHost, 0, storeHostIPLength);
			port = byteBuffer.getInt();
			msgExt.setStoreHost(new InetSocketAddress(InetAddress.getByAddress(storeHost), port));

			int reconsumeTimes = byteBuffer.getInt();
			msgExt.setReconsumeTimes(reconsumeTimes);

			long preparedTransactionOffset = byteBuffer.getLong();
			msgExt.setPreparedTransactionOffset(preparedTransactionOffset);

			int bodyLen = byteBuffer.getInt();
			if (bodyLen > 0) {
				if (readBody) {
					byte[] body = new byte[bodyLen];
					byteBuffer.get(body);

					if (checkCRC) {
						int crc = UtilAll.crc32(body, 0, bodyLen);
						if (crc != bodyCRC) {
							throw new Exception("Msg crc is error!");
						}
					}

					if (deCompressBody && (sysFlag & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG) {
						Compressor compressor = CompressorFactory.getCompressor(MessageSysFlag.getCompressionType(sysFlag));
						body = compressor.decompress(body);
					}

					msgExt.setBody(body);
				}
				else {
					byteBuffer.position(byteBuffer.position() + bodyLen);
				}
			}

			int topicLen = version.getTopicLength(byteBuffer);
			byte[] topic = new byte[topicLen];
			byteBuffer.get(topic);
			msgExt.setTopic(new String(topic, CHARSET_UTF8));

			short propertiesLength = byteBuffer.getShort();
			if (propertiesLength > 0) {
				byte[] properties = new byte[propertiesLength];
				byteBuffer.get(properties);
				String propertiesString = new String(properties, CHARSET_UTF8);
				if (!isSetPropertiesString) {
					Map<String, String> map = string2messageProperties(propertiesString);
					msgExt.setProperties(map);
				}
				else {
					Map<String, String> map = string2messageProperties(propertiesString);
					map.put("propertiesString", propertiesString);
					msgExt.setProperties(map);
				}
			}

			int msgIDLength = storeHostIPLength + 4 + 8;
			ByteBuffer byteBufferMsgId = ByteBuffer.allocate(msgIDLength);
			String msgId = createMessageId(byteBufferMsgId, msgExt.getStoreHostBytes(), msgExt.getCommitLogOffset());
			msgExt.setMsgId(msgId);

			if (isClient) {
				((MessageClientExt) msgExt).setOffsetMsgId(msgId);
			}

			return msgExt;
		}
		catch (Exception e) {
			byteBuffer.position(byteBuffer.limit());
		}

		return null;
	}


	public static byte[] encode(MessageExt messageExt, boolean needCompress) throws IOException {
		byte[] body = messageExt.getBody();
		byte[] topics = messageExt.getTopic().getBytes(CHARSET_UTF8);
		byte topicLen = (byte) topics.length;
		String properties = messageProperties2String(messageExt.getProperties());
		byte[] propertiesBytes = properties.getBytes(CHARSET_UTF8);
		short propertiesLength = (short) propertiesBytes.length;
		int sysFlag = messageExt.getSysFlag();
		int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
		int storeHostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
		byte[] newBody = messageExt.getBody();
		if (needCompress && (sysFlag & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG) {
			Compressor compressor = CompressorFactory.getCompressor(MessageSysFlag.getCompressionType(sysFlag));
			newBody = compressor.compress(body, 5);
		}
		int bodyLength = newBody.length;
		int storeSize = messageExt.getStoreSize();
		ByteBuffer byteBuffer;
		if (storeSize > 0) {
			byteBuffer = ByteBuffer.allocate(storeSize);
		}
		else {
			storeSize = 4
			            + 4
			            + 4
			            + 4
			            + 4
			            + 8
			            + 8
			            + 4
			            + 8
			            + bornHostLength
			            + 8
			            + storeHostAddressLength
			            + 4
			            + 8
			            + 4 + bodyLength
			            + 1 + topicLen
			            + 2 + propertiesLength
			            + 0;
			byteBuffer = ByteBuffer.allocate(storeSize);
		}

		byteBuffer.putInt(storeSize);

		byteBuffer.putInt(MESSAGE_MAGIC_CODE);

		int bodyCRC = messageExt.getBodyCRC();
		byteBuffer.putInt(bodyCRC);

		int queueId = messageExt.getQueueId();
		byteBuffer.putInt(queueId);

		int flag = messageExt.getFlag();
		byteBuffer.putInt(flag);

		long queueOffset = messageExt.getQueueOffset();
		byteBuffer.putLong(queueOffset);

		byteBuffer.putInt(sysFlag);

		long bornTimestamp = messageExt.getBornTimestamp();
		byteBuffer.putLong(bornTimestamp);

		InetSocketAddress bornHost = (InetSocketAddress) messageExt.getBornHost();
		byteBuffer.put(bornHost.getAddress().getAddress());
		byteBuffer.putInt(bornHost.getPort());

		long storeTimestamp = messageExt.getStoreTimestamp();
		byteBuffer.putLong(storeTimestamp);

		InetSocketAddress serverHost = (InetSocketAddress) messageExt.getStoreHost();
		byteBuffer.put(serverHost.getAddress().getAddress());
		byteBuffer.putInt(serverHost.getPort());

		int reconsumeTimes = messageExt.getReconsumeTimes();
		byteBuffer.putInt(reconsumeTimes);

		long preparedTransactionOffset = messageExt.getPreparedTransactionOffset();
		byteBuffer.putLong(preparedTransactionOffset);

		byteBuffer.putInt(bodyLength);
		byteBuffer.put(newBody);

		byteBuffer.put(topicLen);
		byteBuffer.put(topics);

		byteBuffer.putShort(propertiesLength);
		byteBuffer.put(propertiesBytes);

		return byteBuffer.array();
	}

	public static byte[] encodeUniquely(MessageExt messageExt, boolean needCompress) throws IOException {
		byte[] body = messageExt.getBody();
		byte[] topics = messageExt.getTopic().getBytes(CHARSET_UTF8);
		byte topicLen = (byte) topics.length;
		String properties = messageProperties2String(messageExt.getProperties());
		byte[] propertiesBytes = properties.getBytes(CHARSET_UTF8);
		short propertiesLength = (short) propertiesBytes.length;
		int sysFlag = messageExt.getSysFlag();
		int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
		byte[] newBody = messageExt.getBody();
		if (needCompress && (sysFlag & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG) {
			newBody = UtilAll.compress(body, 0);
		}
		int bodyLength = newBody.length;
		int storeSize = messageExt.getStoreSize();
		ByteBuffer byteBuffer;
		if (storeSize > 0) {
			byteBuffer = ByteBuffer.allocate(storeSize - 8);
		}
		else {
			storeSize = 4
					+ 4
					+ 4
					+ 4
					+ 4
					+ 8
					+ 8
					+ 4
					+ 8
					+ bornHostLength
					+ 4
					+ 8
					+ 4 + bodyLength
					+ 1 + topicLen
					+ 2
					+ propertiesLength;

			byteBuffer = ByteBuffer.allocate(storeSize);
		}

		byteBuffer.putInt(storeSize);

		byteBuffer.putInt(MESSAGE_MAGIC_CODE);

		byteBuffer.putInt(messageExt.getBodyCRC());

		byteBuffer.putInt(messageExt.getQueueId());

		byteBuffer.putInt(messageExt.getFlag());

		byteBuffer.putLong(messageExt.getQueueOffset());

		byteBuffer.putLong(messageExt.getCommitLogOffset());

		byteBuffer.putInt(sysFlag);

		byteBuffer.putLong(messageExt.getBornTimestamp());

		InetSocketAddress bornHost = (InetSocketAddress) messageExt.getBornHost();
		byteBuffer.put(bornHost.getAddress().getAddress());
		byteBuffer.putInt(bornHost.getPort());

		byteBuffer.putInt(messageExt.getReconsumeTimes());

		byteBuffer.putLong(messageExt.getPreparedTransactionOffset());

		byteBuffer.putInt(bodyLength);
		byteBuffer.put(body);

		byteBuffer.putShort(propertiesLength);
		byteBuffer.put(propertiesBytes);

		return byteBuffer.array();
	}

	public static String messageProperties2String(Map<String, String> properties) {
		if (properties == null) {
			return "";
		}

		int len = 0;
		for (Map.Entry<String, String> entry : properties.entrySet()) {
			String name = entry.getKey();
			String value = entry.getValue();

			if (value == null) {
				continue;
			}

			if (name != null) {
				len += name.length();
			}

			len += value.length();
			len += 2; // separator
		}

		StringBuilder sb = new StringBuilder(len);
		for (Map.Entry<String, String> entry : properties.entrySet()) {
			String name = entry.getKey();
			String value = entry.getValue();

			if (value == null) {
				continue;
			}

			sb.append(name);
			sb.append(NAME_VALUE_SEPARATOR);
			sb.append(value);
			sb.append(PROPERTY_SEPARATOR);
		}
		return sb.toString();
	}

	public static Map<String, String> string2messageProperties(final String properties) {
		Map<String, String> map = new HashMap<>(128);
		if (properties != null) {
			int len = properties.length();
			int index = 0;

			while (index < len) {
				int newIndex = properties.indexOf(PROPERTY_SEPARATOR, index);
				if (newIndex < 0) {
					newIndex = len;
				}
				if (newIndex - index >= 3) {
					int kvSepIndex = properties.indexOf(NAME_VALUE_SEPARATOR, index);
					if (kvSepIndex > index && kvSepIndex < newIndex - 1) {
						String k = properties.substring(index, kvSepIndex);
						String v = properties.substring(kvSepIndex + 1, newIndex);
						map.put(k, v);
					}
				}
				index = newIndex + 1;
			}
		}
		return map;
	}

	public static byte[] encodeMessage(Message message) {
		byte[] body = message.getBody();
		int bodyLen = body.length;
		String properties = messageProperties2String(message.getProperties());
		byte[] propertiesBytes = properties.getBytes(CHARSET_UTF8);

		short propertiesLength = (short) propertiesBytes.length;
		int storeSize = 4
		                + 4
		                + 4
		                + 4
		                + 4 + bodyLen
		                + 2 + propertiesLength;

		ByteBuffer byteBuffer = ByteBuffer.allocate(storeSize);

		byteBuffer.putInt(storeSize);

		byteBuffer.putInt(0);

		byteBuffer.putInt(0);

		byteBuffer.putInt(message.getFlag());

		byteBuffer.putInt(bodyLen);
		byteBuffer.put(body);

		byteBuffer.putShort(propertiesLength);
		byteBuffer.put(propertiesBytes);

		return byteBuffer.array();
	}

	public static Message decodeMessage(ByteBuffer byteBuffer) {
		Message message = new Message();

		int position = byteBuffer.position();
		byteBuffer.position(position + 4 + 4 + 4);

//		byteBuffer.getInt();

//		byteBuffer.getInt();

//		byteBuffer.getInt();

		int flag = byteBuffer.getInt();
		message.setFlag(flag);

		int bodyLen = byteBuffer.getInt();
		byte[] body = new byte[bodyLen];
		byteBuffer.get(body);
		message.setBody(body);

		short propertiesLen = byteBuffer.getShort();
		byte[] propertiesBytes = new byte[propertiesLen];
		byteBuffer.put(propertiesBytes);
		message.setProperties(string2messageProperties(new String(propertiesBytes, CHARSET_UTF8)));

		return message;
	}

	public static byte[] encodeMessages(List<Message> messages) {
		List<byte[]> encodedMessages = new ArrayList<>(messages.size());
		int allSize = 0;
		for (Message message : messages) {
			byte[] temp = encodeMessage(message);
			encodedMessages.add(temp);
			allSize += temp.length;
		}

		byte[] allBytes = new byte[allSize];
		int pos = 0;
		for (byte[] bytes : encodedMessages) {
			System.arraycopy(bytes,0,allBytes,pos,bytes.length );
			pos += bytes.length;
		}
		return allBytes;
	}

	public static List<Message> decodeMessages(ByteBuffer byteBuffer) {
		List<Message> msgs = new ArrayList<>();
		while (byteBuffer.hasRemaining()) {
			Message msg = decodeMessage(byteBuffer);
			msgs.add(msg);
		}
		return msgs;
	}

	public static void decodeMessages(MessageExt messageExt, List<MessageExt> list) {
		List<Message> messages = MessageDecoder.decodeMessages(ByteBuffer.wrap(messageExt.getBody()));

		for (int i = 0; i < messages.size(); i++) {
			Message message = messages.get(i);
			MessageClientExt messageClientExt = new MessageClientExt();
			messageClientExt.setTopic(messageExt.getTopic());
			messageClientExt.setQueueOffset(messageExt.getQueueOffset() - 1);
			messageClientExt.setQueueId(messageExt.getQueueId());
			messageClientExt.setFlag(message.getFlag());
			MessageAccessor.setProperties(messageClientExt, message.getProperties());
			messageClientExt.setBody(message.getBody());
			messageClientExt.setBornHost(messageExt.getBornHost());
			messageClientExt.setBornTimestamp(messageExt.getBornTimestamp());
			messageClientExt.setStoreHost(messageExt.getStoreHost());
			messageClientExt.setStoreTimestamp(messageExt.getStoreTimestamp());
			messageClientExt.setSysFlag(messageExt.getSysFlag());
			messageClientExt.setCommitLogOffset(messageExt.getCommitLogOffset());
			messageClientExt.setWaitStoreMsgOK(messageExt.isWaitStoreMsgOK());
			list.add(messageClientExt);
		}
	}

	public static int countInnerMsgNum(ByteBuffer byteBuffer) {
		int count = 0;
		while (byteBuffer.hasRemaining()) {
			count++;

			int currPos = byteBuffer.position();
			int size = byteBuffer.getInt();
			byteBuffer.position(currPos + size);
		}

		return count;
	}
}