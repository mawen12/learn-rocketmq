package com.mawen.learn.rocketmq.remoting.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import io.netty.buffer.ByteBuf;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public class RocketMQSerializable {

	private static final Charset CHARSET_UTF8 = StandardCharsets.UTF_8;

	public static void writeStr(ByteBuf buf, boolean useShortLength, String str) {
		int index = buf.writerIndex();
		if (useShortLength) {
			buf.writeShort(0);
		}
		else {
			buf.writeInt(0);
		}

		int len = buf.writeCharSequence(str, StandardCharsets.UTF_8);
		if (useShortLength) {
			buf.setShort(index, len);
		}
		else {
			buf.setInt(index, len);
		}
	}

	public static String readStr(ByteBuf buf, boolean useShortLength, int limit) throws RemotingCommandException {
		int len = useShortLength ? buf.readShort() : buf.readInt();
		if (len == 0) {
			return null;
		}

		if (len > limit) {
			throw new RemotingCommandException("string length exceed limit:" + limit);
		}
		CharSequence cs = buf.readCharSequence(len, StandardCharsets.UTF_8);
		return cs == null ? null : cs.toString();
	}

	public static int rocketMQProtocolEncode(RemotingCommand cmd, ByteBuf out) {
		int beginIndex = out.writerIndex();

		out.writeShort(cmd.getCode());

		out.writeByte(cmd.getLanguage().getCode());

		out.writeShort(cmd.getVersion());

		out.writeInt(cmd.getOpaque());

		out.writeInt(cmd.getFlag());

		String remark = cmd.getRemark();
		if (remark != null && !remark.isEmpty()) {
			writeStr(out, false, remark);
		}
		else {
			out.writeInt(0);
		}

		int mapLenIndex = out.writerIndex();
		out.writeInt(0);
		if (cmd.readCustomHeader() instanceof FastCodesHeader) {
			((FastCodesHeader) cmd.readCustomHeader()).encode(out);
		}

		Map<String, String> map = cmd.getExtFields();
		if (map != null && !map.isEmpty()) {
			map.forEach((k, v) -> {
				if (k != null && v != null) {
					writeStr(out, true, k);
					writeStr(out, false, v);
				}
			});
		}

		out.setInt(mapLenIndex, out.writerIndex() - mapLenIndex - 4);
		return out.writerIndex() - beginIndex;
	}

	public static byte[] rocketMQProtocolEncode(RemotingCommand cmd) {
		byte[] remarkBytes = null;
		int remarkLen = 0;

		if (cmd.getRemark() != null && cmd.getRemark().length() > 0) {
			remarkBytes = cmd.getRemark().getBytes(CHARSET_UTF8);
			remarkLen = remarkBytes.length;
		}

		byte[] extFieldsBytes = null;
		int extLen = 0;
		if (cmd.getExtFields() != null && !cmd.getExtFields().isEmpty()) {
			extFieldsBytes = mapSerialize(cmd.getExtFields());
			extLen = extFieldsBytes.length;
		}

		int totalLen = calTotalLen(remarkLen, extLen);

		ByteBuffer headerBuffer = ByteBuffer.allocate(totalLen);

		headerBuffer.putShort((short) cmd.getCode());
		headerBuffer.put(cmd.getLanguage().getCode());
		headerBuffer.putShort((short) cmd.getVersion());
		headerBuffer.putInt(cmd.getOpaque());
		headerBuffer.putInt(cmd.getFlag());

		if (remarkBytes != null) {
			headerBuffer.putInt(remarkLen);
			headerBuffer.put(remarkBytes);
		}
		else {
			headerBuffer.putInt(0);
		}

		if (extFieldsBytes != null) {
			headerBuffer.putInt(extLen);
			headerBuffer.put(extFieldsBytes);
		}
		else {
			headerBuffer.putInt(0);
		}

		return headerBuffer.array();
	}

	public static byte[] mapSerialize(Map<String, String> map) {
		if (map == null || map.isEmpty()) {
			return null;
		}

		int totalLength = 0;
		int kvLength;
		Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> entry = it.next();
			if (entry.getKey() != null && entry.getValue() != null) {
				kvLength = 2 + entry.getKey().getBytes(CHARSET_UTF8).length
						+ 4 + entry.getValue().getBytes(CHARSET_UTF8).length;
				totalLength += kvLength;
			}
		}

		ByteBuffer content = ByteBuffer.allocate(totalLength);
		byte[] key;
		byte[] val;
		it = map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> entry = it.next();
			if (entry.getKey() != null && entry.getValue() != null) {
				key = entry.getKey().getBytes(CHARSET_UTF8);
				val = entry.getKey().getBytes(CHARSET_UTF8);

				content.putShort((short) key.length);
				content.put(key);

				content.putInt(val.length);
				content.put(val);
			}
		}

		return content.array();
	}

	private static int calTotalLen(int remark, int ext) {
		return  2 + 1 + 2 + 4 + 4 + 4 + remark + 4 + ext;
	}

	public static RemotingCommand rocketMQProtocolDecode(final ByteBuf buf, int headerLen) throws RemotingCommandException {
		RemotingCommand cmd = new RemotingCommand();

		cmd.setCode(buf.readShort());

		cmd.setLanguage(LanguageCode.valueOf(buf.readByte()));

		cmd.setVersion(buf.readShort());

		cmd.setOpaque(buf.readInt());

		cmd.setFlag(buf.readInt());

		cmd.setRemark(readStr(buf, false, headerLen));

		int extFieldsLength = buf.readInt();
		if (extFieldsLength > 0) {
			if (extFieldsLength > headerLen) {
				throw new RemotingCommandException("RocketMQ protocol decoding failed, extFields length: " + extFieldsLength);
			}
			cmd.setExtFields(mapDeserialize(buf, extFieldsLength));
		}
		return cmd;
	}

	public static Map<String, String> mapDeserialize(ByteBuf buf, int len) throws RemotingCommandException {
		Map<String, String> map = new HashMap<>(128);

		int endIndex = buf.readerIndex() + len;
		while (buf.readerIndex() < endIndex) {
			String k = readStr(buf, true, len);
			String v = readStr(buf, false, len);
			map.put(k, v);
		}
		return map;
	}
}
