package com.mawen.learn.rocketmq.remoting.protocol;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.base.Stopwatch;
import com.mawen.learn.rocketmq.common.BoundaryType;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public class RemotingCommand {
	public static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

	public static final String SERIALIZE_TYPE_PROPERTY = "rocketmq.serialize.type";
	public static final String SERIALIZE_TYPE_ENV = "ROCKETMQ_SERIALIZE_TYPE";
	public static final String REMOTING_VERSION_KEY = "rocketmq.remoting.version";

	private static final int RPC_TYPE = 0;
	private static final int RPC_ONEWAY = 1;
	private static final Map<Class<? extends CommandCustomHeader>, Field[]> CLASS_HASH_MAP = new HashMap<>();
	private static final Map<Class, String> CANONICAL_NAME_CACHE = new HashMap<>();
	private static final Map<Field, Boolean> NULLABLE_FIELD_CACHE = new HashMap<>();
	private static final String STRING_CANONICAL_NAME = String.class.getCanonicalName();
	private static final String DOUBLE_CANONICAL_NAME_1 = Double.class.getCanonicalName();
	private static final String DOUBLE_CANONICAL_NAME_2 = double.class.getCanonicalName();
	private static final String INTEGER_CANONICAL_NAME_1 = Integer.class.getCanonicalName();
	private static final String INTEGER_CANONICAL_NAME_2 = int.class.getCanonicalName();
	private static final String LONG_CANONICAL_NAME_1 = Long.class.getCanonicalName();
	private static final String LONG_CANONICAL_NAME_2 = long.class.getCanonicalName();
	private static final String BOOLEAN_CANONICAL_NAME_1 = Boolean.class.getCanonicalName();
	private static final String BOOLEAN_CANONICAL_NAME_2 = boolean.class.getCanonicalName();
	private static final String BOUNDARY_TYPE_CANONICAL_NAME = BoundaryType.class.getCanonicalName();

	private static volatile int configVersion = 1;
	private static AtomicInteger requestId = new AtomicInteger(0);
	private static SerializeType serializeTypeConfigInThisServer = SerializeType.JSON;

	static {
		final String protocol = System.getProperty(SERIALIZE_TYPE_PROPERTY, System.getenv(SERIALIZE_TYPE_ENV));
		if (!StringUtils.isBlank(protocol)) {
			try {
				serializeTypeConfigInThisServer = SerializeType.valueOf(protocol);
			}
			catch (IllegalArgumentException e) {
				throw new RuntimeException("parser specified protocol error. protocol=" + protocol, e);
			}
		}
	}

	private int code;
	private LanguageCode language = LanguageCode.JAVA;
	private int version;
	private int opaque = requestId.getAndIncrement();
	private int flag;
	private String remark;
	private Map<String, String> extFields;
	private transient CommandCustomHeader customHeader;
	private transient CommandCustomHeader cachedHeader;
	private SerializeType serializeTypeCurrentRPC = serializeTypeConfigInThisServer;

	private transient byte[] body;
	private boolean suspended;
	private transient Stopwatch processTimer;

	protected RemotingCommand() {}

	public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {
		RemotingCommand cmd = new RemotingCommand();
		cmd.setCode(code);
		cmd.customHeader = customHeader;
		setCmdVersion(cmd);
		return cmd;
	}

	public static RemotingCommand createResponseCommandWithHeader(int code, CommandCustomHeader customHeader) {
		RemotingCommand cmd = new RemotingCommand();
		cmd.setCode(code);
		cmd.markResponseType();
		cmd.customHeader =customHeader;
		setCmdVersion(cmd);
		return cmd;
	}

	public static RemotingCommand createResponseCommand(Class<? extends CommandCustomHeader> classHeader) {
		return createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "not yet any response code", classHeader);
	}

	public static RemotingCommand buildErrorResponse(int code, String remark, Class<? extends CommandCustomHeader> classHeader) {
		RemotingCommand response = RemotingCommand.createResponseCommand(classHeader);
		response.setCode(code);
		response.setRemark(remark);
		return response;
	}

	public static RemotingCommand buildErrorResponse(int code, String remark) {
		return buildErrorResponse(code, remark, null);
	}

	public static RemotingCommand createResponseCommand(int code, String remark, Class<? extends CommandCustomHeader> classHeader) {
		RemotingCommand cmd = new RemotingCommand();
		cmd.markResponseType();
		cmd.setCode(code);
		cmd.setRemark(remark);
		setCmdVersion(cmd);

		if (classHeader != null) {
			try {
				CommandCustomHeader objectHeader = classHeader.getDeclaredConstructor().newInstance();
				cmd.customHeader = objectHeader;
			}
			catch (InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
				return null;
			}
		}

		return cmd;
	}

	public static RemotingCommand createResponseCommand(int code, String remark) {
		return createResponseCommand(code, remark, null);
	}

	public static RemotingCommand decode(final byte[] array) throws RemotingCommandException {
		return decode(ByteBuffer.wrap(array));
	}

	public static RemotingCommand decode(final ByteBuffer buffer) throws RemotingCommandException {
		return decode(Unpooled.wrappedBuffer(buffer));
	}

	public static RemotingCommand decode(final ByteBuf buf) throws RemotingCommandException {
		int length = buf.readableBytes();

		int oriHeaderLength = buf.readInt();
		int headerLength = getHeaderLength(oriHeaderLength);
		if (headerLength > length - 4) {
			throw new RemotingCommandException("decode error, bad header length: " + headerLength);
		}

		RemotingCommand cmd = headerDecode(buf, headerLength, getProtocolType(oriHeaderLength));

		int bodyLength = length - 4 - headerLength;
		byte[] bodyData = null;
		if (bodyLength > 0) {
			bodyData = new byte[bodyLength];
			buf.readBytes(bodyData);
		}
		cmd.body = bodyData;

		return cmd;
	}

	public static int getHeaderLength(int length) {
		return length & 0xFFFFFF;
	}

	private static RemotingCommand headerDecode(ByteBuf buf, int len, SerializeType type) throws RemotingCommandException {
		switch (type) {
			case JSON:
				byte[] headerData = new byte[len];
				buf.readBytes(headerData);
				RemotingCommand resultJson = RemotingSerializable.decode(headerData, RemotingCommand.class);
				resultJson.setSerializeTypeCurrentRPC(type);
				return resultJson;
			case ROCKETMQ:
				RemotingCommand resultRMQ = RocketMQSerializable.rocketMQProtocolDecode(buf, len);
				resultRMQ.setSerializeTypeCurrentRPC(type);
				return resultRMQ;
			default:
				break;
		}

		return null;
	}

	public static SerializeType getProtocolType(int source) {
		return SerializeType.valueOf((byte) ((source >> 24) & 0xFF));
	}

	public static int createNewRequestId() {
		return requestId.getAndIncrement();
	}

	public static SerializeType getSerializeTypeConfigInThisServer() {
		return serializeTypeConfigInThisServer;
	}

	public static int markProtocolType(int source, SerializeType type) {
		return type.getCode() << 24 | (source & 0x00FFFFFF);
	}

	public void markResponseType() {
		int bits = 1 << RPC_TYPE;
		this.flag |= bits;
	}

	public CommandCustomHeader readCustomHeader() {
		return customHeader;
	}

	public void setCustomHeader(CommandCustomHeader customHeader) {
		this.customHeader = customHeader;
	}

	public <T extends CommandCustomHeader> T decodeCommandCustomHeader(Class<T> classHeader) throws RemotingCommandException {
		return decodeCommandCustomHeader(classHeader, false);
	}

	public <T extends CommandCustomHeader> T decodeCommandCustomHeader(Class<T> classHeader, boolean isCached) throws RemotingCommandException {
		if (isCached && cachedHeader != null) {
			return classHeader.cast(cachedHeader);
		}

		cachedHeader = decodeCommandCustomHeaderDirectly(classHeader, true);
		if (cachedHeader == null) {
			return null;
		}

		return classHeader.cast(cachedHeader);
	}

	public <T extends CommandCustomHeader> T decodeCommandCustomHeaderDirectly(Class<T> classHeader, boolean useFastEncode) throws RemotingCommandException {
		T objectHeader;
		try {
			objectHeader = classHeader.getDeclaredConstructor().newInstance();
		}
		catch (Exception e) {
			return null;
		}

		if (this.extFields != null) {
			if (objectHeader instanceof FastCodesHeader && useFastEncode) {
				((FastCodesHeader) objectHeader).decode(this.extFields);
				objectHeader.checkFields();
				return objectHeader;
			}

			Field[] fields = getClazzFields(classHeader);
			for (Field field : fields) {
				if (!Modifier.isStatic(field.getModifiers())) {
					String name = field.getName();
					if (!name.startsWith("this")) {
						try {
							String value = this.extFields.get(name);
							if (value == null) {
								if (!isFieldNullable(field)) {
									throw new RemotingCommandException("the custom field <" + name + "> is null");
								}
								continue;
							}

							field.setAccessible(true);
							String type = getCanonicalName(field.getType());
							Object valueParsed;

							if (type.equals(STRING_CANONICAL_NAME)) {
								valueParsed = value;
							}
							else if (type.equals(INTEGER_CANONICAL_NAME_1) || type.equals(INTEGER_CANONICAL_NAME_2)) {
								valueParsed = Integer.parseInt(value);
							}
							else if (type.equals(LONG_CANONICAL_NAME_1) || type.equals(LONG_CANONICAL_NAME_2)) {
								valueParsed = Long.parseLong(value);
							}
							else if (type.equals(BOOLEAN_CANONICAL_NAME_1) || type.equals(BOOLEAN_CANONICAL_NAME_2)) {
								valueParsed = Boolean.parseBoolean(value);
							}
							else if (type.equals(DOUBLE_CANONICAL_NAME_1) || type.equals(DOUBLE_CANONICAL_NAME_2)) {
								valueParsed = Double.parseDouble(value);
							}
							else if (type.equals(BOUNDARY_TYPE_CANONICAL_NAME)) {
								valueParsed = BoundaryType.getType(value);
							}
							else {
								throw new RemotingCommandException("the custom field <" + name + "> type is not supported");
							}

							field.set(objectHeader, valueParsed);
						}
						catch (Throwable e) {
							log.error("Failed field [{}] decoding", name, e);
						}
					}
				}
			}

			objectHeader.checkFields();
		}

		return objectHeader;
	}

	public void fastEncodeHeader(ByteBuf out) {
		int bodySize = this.body != null ? this.body.length : 0;
		int beginIndex = out.writerIndex();

		out.writeLong(0);

		int headerSize;
		if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC) {
			if (customHeader != null && !(customHeader instanceof FastCodesHeader)) {
				this.makeCustomHeaderToNet();
			}
			headerSize = RocketMQSerializable.rocketMQProtocolEncode(this, out);
		}
		else {
			this.makeCustomHeaderToNet();
			byte[] header = RemotingSerializable.encode(this);
			headerSize = header.length;
			out.writeBytes(header);
		}

		out.setInt(beginIndex, 4 + headerSize + bodySize);
		out.setInt(beginIndex + 4, markProtocolType(headerSize, serializeTypeCurrentRPC));
	}

	public void makeCustomHeaderToNet() {
		if (this.customHeader != null) {
			Field[] fields = getClazzFields(customHeader.getClass());
			if (this.extFields == null) {
				this.extFields = new HashMap<>();
			}

			for (Field field : fields) {
				if (!Modifier.isStatic(field.getModifiers())) {
					String name = field.getName();
					if (!name.startsWith("this")) {
						Object value = null;
						try {
							field.setAccessible(true);
							value = field.get(this.customHeader);
						}
						catch (Exception e) {
							log.error("Failed to access field [{}]", name, e);
						}

						if (value != null) {
							this.extFields.put(name, value.toString());
						}
					}
				}
			}
		}
	}

	public ByteBuffer encode() {
		int length = 4;

		byte[] headerData = this.headerEncode();
		length += headerData.length;

		if (this.body != null) {
			length += body.length;
		}

		ByteBuffer result = ByteBuffer.allocate(4 + length);

		result.putInt(length);

		result.putInt(markProtocolType(headerData.length, serializeTypeCurrentRPC));

		result.put(headerData);

		if (this.body != null) {
			result.put(this.body);
		}

		result.flip();

		return result;
	}

	public ByteBuffer encodeHeader() {
		return encodeHeader(this.body != null ? this.body.length : 0);
	}

	public ByteBuffer encodeHeader(final int bodyLength) {
		int length = 4;

		byte[] headerData = this.headerEncode();
		length += headerData.length;

		length += bodyLength;

		ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

		result.putInt(length);
		result.putInt(markProtocolType(headerData.length, serializeTypeCurrentRPC));
		result.put(headerData);

		result.flip();

		return result;
	}

	public void markOnewayRPC() {
		int bits = 1 << RPC_ONEWAY;
		this.flag |= bits;
	}

	public boolean isOnewayRPC() {
		int bits = 1 << RPC_ONEWAY;
		return (this.flag & bits) == bits;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	@JSONField(serialize = false)
	public RemotingCommandType getType() {
		if (this.isResponseType()) {
			return RemotingCommandType.RESPONSE_COMMAND;
		}

		return RemotingCommandType.REQUEST_COMMAND;
	}

	@JSONField(serialize = false)
	public boolean isResponseType() {
		int bits = 1 << RPC_TYPE;
		return (this.flag & bits) == bits;
	}

	public LanguageCode getLanguage() {
		return language;
	}

	public void setLanguage(LanguageCode language) {
		this.language = language;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public int getOpaque() {
		return opaque;
	}

	public void setOpaque(int opaque) {
		this.opaque = opaque;
	}

	public int getFlag() {
		return flag;
	}

	public void setFlag(int flag) {
		this.flag = flag;
	}

	public String getRemark() {
		return remark;
	}

	public void setRemark(String remark) {
		this.remark = remark;
	}

	public byte[] getBody() {
		return body;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}

	@JSONField(serialize = false)
	public boolean isSuspended() {
		return suspended;
	}

	@JSONField(serialize = false)
	public void setSuspended(boolean suspended) {
		this.suspended = suspended;
	}

	public Map<String, String> getExtFields() {
		return extFields;
	}

	public void setExtFields(Map<String, String> extFields) {
		this.extFields = extFields;
	}

	public void addExtField(String key, String value) {
		if (extFields == null) {
			extFields = new HashMap<>(256);
		}
		extFields.put(key, value);
	}

	public void addExtFieldIfNotExist(String key, String value) {
		extFields.putIfAbsent(key, value);
	}

	public SerializeType getSerializeTypeCurrentRPC() {
		return serializeTypeCurrentRPC;
	}

	public void setSerializeTypeCurrentRPC(SerializeType serializeTypeCurrentRPC) {
		this.serializeTypeCurrentRPC = serializeTypeCurrentRPC;
	}

	public Stopwatch getProcessTimer() {
		return processTimer;
	}

	public void setProcessTimer(Stopwatch processTimer) {
		this.processTimer = processTimer;
	}

	@Override
	public String toString() {
		return "RemotingCommand [code=" + code + ", language=" + language + ", version=" + version + ", opaque=" + opaque + ", flag(B)="
				+ Integer.toBinaryString(flag) + ", remark=" + remark + ", extFields=" + extFields + ", serializeTypeCurrentRPC="
				+ serializeTypeCurrentRPC + "]";
	}

	protected static void setCmdVersion(RemotingCommand cmd) {
		if (configVersion >= 0) {
			cmd.setVersion(configVersion);
		}
		else {
			String v = System.getProperty(REMOTING_VERSION_KEY);
			if (v != null) {
				int value = Integer.parseInt(v);
				cmd.setVersion(value);
				configVersion = value;
			}
		}
	}

	private byte[] headerEncode() {
		this.makeCustomHeaderToNet();
		if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC) {
			return RocketMQSerializable.rocketMQProtocolEncode(this);
		}
		else {
			return RemotingSerializable.encode(this);
		}
	}

	private String getCanonicalName(Class clazz) {
		String name = CANONICAL_NAME_CACHE.get(clazz);

		if (name == null) {
			name = clazz.getCanonicalName();
			synchronized (CANONICAL_NAME_CACHE) {
				CANONICAL_NAME_CACHE.put(clazz, name);
			}
		}
		return name;
	}

	private boolean isFieldNullable(Field field) {
		if (!NULLABLE_FIELD_CACHE.containsKey(field)) {
			CFNotNull annotation = field.getAnnotation(CFNotNull.class);
			synchronized (NULLABLE_FIELD_CACHE) {
				NULLABLE_FIELD_CACHE.put(field, annotation == null);
			}
		}
		return NULLABLE_FIELD_CACHE.get(field);
	}

	private Field[] getClazzFields(Class<? extends CommandCustomHeader> classHeader) {
		Field[] fields = CLASS_HASH_MAP.get(classHeader);

		if (fields == null) {
			Set<Field> fieldList = new HashSet<>();
			for (Class className = classHeader; className != Object.class; className = className.getSuperclass()) {
				Field[] fs = className.getDeclaredFields();
				fieldList.addAll(Arrays.asList(fs));
			}
			fields = fieldList.toArray(new Field[0]);
			synchronized (CLASS_HASH_MAP) {
				CLASS_HASH_MAP.put(classHeader, fields);
			}
		}
		return fields;
	}
}
