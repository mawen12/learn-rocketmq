package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import com.alibaba.fastjson.JSON;
import com.mawen.learn.rocketmq.common.MQVersion;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.TopicConfig;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.remoting.protocol.DataVersion;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import io.netty.buffer.ByteBuf;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class RegisterBrokerBody extends RemotingSerializable {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

	private static final long MINIMUM_TAKE_TIME_MILLISECOND = 50;

	private TopicConfigAndMappingSerializeWrapper topicConfigSerializeWrapper = new TopicConfigAndMappingSerializeWrapper();

	private List<String> filterServerList = new ArrayList<>();

	public byte[] encode(boolean compress) {
		if (!compress) {
			return super.encode();
		}

		long start = System.currentTimeMillis();

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		DeflaterOutputStream outputStream = new DeflaterOutputStream(byteArrayOutputStream, new Deflater(Deflater.BEST_COMPRESSION));
		DataVersion dataVersion = topicConfigSerializeWrapper.getDataVersion();
		ConcurrentMap<String, TopicConfig> topicConfigTable = clientTopicConfigTable(topicConfigSerializeWrapper.getTopicConfigTable());
		assert topicConfigTable != null;

		try {
			byte[] buffer = dataVersion.encode();
			outputStream.write(convertIntToByteArray(buffer.length));
			outputStream.write(buffer);

			int topicNumber = topicConfigTable.size();
			outputStream.write(convertIntToByteArray(topicNumber));
			for (Map.Entry<String, TopicConfig> entry : topicConfigTable.entrySet()) {
				buffer = entry.getValue().encode().getBytes(MixAll.DEFAULT_CHARSET);
				outputStream.write(convertIntToByteArray(buffer.length));
				outputStream.write(buffer);
			}

			buffer = JSON.toJSONString(filterServerList).getBytes(MixAll.DEFAULT_CHARSET);
			outputStream.write(convertIntToByteArray(buffer.length));
			outputStream.write(buffer);

			Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap = topicConfigSerializeWrapper.getTopicQueueMappingInfoMap();
			if (topicQueueMappingInfoMap == null) {
				topicQueueMappingInfoMap = new ConcurrentHashMap<>();
			}
			outputStream.write(convertIntToByteArray(topicQueueMappingInfoMap.size()));
			for (TopicQueueMappingInfo info : topicQueueMappingInfoMap.values()) {
				buffer = JSON.toJSONString(info).getBytes(MixAll.DEFAULT_CHARSET);
				outputStream.write(convertIntToByteArray(buffer.length));
				outputStream.write(buffer);
			}

			outputStream.flush();
			long takeTime = System.currentTimeMillis() - start;
			if (takeTime > MINIMUM_TAKE_TIME_MILLISECOND) {
				log.info("Compressing takes {}ms", takeTime);
			}
			return byteArrayOutputStream.toByteArray();
		}
		catch (IOException e) {
			log.error("Failed to compress RegisterBrokerBody object", e);
		}
		return null;
	}

	public static RegisterBrokerBody decode(byte[] data, boolean compressed, MQVersion.Version brokerVersion) {
		if (!compressed) {
			return RegisterBrokerBody.decode(data, RegisterBrokerBody.class);
		}

		long start = System.currentTimeMillis();
		InflaterInputStream inflaterInputStream = new InflaterInputStream(new ByteArrayInputStream(data));

		int dataVersionLength = readInt(inflaterInputStream);
		byte[] dataVersionBytes = readBytes(inflaterInputStream, dataVersionLength);
		DataVersion dataVersion = DataVersion.decode(dataVersionBytes, DataVersion.class);

		RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();
		registerBrokerBody.getTopicConfigSerializeWrapper().setDataVersion(dataVersion);
		ConcurrentMap<String, TopicConfig> topicConfigTable = registerBrokerBody.getTopicConfigSerializeWrapper().getTopicConfigTable();

		int topicConfigNumber = readInt(inflaterInputStream);
		log.debug("{} topic configs to extract", topicConfigNumber);

		for (int i = 0; i < topicConfigNumber; i++) {
			int topicConfigJsonLength = readInt(inflaterInputStream);
			byte[] buffer = readBytes(inflaterInputStream, topicConfigJsonLength);
			TopicConfig topicConfig = new TopicConfig();
			String topicConfigJson = new String(buffer, MixAll.DEFAULT_CHARSET);
			topicConfig.decode(topicConfigJson);
			topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
		}

		int filterServerListJsonLength = readInt(inflaterInputStream);
		byte[] filterServerListBytes = readBytes(inflaterInputStream, filterServerListJsonLength);
		String filterServerListJson = new String(filterServerListBytes, MixAll.DEFAULT_CHARSET);
		List<String> filterServerList = new ArrayList<>();
		try {
			filterServerList = JSON.parseArray(filterServerListJson, String.class);
		}
		catch (Exception e) {
			log.error("Decompressing occur Exception {}", filterServerList);
		}
		registerBrokerBody.setFilterServerList(filterServerList);

		if (brokerVersion.ordinal() >= MQVersion.Version.V5_0_0.ordinal()) {
			int topicQueueMappingNum = readInt(inflaterInputStream);
			Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap = new ConcurrentHashMap<>();
			for (int i = 0; i < topicQueueMappingNum; i++) {
				int mappingJsonLen = readInt(inflaterInputStream);
				byte[] mappingJsonBytes = readBytes(inflaterInputStream, mappingJsonLen);
				TopicQueueMappingInfo info = TopicQueueMappingInfo.decode(mappingJsonBytes, TopicQueueMappingInfo.class);
				topicQueueMappingInfoMap.put(info.getTopic(), info);
			}
			registerBrokerBody.getTopicConfigSerializeWrapper().setTopicQueueMappingInfoMap(topicQueueMappingInfoMap);
		}

		long takeTime = System.currentTimeMillis() - start;
		if (takeTime > MINIMUM_TAKE_TIME_MILLISECOND) {
			log.info("Decompressing takes {}ms", takeTime);
		}
		return registerBrokerBody;
	}

	private static byte[] convertIntToByteArray(int n) {
		ByteBuffer buffer = ByteBuffer.allocate(n);
		buffer.putInt(n);
		return buffer.array();
	}

	private static int readInt(InflaterInputStream inflaterInputStream) throws IOException {
		byte[] buffer = readBytes(inflaterInputStream, 4);
		ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
		return byteBuffer.getInt();
	}

	private static byte[] readBytes(InflaterInputStream inflaterInputStream, int length) throws IOException {
		byte[] buffer = new byte[length];
		int bytesRead = 0;

		while (bytesRead < length) {
			int len = inflaterInputStream.read(buffer, bytesRead, length - bytesRead);
			if (len == -1) {
				throw new IOException("End of compressed data has reached");
			}
			else {
				bytesRead = len;
			}
		}
		return buffer;
	}

	public TopicConfigAndMappingSerializeWrapper getTopicConfigSerializeWrapper() {
		return topicConfigSerializeWrapper;
	}

	public void setTopicConfigSerializeWrapper(TopicConfigAndMappingSerializeWrapper topicConfigSerializeWrapper) {
		this.topicConfigSerializeWrapper = topicConfigSerializeWrapper;
	}

	public List<String> getFilterServerList() {
		return filterServerList;
	}

	public void setFilterServerList(List<String> filterServerList) {
		this.filterServerList = filterServerList;
	}

	private ConcurrentMap<String, TopicConfig> clientTopicConfigTable(ConcurrentMap<String, TopicConfig> topicConfigConcurrentMap) {
		if (topicConfigConcurrentMap == null) {
			return null;
		}

		ConcurrentMap<String, TopicConfig> result = new ConcurrentHashMap<>(topicConfigConcurrentMap.size());
		result.putAll(topicConfigConcurrentMap);
		return result;
	}
}
