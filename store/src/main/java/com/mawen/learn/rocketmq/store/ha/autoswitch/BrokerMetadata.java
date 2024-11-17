package com.mawen.learn.rocketmq.store.ha.autoswitch;

import java.io.IOException;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
@Getter
@ToString
@EqualsAndHashCode
public class BrokerMetadata extends MetadataFile {

	protected String clusterName;

	protected String brokerName;

	protected Long brokerId;

	public BrokerMetadata(String filePath) {
		this.filePath = filePath;
	}

	public void updateAndPersist(String clusterName, String brokerName, Long brokerId) throws IOException {
		this.clusterName = clusterName;
		this.brokerName = brokerName;
		this.brokerId = brokerId;
		writeToFile();
	}

	@Override
	public String encodeToStr() {
		StringBuilder sb = new StringBuilder();
		sb.append(clusterName).append('#');
		sb.append(brokerName).append('#');
		sb.append(brokerId);
		return sb.toString();
	}

	@Override
	public void decodeFromStr(String dataStr) {
		if (dataStr == null) {
			return;
		}

		String[] dataArr = dataStr.split("#");
		this.clusterName = dataArr[0];
		this.brokerName = dataArr[1];
		this.brokerId = Long.valueOf(dataArr[2]);
	}

	@Override
	public boolean isLoaded() {
		return StringUtils.isNotEmpty(clusterName) && StringUtils.isNotEmpty(brokerName) && brokerId != null;
	}

	@Override
	public void clearInMem() {
		this.clusterName = null;
		this.brokerName = null;
		this.brokerId = null;
	}
}
