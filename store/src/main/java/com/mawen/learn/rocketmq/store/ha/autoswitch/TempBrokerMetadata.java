package com.mawen.learn.rocketmq.store.ha.autoswitch;

import java.io.IOException;

import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
@Getter
@ToString
public class TempBrokerMetadata extends BrokerMetadata {

	private String registerCheckCode;


	public TempBrokerMetadata(String filePath) {
		this(filePath, null, null, null, null);
	}

	public TempBrokerMetadata(String filePath, String clusterName, String brokerName, Long brokerId, String registerCheckCode) {
		super(filePath);
		super.clusterName = clusterName;
		super.brokerName = brokerName;
		super.brokerId = brokerId;
		this.registerCheckCode = registerCheckCode;
	}

	public void updateAndPersis(String clusterName, String brokerName, Long brokerId, String registerCheckCode) throws IOException {
		super.clusterName = clusterName;
		super.brokerName = brokerName;
		super.brokerId = brokerId;
		this.registerCheckCode = registerCheckCode;
		writeToFile();
	}

	@Override
	public String encodeToStr() {
		StringBuilder sb = new StringBuilder();
		sb.append(clusterName).append("#");
		sb.append(brokerName).append("#");
		sb.append(brokerId).append("#");
		sb.append(registerCheckCode);
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
		this.registerCheckCode = dataArr[3];
	}

	@Override
	public boolean isLoaded() {
		return super.isLoaded() && StringUtils.isNotEmpty(registerCheckCode);
	}

	@Override
	public void clearInMem() {
		super.clearInMem();
		this.registerCheckCode = null;
	}
}
