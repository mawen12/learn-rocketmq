package com.mawen.learn.rocketmq.common;

import java.io.File;
import java.util.Arrays;

import com.mawen.learn.rocketmq.common.metrics.MetricsExporterType;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class ControllerConfig {

	private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));

	private String configStorePath = System.getProperty("user.home") + File.separator + "controller" + File.separator + "controller.properties";

	public static final String DLEDGER_CONTROLLER = "DLedger";

	public static final String JRAFT_CONTROLLER = "jRaft";

	private JraftConfig jraftConfig = new JraftConfig();

	private String controllerType = DLEDGER_CONTROLLER;

	private long scanNotActiveBrokerInterval = 5 * 1000;

	private int controllerThreadPoolNums = 16;

	private int controllerRequestsThreadPoolQueueCapacity = 50000;

	private String controllerDLegerGroup;
	private String controllerDLegerPeers;
	private String controllerDLegerSelfId;
	private int mappedFileSize = 1024 * 1024 * 1024;
	private String controllerStorePath = "";

	private int electMasterMaxRetryCount = 3;

	private boolean enableElectUncleanMaster = false;

	private boolean isProcessReadEvent = false;

	private volatile boolean notifyBrokerRoleChanged = true;

	private long scanInactiveMasterInterval = 5 * 1000;

	private MetricsExporterType metricsExporterType = MetricsExporterType.DISABLE;

	private String metricsGrpcExporterTarget = "";

	private String metricsGrpcExporterHeader = "";

	private long metricGrpcExporterTimeOutInMillis = 3 * 1000;

	private long metricGrpcExporterIntervalInMillis = 60 * 1000;

	private long metricLoggingExporterIntervalInMillis = 10 * 1000;

	private int metricsPromExporterPort = 5557;

	private String metricsPromExporterHost = "";

	private String metricsLabel = "";

	private boolean metricsInDelta = false;

	private String configBlackList = "configBlackList;configStorePath";

	public String getDLedgerAddress() {
		return Arrays.stream(this.controllerDLegerPeers.split(";"))
				.filter(x -> this.controllerDLegerSelfId.equals(x.split("-")[0]))
				.map(x -> x.split("-")[1])
				.findFirst().get();
	}

	public String getRocketmqHome() {
		return rocketmqHome;
	}

	public void setRocketmqHome(String rocketmqHome) {
		this.rocketmqHome = rocketmqHome;
	}

	public String getConfigStorePath() {
		return configStorePath;
	}

	public void setConfigStorePath(String configStorePath) {
		this.configStorePath = configStorePath;
	}

	public JraftConfig getJraftConfig() {
		return jraftConfig;
	}

	public void setJraftConfig(JraftConfig jraftConfig) {
		this.jraftConfig = jraftConfig;
	}

	public String getControllerType() {
		return controllerType;
	}

	public void setControllerType(String controllerType) {
		this.controllerType = controllerType;
	}

	public long getScanNotActiveBrokerInterval() {
		return scanNotActiveBrokerInterval;
	}

	public void setScanNotActiveBrokerInterval(long scanNotActiveBrokerInterval) {
		this.scanNotActiveBrokerInterval = scanNotActiveBrokerInterval;
	}

	public int getControllerThreadPoolNums() {
		return controllerThreadPoolNums;
	}

	public void setControllerThreadPoolNums(int controllerThreadPoolNums) {
		this.controllerThreadPoolNums = controllerThreadPoolNums;
	}

	public int getControllerRequestsThreadPoolQueueCapacity() {
		return controllerRequestsThreadPoolQueueCapacity;
	}

	public void setControllerRequestsThreadPoolQueueCapacity(int controllerRequestsThreadPoolQueueCapacity) {
		this.controllerRequestsThreadPoolQueueCapacity = controllerRequestsThreadPoolQueueCapacity;
	}

	public String getControllerDLegerGroup() {
		return controllerDLegerGroup;
	}

	public void setControllerDLegerGroup(String controllerDLegerGroup) {
		this.controllerDLegerGroup = controllerDLegerGroup;
	}

	public String getControllerDLegerPeers() {
		return controllerDLegerPeers;
	}

	public void setControllerDLegerPeers(String controllerDLegerPeers) {
		this.controllerDLegerPeers = controllerDLegerPeers;
	}

	public String getControllerDLegerSelfId() {
		return controllerDLegerSelfId;
	}

	public void setControllerDLegerSelfId(String controllerDLegerSelfId) {
		this.controllerDLegerSelfId = controllerDLegerSelfId;
	}

	public int getMappedFileSize() {
		return mappedFileSize;
	}

	public void setMappedFileSize(int mappedFileSize) {
		this.mappedFileSize = mappedFileSize;
	}

	public String getControllerStorePath() {
		if (controllerStorePath.isEmpty()) {
			controllerStorePath = System.getProperty("user.home") + File.separator + controllerType + "Controller";
		}
		return controllerStorePath;
	}

	public void setControllerStorePath(String controllerStorePath) {
		this.controllerStorePath = controllerStorePath;
	}

	public int getElectMasterMaxRetryCount() {
		return electMasterMaxRetryCount;
	}

	public void setElectMasterMaxRetryCount(int electMasterMaxRetryCount) {
		this.electMasterMaxRetryCount = electMasterMaxRetryCount;
	}

	public boolean isEnableElectUncleanMaster() {
		return enableElectUncleanMaster;
	}

	public void setEnableElectUncleanMaster(boolean enableElectUncleanMaster) {
		this.enableElectUncleanMaster = enableElectUncleanMaster;
	}

	public boolean isProcessReadEvent() {
		return isProcessReadEvent;
	}

	public void setProcessReadEvent(boolean processReadEvent) {
		isProcessReadEvent = processReadEvent;
	}

	public boolean isNotifyBrokerRoleChanged() {
		return notifyBrokerRoleChanged;
	}

	public void setNotifyBrokerRoleChanged(boolean notifyBrokerRoleChanged) {
		this.notifyBrokerRoleChanged = notifyBrokerRoleChanged;
	}

	public long getScanInactiveMasterInterval() {
		return scanInactiveMasterInterval;
	}

	public void setScanInactiveMasterInterval(long scanInactiveMasterInterval) {
		this.scanInactiveMasterInterval = scanInactiveMasterInterval;
	}

	public MetricsExporterType getMetricsExporterType() {
		return metricsExporterType;
	}

	public void setMetricsExporterType(MetricsExporterType metricsExporterType) {
		this.metricsExporterType = metricsExporterType;
	}

	public String getMetricsGrpcExporterTarget() {
		return metricsGrpcExporterTarget;
	}

	public void setMetricsGrpcExporterTarget(String metricsGrpcExporterTarget) {
		this.metricsGrpcExporterTarget = metricsGrpcExporterTarget;
	}

	public String getMetricsGrpcExporterHeader() {
		return metricsGrpcExporterHeader;
	}

	public void setMetricsGrpcExporterHeader(String metricsGrpcExporterHeader) {
		this.metricsGrpcExporterHeader = metricsGrpcExporterHeader;
	}

	public long getMetricGrpcExporterTimeOutInMillis() {
		return metricGrpcExporterTimeOutInMillis;
	}

	public void setMetricGrpcExporterTimeOutInMillis(long metricGrpcExporterTimeOutInMillis) {
		this.metricGrpcExporterTimeOutInMillis = metricGrpcExporterTimeOutInMillis;
	}

	public long getMetricGrpcExporterIntervalInMillis() {
		return metricGrpcExporterIntervalInMillis;
	}

	public void setMetricGrpcExporterIntervalInMillis(long metricGrpcExporterIntervalInMillis) {
		this.metricGrpcExporterIntervalInMillis = metricGrpcExporterIntervalInMillis;
	}

	public long getMetricLoggingExporterIntervalInMillis() {
		return metricLoggingExporterIntervalInMillis;
	}

	public void setMetricLoggingExporterIntervalInMillis(long metricLoggingExporterIntervalInMillis) {
		this.metricLoggingExporterIntervalInMillis = metricLoggingExporterIntervalInMillis;
	}

	public int getMetricsPromExporterPort() {
		return metricsPromExporterPort;
	}

	public void setMetricsPromExporterPort(int metricsPromExporterPort) {
		this.metricsPromExporterPort = metricsPromExporterPort;
	}

	public String getMetricsPromExporterHost() {
		return metricsPromExporterHost;
	}

	public void setMetricsPromExporterHost(String metricsPromExporterHost) {
		this.metricsPromExporterHost = metricsPromExporterHost;
	}

	public String getMetricsLabel() {
		return metricsLabel;
	}

	public void setMetricsLabel(String metricsLabel) {
		this.metricsLabel = metricsLabel;
	}

	public boolean isMetricsInDelta() {
		return metricsInDelta;
	}

	public void setMetricsInDelta(boolean metricsInDelta) {
		this.metricsInDelta = metricsInDelta;
	}

	public String getConfigBlackList() {
		return configBlackList;
	}

	public void setConfigBlackList(String configBlackList) {
		this.configBlackList = configBlackList;
	}
}
