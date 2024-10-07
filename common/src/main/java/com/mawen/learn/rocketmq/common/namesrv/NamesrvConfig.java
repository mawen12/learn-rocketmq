package com.mawen.learn.rocketmq.common.namesrv;

import java.io.File;

import com.mawen.learn.rocketmq.common.MixAll;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/7
 */
public class NamesrvConfig {

	private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));
	private String kvConfigPath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "kvConfig.json";
	private String configStorePath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "namesrv.properties";
	private String productEnvName = "center";
	private boolean clusterTest = false;
	private boolean orderMessageEnable = false;
	private boolean returnOrderTopicConfigToBroker = true;

	private int clientRequestThreadPoolNums = 8;

	private int defaultThreadPoolNums = 16;

	private int clientRequestThreadPoolQueueCapacity = 50000;

	private int defaultThreadPoolQueueCapacity = 10000;

	private long scanNotActiveBrokerInterval = 5 * 1000;

	private int unRegisterBrokerQueueCapacity = 3000;

	private boolean supportActingMaster = false;

	private volatile boolean enableAllTopicList = true;

	private volatile boolean enableTopicList = true;

	private volatile boolean notifyMinBrokerIdChanged = false;

	private boolean enableControllerInNamesrv = false;

	private volatile boolean needWaitForService = false;

	private int waitSecondsForService = 45;

	private boolean deleteTopicWithBrokerRegistration = false;

	private String configBlackList = "configBlackList;configStorePath;kvConfigPath";

	public String getConfigBlackList() {
		return configBlackList;
	}

	public void setConfigBlackList(String configBlackList) {
		this.configBlackList = configBlackList;
	}

	public boolean isOrderMessageEnable() {
		return orderMessageEnable;
	}

	public void setOrderMessageEnable(boolean orderMessageEnable) {
		this.orderMessageEnable = orderMessageEnable;
	}

	public String getRocketmqHome() {
		return rocketmqHome;
	}

	public void setRocketmqHome(String rocketmqHome) {
		this.rocketmqHome = rocketmqHome;
	}

	public String getKvConfigPath() {
		return kvConfigPath;
	}

	public void setKvConfigPath(String kvConfigPath) {
		this.kvConfigPath = kvConfigPath;
	}

	public String getProductEnvName() {
		return productEnvName;
	}

	public void setProductEnvName(String productEnvName) {
		this.productEnvName = productEnvName;
	}

	public boolean isClusterTest() {
		return clusterTest;
	}

	public void setClusterTest(boolean clusterTest) {
		this.clusterTest = clusterTest;
	}

	public String getConfigStorePath() {
		return configStorePath;
	}

	public void setConfigStorePath(final String configStorePath) {
		this.configStorePath = configStorePath;
	}

	public boolean isReturnOrderTopicConfigToBroker() {
		return returnOrderTopicConfigToBroker;
	}

	public void setReturnOrderTopicConfigToBroker(boolean returnOrderTopicConfigToBroker) {
		this.returnOrderTopicConfigToBroker = returnOrderTopicConfigToBroker;
	}

	public int getClientRequestThreadPoolNums() {
		return clientRequestThreadPoolNums;
	}

	public void setClientRequestThreadPoolNums(final int clientRequestThreadPoolNums) {
		this.clientRequestThreadPoolNums = clientRequestThreadPoolNums;
	}

	public int getDefaultThreadPoolNums() {
		return defaultThreadPoolNums;
	}

	public void setDefaultThreadPoolNums(final int defaultThreadPoolNums) {
		this.defaultThreadPoolNums = defaultThreadPoolNums;
	}

	public int getClientRequestThreadPoolQueueCapacity() {
		return clientRequestThreadPoolQueueCapacity;
	}

	public void setClientRequestThreadPoolQueueCapacity(final int clientRequestThreadPoolQueueCapacity) {
		this.clientRequestThreadPoolQueueCapacity = clientRequestThreadPoolQueueCapacity;
	}

	public int getDefaultThreadPoolQueueCapacity() {
		return defaultThreadPoolQueueCapacity;
	}

	public void setDefaultThreadPoolQueueCapacity(final int defaultThreadPoolQueueCapacity) {
		this.defaultThreadPoolQueueCapacity = defaultThreadPoolQueueCapacity;
	}

	public long getScanNotActiveBrokerInterval() {
		return scanNotActiveBrokerInterval;
	}

	public void setScanNotActiveBrokerInterval(long scanNotActiveBrokerInterval) {
		this.scanNotActiveBrokerInterval = scanNotActiveBrokerInterval;
	}

	public int getUnRegisterBrokerQueueCapacity() {
		return unRegisterBrokerQueueCapacity;
	}

	public void setUnRegisterBrokerQueueCapacity(final int unRegisterBrokerQueueCapacity) {
		this.unRegisterBrokerQueueCapacity = unRegisterBrokerQueueCapacity;
	}

	public boolean isSupportActingMaster() {
		return supportActingMaster;
	}

	public void setSupportActingMaster(final boolean supportActingMaster) {
		this.supportActingMaster = supportActingMaster;
	}

	public boolean isEnableAllTopicList() {
		return enableAllTopicList;
	}

	public void setEnableAllTopicList(boolean enableAllTopicList) {
		this.enableAllTopicList = enableAllTopicList;
	}

	public boolean isEnableTopicList() {
		return enableTopicList;
	}

	public void setEnableTopicList(boolean enableTopicList) {
		this.enableTopicList = enableTopicList;
	}

	public boolean isNotifyMinBrokerIdChanged() {
		return notifyMinBrokerIdChanged;
	}

	public void setNotifyMinBrokerIdChanged(boolean notifyMinBrokerIdChanged) {
		this.notifyMinBrokerIdChanged = notifyMinBrokerIdChanged;
	}

	public boolean isEnableControllerInNamesrv() {
		return enableControllerInNamesrv;
	}

	public void setEnableControllerInNamesrv(boolean enableControllerInNamesrv) {
		this.enableControllerInNamesrv = enableControllerInNamesrv;
	}

	public boolean isNeedWaitForService() {
		return needWaitForService;
	}

	public void setNeedWaitForService(boolean needWaitForService) {
		this.needWaitForService = needWaitForService;
	}

	public int getWaitSecondsForService() {
		return waitSecondsForService;
	}

	public void setWaitSecondsForService(int waitSecondsForService) {
		this.waitSecondsForService = waitSecondsForService;
	}

	public boolean isDeleteTopicWithBrokerRegistration() {
		return deleteTopicWithBrokerRegistration;
	}

	public void setDeleteTopicWithBrokerRegistration(boolean deleteTopicWithBrokerRegistration) {
		this.deleteTopicWithBrokerRegistration = deleteTopicWithBrokerRegistration;
	}
}
