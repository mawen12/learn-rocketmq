package com.mawen.learn.rocketmq.store.ha;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/8
 */
public interface HAClient {

	void start();

	void shutdown();

	void wakeup();

	void updateMasterAddress(String newAddress);

	void updateHaMasterAddress(String newAddress);

	String getMasterAddress();

	String getHaMasterAddress();

	long getLastReadTimestamp();

	long getLastWriteTimestamp();

	HAConnectionState getCurrentState();

	void changeCurrentState(HAConnectionState state);

	void closeMaster();

	long getTransferredByteInSecond();
}
