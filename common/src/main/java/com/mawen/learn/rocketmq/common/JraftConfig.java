package com.mawen.learn.rocketmq.common;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class JraftConfig {

	private int jRaftElectionTimeoutMs = 1000;
	private int jRaftScanWaitTimeoutMs = 1000;
	private int jRafSnapshotIntervalSecs = 3600;

	private String jRaftGroupId = "jRaft-Controller";
	private String jRaftServerId = "localhost:9880";
	private String jRaftInitConf = "localhost:9880,localhost:9881,localhost:9882";
	private String jRaftControllerRPCAddr = "localhost:9770,localhost:9771,localhost:9772";

	public int getjRaftElectionTimeoutMs() {
		return jRaftElectionTimeoutMs;
	}

	public void setjRaftElectionTimeoutMs(int jRaftElectionTimeoutMs) {
		this.jRaftElectionTimeoutMs = jRaftElectionTimeoutMs;
	}

	public int getjRaftScanWaitTimeoutMs() {
		return jRaftScanWaitTimeoutMs;
	}

	public void setjRaftScanWaitTimeoutMs(int jRaftScanWaitTimeoutMs) {
		this.jRaftScanWaitTimeoutMs = jRaftScanWaitTimeoutMs;
	}

	public int getjRafSnapshotIntervalSecs() {
		return jRafSnapshotIntervalSecs;
	}

	public void setjRafSnapshotIntervalSecs(int jRafSnapshotIntervalSecs) {
		this.jRafSnapshotIntervalSecs = jRafSnapshotIntervalSecs;
	}

	public String getjRaftGroupId() {
		return jRaftGroupId;
	}

	public void setjRaftGroupId(String jRaftGroupId) {
		this.jRaftGroupId = jRaftGroupId;
	}

	public String getjRaftServerId() {
		return jRaftServerId;
	}

	public void setjRaftServerId(String jRaftServerId) {
		this.jRaftServerId = jRaftServerId;
	}

	public String getjRaftInitConf() {
		return jRaftInitConf;
	}

	public void setjRaftInitConf(String jRaftInitConf) {
		this.jRaftInitConf = jRaftInitConf;
	}

	public String getjRaftControllerRPCAddr() {
		return jRaftControllerRPCAddr;
	}

	public void setjRaftControllerRPCAddr(String jRaftControllerRPCAddr) {
		this.jRaftControllerRPCAddr = jRaftControllerRPCAddr;
	}
}
