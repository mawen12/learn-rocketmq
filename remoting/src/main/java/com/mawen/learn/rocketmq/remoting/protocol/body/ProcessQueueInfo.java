package com.mawen.learn.rocketmq.remoting.protocol.body;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class ProcessQueueInfo {

	private long commitOffset;

	private long cachedMsgMinOffset;

	private long cachedMsgMaxOffset;

	private long cachedMsgCount;

	private long cachedMsgSizeInMiB;

	private long transactionMsgMinOffset;

	private long transactionMsgMaxOffset;

	private int transactionMsgCount;

	private boolean locked;

	private long tryUnlockTimes;

	private long lastLockTimestamp;

	private boolean droped;

	private long lastPullTimestamp;

	private long lastConsumeTimestamp;

	public long getCommitOffset() {
		return commitOffset;
	}

	public void setCommitOffset(long commitOffset) {
		this.commitOffset = commitOffset;
	}

	public long getCachedMsgMinOffset() {
		return cachedMsgMinOffset;
	}

	public void setCachedMsgMinOffset(long cachedMsgMinOffset) {
		this.cachedMsgMinOffset = cachedMsgMinOffset;
	}

	public long getCachedMsgMaxOffset() {
		return cachedMsgMaxOffset;
	}

	public void setCachedMsgMaxOffset(long cachedMsgMaxOffset) {
		this.cachedMsgMaxOffset = cachedMsgMaxOffset;
	}

	public long getCachedMsgCount() {
		return cachedMsgCount;
	}

	public void setCachedMsgCount(long cachedMsgCount) {
		this.cachedMsgCount = cachedMsgCount;
	}

	public long getCachedMsgSizeInMiB() {
		return cachedMsgSizeInMiB;
	}

	public void setCachedMsgSizeInMiB(long cachedMsgSizeInMiB) {
		this.cachedMsgSizeInMiB = cachedMsgSizeInMiB;
	}

	public long getTransactionMsgMinOffset() {
		return transactionMsgMinOffset;
	}

	public void setTransactionMsgMinOffset(long transactionMsgMinOffset) {
		this.transactionMsgMinOffset = transactionMsgMinOffset;
	}

	public long getTransactionMsgMaxOffset() {
		return transactionMsgMaxOffset;
	}

	public void setTransactionMsgMaxOffset(long transactionMsgMaxOffset) {
		this.transactionMsgMaxOffset = transactionMsgMaxOffset;
	}

	public int getTransactionMsgCount() {
		return transactionMsgCount;
	}

	public void setTransactionMsgCount(int transactionMsgCount) {
		this.transactionMsgCount = transactionMsgCount;
	}

	public boolean isLocked() {
		return locked;
	}

	public void setLocked(boolean locked) {
		this.locked = locked;
	}

	public long getTryUnlockTimes() {
		return tryUnlockTimes;
	}

	public void setTryUnlockTimes(long tryUnlockTimes) {
		this.tryUnlockTimes = tryUnlockTimes;
	}

	public long getLastLockTimestamp() {
		return lastLockTimestamp;
	}

	public void setLastLockTimestamp(long lastLockTimestamp) {
		this.lastLockTimestamp = lastLockTimestamp;
	}

	public boolean isDroped() {
		return droped;
	}

	public void setDroped(boolean droped) {
		this.droped = droped;
	}

	public long getLastPullTimestamp() {
		return lastPullTimestamp;
	}

	public void setLastPullTimestamp(long lastPullTimestamp) {
		this.lastPullTimestamp = lastPullTimestamp;
	}

	public long getLastConsumeTimestamp() {
		return lastConsumeTimestamp;
	}

	public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
		this.lastConsumeTimestamp = lastConsumeTimestamp;
	}

	@Override
	public String toString() {
		return "ProcessQueueInfo{" +
		       "commitOffset=" + commitOffset +
		       ", cachedMsgMinOffset=" + cachedMsgMinOffset +
		       ", cachedMsgMaxOffset=" + cachedMsgMaxOffset +
		       ", cachedMsgCount=" + cachedMsgCount +
		       ", cachedMsgSizeInMiB=" + cachedMsgSizeInMiB +
		       ", transactionMsgMinOffset=" + transactionMsgMinOffset +
		       ", transactionMsgMaxOffset=" + transactionMsgMaxOffset +
		       ", transactionMsgCount=" + transactionMsgCount +
		       ", locked=" + locked +
		       ", tryUnlockTimes=" + tryUnlockTimes +
		       ", lastLockTimestamp=" + lastLockTimestamp +
		       ", dropped=" + droped +
		       ", lastPullTimestamp=" + lastPullTimestamp +
		       ", lastConsumeTimestamp=" + lastConsumeTimestamp +
		       '}';
	}
}
