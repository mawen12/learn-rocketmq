package com.mawen.learn.rocketmq.store;

import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/8
 */
@NoArgsConstructor
@Getter
public class RunningFlags {

	private static final int NOT_READABLE_BIT = 1;

	private static final int NOT_WRITABLE_BIT = 1 << 1;

	private static final int WRITE_LOGICS_QUEUE_ERROR_BIT = 1 << 2;

	private static final int WRITE_INDEX_QUEUE_ERROR_BIT = 1 << 3;

	private static final int DISK_FULL_BIT = 1 << 4;

	private static final int FENCED_BIT = 1 << 5;

	private static final int LOGIC_DISK_FULL_BIT = 1 << 6;

	private volatile int flagBits = 0;

	public boolean isReadable() {
		return (flagBits & NOT_READABLE_BIT) == 0;
	}

	public boolean isFenced() {
		return (flagBits & FENCED_BIT) != 0;
	}

	public boolean isWritable() {
		return (flagBits & (NOT_READABLE_BIT | WRITE_LOGICS_QUEUE_ERROR_BIT | DISK_FULL_BIT | WRITE_INDEX_QUEUE_ERROR_BIT | FENCED_BIT | LOGIC_DISK_FULL_BIT)) == 0;
	}

	public boolean isCQWritable() {
		return (flagBits & (NOT_WRITABLE_BIT | WRITE_LOGICS_QUEUE_ERROR_BIT | WRITE_INDEX_QUEUE_ERROR_BIT | LOGIC_DISK_FULL_BIT)) == 0;
	}

	public boolean getAndMakeReadable() {
		boolean result = isReadable();
		if (!result) {
			flagBits &= ~NOT_READABLE_BIT;
		}
		return result;
	}

	public boolean getAndMakeNotReadable() {
		boolean result = isReadable();
		if (result) {
			flagBits |= NOT_READABLE_BIT;
		}
		return result;
	}

	public void clearLogicQueueError() {
		flagBits &= ~WRITE_LOGICS_QUEUE_ERROR_BIT;
	}

	public boolean getAndMakeWritable() {
		boolean result = isWritable();
		if (!result) {
			flagBits &= ~NOT_WRITABLE_BIT;
		}
		return result;
	}

	public boolean getAndMakeNotWritable() {
		boolean result = isWritable();
		if (result) {
			flagBits |= NOT_WRITABLE_BIT;
		}
		return result;
	}

	public void makeLogicsQueueError() {
		flagBits |= WRITE_INDEX_QUEUE_ERROR_BIT;
	}

	public void makeFenced(boolean fenced) {
		if (fenced) {
			flagBits |= FENCED_BIT;
		}
		else {
			flagBits &= ~FENCED_BIT;
		}
	}

	public boolean isLogicsQueueError() {
		return (flagBits & WRITE_LOGICS_QUEUE_ERROR_BIT) == WRITE_LOGICS_QUEUE_ERROR_BIT;
	}

	public void makeIndexFileError() {
		flagBits |= WRITE_INDEX_QUEUE_ERROR_BIT;
	}

	public boolean isIndexFileError() {
		return (flagBits & WRITE_INDEX_QUEUE_ERROR_BIT) == WRITE_INDEX_QUEUE_ERROR_BIT;
	}

	public boolean getAndMakeDiskFull() {
		boolean result = !((flagBits & DISK_FULL_BIT) == DISK_FULL_BIT);
		flagBits |= DISK_FULL_BIT;
		return result;
	}

	public boolean getAndMakeDiskOK() {
		boolean result = !((flagBits & DISK_FULL_BIT) == DISK_FULL_BIT);
		flagBits &= ~DISK_FULL_BIT;
		return result;
	}

	public boolean getAndMakeLogicDiskFull() {
		boolean result = !((flagBits & LOGIC_DISK_FULL_BIT) == LOGIC_DISK_FULL_BIT);
		flagBits |= LOGIC_DISK_FULL_BIT;
		return result;
	}

	public boolean getAndMakeLogicDiskOK() {
		boolean result = !((flagBits & LOGIC_DISK_FULL_BIT) == LOGIC_DISK_FULL_BIT);
		flagBits &= ~LOGIC_DISK_FULL_BIT;
		return result;
	}
}
