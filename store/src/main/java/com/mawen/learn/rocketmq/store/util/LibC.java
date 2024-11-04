package com.mawen.learn.rocketmq.store.util;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/4
 */
public interface LibC extends Library {
	LibC INSTANCE = (LibC) Native.loadLibrary(Platform.isWindows() ? "msvcrt" : "c", LibC.class);

	int MADV_NORMAL = 0;
	int MADV_RANDOM = 1;
	int MADV_WILLNEED = 3;
	int MADV_DONTNEED = 4;

	int MCL_CURRENT = 1;
	int MCL_FUTURE = 2;
	int MCL_ONFAULT = 4;

	int MS_ASYNC = 0x0001;
	int MS_INVALIDATE = 0x0002;
	int MS_SYNC = 0x0004;

	int mlock(Pointer var1, NativeLong var2);

	int munlock(Pointer var1, NativeLong var2);

	int madvise(Pointer var1, NativeLong var2, int var3);

	Pointer memset(Pointer p, int v, long len);

	int mlockall(int flags);

	int msync(Pointer p, NativeLong length, int flags);

	int mincore(Pointer p, NativeLong length, byte[] vec);

	int getpagesize();
}
