package com.mawen.learn.rocketmq.common.sysflag;

import com.mawen.learn.rocketmq.common.compression.CompressionType;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public class MessageSysFlag {

	public static final int COMPRESSED_FLAG = 0x1;
	public static final int MULTI_TAGS_FLAG = 0x1 << 1;
	public static final int TRANSACTION_NOT_TYPE = 0;
	public static final int TRANSACTION_PREPARED_TYPE = 0x1 << 2;
	public static final int TRANSACTION_COMMIT_TYPE = 0x2 << 2;
	public static final int TRANSACTION_ROLLBACK_TYPE = 0x3 << 2;
	public static final int BORNHOST_V6_FLAG = 0x1 << 4;
	public static final int STOREHOSTADDRESS_V6_FLAG = 0x1 << 5;

	public static final int NEED_UNWRAP_FLAG = 0x1 << 6;
	public static final int INNER_BATCH_FLAG = 0x1 << 7;


	public static final int COMPRESSION_LZ4_TYPE = 0x1 << 8;
	public static final int COMPRESSION_ZSTD_TYPE = 0x2 << 8;
	public static final int COMPRESSION_ZLIB_TYPE = 0x3 << 8;
	public static final int COMPRESSION_TYPE_COMPARATOR = 0x7 << 8;

	public static int getTransactionValue(final int flag) {
		return flag & TRANSACTION_ROLLBACK_TYPE;
	}

	public static int resetTransactionValue(final int flag, final int type) {
		return (flag & (~TRANSACTION_ROLLBACK_TYPE)) | type;
	}

	public static int clearCompressedFlag(final int flag) {
		return flag & (~COMPRESSED_FLAG);
	}

	public static CompressionType getCompressionType(final int flag) {
		return CompressionType.findByValue((flag & COMPRESSION_TYPE_COMPARATOR) >> 8);
	}

	public static boolean check(int flag, int expectedFlag) {
		return (flag & expectedFlag) != 0;
	}
}
