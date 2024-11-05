package com.mawen.learn.rocketmq.store.timer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.topic.TopicValidator;
import com.mawen.learn.rocketmq.store.MessageStore;
import com.mawen.learn.rocketmq.store.util.PerfCounter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
public class TimerMessageStore {

	private static final Logger log = LoggerFactory.getLogger(TimerMessageStore.class);

	public static final int INITIAL = 0, RUNNING = 1, HAULT = 2, SHUTDOWN = 3;
	public static final String TIMER_TOPIC = TopicValidator.SYSTEM_TOPIC_PREFIX + "wheel_timer";
	public static final String TIMER_OUT_MS = MessageConst.PROPERTY_TIMER_OUT_MS;
	public static final String TIMER_ENQUEUE_MS = MessageConst.PROPERTY_TIMER_ENQUEUE_MS;
	public static final String TIMER_DEQUEUE_MS = MessageConst.PROPERTY_TIMER_DEQUEUE_MS;
	public static final String TIMER_ROLL_TIMES = MessageConst.PROPERTY_TIMER_ROLL_TIMES;
	public static final String TIMER_DELETE_UNIQUE_KEY = MessageConst.PROPERTY_TIMER_DEL_UNIQKEY;

	public static final Random RANDOM = new Random();
	public static final int PUT_OK = 0, PUT_NEED_RETRY = 1, PTU_NO_RETRY = 2;
	public static final int DAY_SECS = 24 * 3600;
	public static final int DEFAULT_CAPACITY = 1024;

	public static final int TIMER_WHEEL_TTL_DAY = 7;
	public static final int TIMER_BLANK_SLOTS = 60;
	public static final int MAGIC_DEFAULT = 1;
	public static final int MAGIC_ROLL = 1 << 1;
	public static final int MAGIC_DELETE = 1 << 2;

	protected static final String ENQUEUE_PUT = "enqueue_put";
	protected static final String DEQUEUE_PUT = "dequeue_put";

	public boolean debug = false;
	private volatile int state = INITIAL;

	protected final PerfCounter.Ticks perfCounterTicks = new PerfCounter.Ticks(log);
	protected final BlockingQueue<TimerRequest> enqueuePutQueue;
	protected final BlockingQueue<List<TimerRequest>> dequeueGetQueue;
	protected final BlockingQueue<TimerRequest> dequeuePutQueue;

	private final ByteBuffer timerLogBuffer = ByteBuffer.allocate(4 * 1024);
	private final ThreadLocal<ByteBuffer> bufferLocal;
	private final ScheduledExecutorService scheduler;

	private final MessageStore messageStore;
	private final TimerWheel timerWheel;
	private final TimerLog timerLog;
}
