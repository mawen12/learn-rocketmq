package com.mawen.learn.rocketmq.common.message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.mawen.learn.rocketmq.common.MixAll;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/4
 */
public class MessageBatch extends Message implements Iterable<Message> {

	private static final long serialVersionUID = 4268005225052573072L;

	private final List<Message> messages;

	public MessageBatch(List<Message> messages) {
		this.messages = messages;
	}

	public byte[] encode() {
		return MessageDecoder.encodeMessages(messages);
	}

	@Override
	public Iterator<Message> iterator() {
		return messages.iterator();
	}

	public static MessageBatch generateFromList(Collection<? extends Message> messages) {
		assert messages != null;
		assert messages.size() > 0;

		List<Message> messageList = new ArrayList<>(messages.size());
		Message first = null;
		for (Message message : messages) {
			if (message.getDelayTimeLevel() > 0) {
				throw new UnsupportedOperationException("TimeDelayLevel is not supported for batching");
			}
			if (message.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
				throw new UnsupportedOperationException("Retry Group is not supported for batching");
			}
			if (first == null) {
				first = message;
			}
			else {
				if (!first.getTopic().equals(message.getTopic())) {
					throw new UnsupportedOperationException("The topic of the messages in one batch should be the same");
				}
				if (first.isWaitStoreMsgOK() != message.isWaitStoreMsgOK()) {
					throw new UnsupportedOperationException("The waitStoreMsgOK of the message in one batch should be the same");
				}
			}
			messageList.add(message);
		}

		MessageBatch messageBatch = new MessageBatch(messageList);
		messageBatch.setTopic(first.getTopic());
		messageBatch.setWaitStoreMsgOK(first.isWaitStoreMsgOK());
		return messageBatch;
	}
}
