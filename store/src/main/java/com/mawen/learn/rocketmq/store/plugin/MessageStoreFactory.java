package com.mawen.learn.rocketmq.store.plugin;

import java.lang.reflect.Constructor;

import com.mawen.learn.rocketmq.store.MessageStore;
import lombok.extern.slf4j.Slf4j;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
@Slf4j
public class MessageStoreFactory {

	public static MessageStore build(MessageStorePluginContext context, MessageStore messageStore) {
		String plugin = context.getBrokerConfig().getMessageStorePluginIn();
		if (plugin != null && plugin.trim().length() != 0) {
			String[] pluginClasses = plugin.split(",");

			for (int i = pluginClasses.length - 1; i >= 0; i--) {
				String pluginClass = pluginClasses[i];
				try {
					Class<AbstractPluginMessageStore> clazz = (Class<AbstractPluginMessageStore>) Class.forName(pluginClass);
					Constructor<AbstractPluginMessageStore> constructor = clazz.getConstructor(MessageStorePluginContext.class, MessageStore.class);
					AbstractPluginMessageStore pluginMessageStore = constructor.newInstance(constructor, messageStore);
					messageStore = pluginMessageStore;
				}
				catch (Throwable e) {
					throw new RuntimeException("Initialize plugin's class: " + pluginClass + " not found!", e);
				}
			}
		}

		return messageStore;
	}
}
