package com.mawen.learn.rocketmq.store;

import com.mawen.learn.rocketmq.store.logfile.MappedFile;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/16
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class FileQueueSnapshot {

	private MappedFile firstFile;
	private long firstFileIndex;
	private MappedFile lastFile;
	private long lastFileIndex;
	private long currentFile;
	private long currentFileIndex;
	private long behindCount;
	private boolean exist;


}
