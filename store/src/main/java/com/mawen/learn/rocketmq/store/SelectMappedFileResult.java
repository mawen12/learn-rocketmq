package com.mawen.learn.rocketmq.store;

import com.mawen.learn.rocketmq.store.logfile.MappedFile;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/16
 */
@Getter
@Setter
@AllArgsConstructor
public class SelectMappedFileResult {

	protected int size;

	protected MappedFile mappedFile;
}
