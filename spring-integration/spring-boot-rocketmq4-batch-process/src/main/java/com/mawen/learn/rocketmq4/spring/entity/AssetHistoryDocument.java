package com.mawen.learn.rocketmq4.spring.entity;

import java.io.Serializable;
import java.util.Date;

import com.mawen.learn.rocketmq4.spring.enums.PublishState;
import lombok.Data;

import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/27
 */
@Data
@Document(indexName = "dagp-asset-his", createIndex = true)
public class AssetHistoryDocument implements Serializable {

	private static final long serialVersionUID = -8314713811182326404L;

	@Field(type = FieldType.Keyword)
	private String tenantId;

	@Field(type = FieldType.Keyword)
	private String moduleCode;

	@Field(type = FieldType.Keyword)
	private String partitionCode;

	@Field(type = FieldType.Keyword)
	private String topicCode;

	@Field(type = FieldType.Keyword)
	private String id;

	@Field(type = FieldType.Keyword)
	private String assetCode;

	@Field(type = FieldType.Keyword)
	private String routing;

	@Field(type = FieldType.Keyword)
	private String chineseName;

	@Field(type = FieldType.Keyword)
	private String englishName;

	@Field(type = FieldType.Keyword)
	private String assetMean;

	@Field(type = FieldType.Keyword)
	private Date timestamp;

	@Field(type = FieldType.Keyword)
	private Integer hot;

	@Field(type = FieldType.Keyword)
	private Integer viewCount;

	@Field(type = FieldType.Keyword)
	private Integer searchCount;

	@Field(type = FieldType.Keyword)
	private PublishState publishState;
}
