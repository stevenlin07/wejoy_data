package com.weibo.wejoy.data.constant;

/**
 * wesync处理消息类型
 * 
 * 方便扩展消息
 * 
 */
public enum McqProcessType {
	/** metaMessage **/
	SAVE_META_MESSAGE("100");

	public String value() {
		return value;
	}

	private McqProcessType(String value) {
		this.value = value;
	}

	private String value;
}
