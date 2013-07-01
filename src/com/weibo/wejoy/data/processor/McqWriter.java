package com.weibo.wejoy.data.processor;

import com.weibo.wejoy.data.model.MetaMessagePB;

public interface McqWriter {

	/**
	 * 新增加metaMessage
	 * 
	 * @param id
	 * @param meta
	 * @return
	 */
	boolean addMetaMessage(MetaMessagePB metaMessagePB);
}
