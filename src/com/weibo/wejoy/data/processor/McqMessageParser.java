package com.weibo.wejoy.data.processor;

import com.weibo.wejoy.data.model.MetaMessagePB;
import com.weibo.wejoy.data.model.MetaMessagePBUtil;

/**
 * Mcq Parse
 */
public class McqMessageParser {

	public byte[] toReadMqMsgAsBytes(MetaMessagePB metaMessagePB) {
		
		return MetaMessagePBUtil.toPB(metaMessagePB);
	}

}
