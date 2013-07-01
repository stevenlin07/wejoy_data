package com.weibo.wejoy.data.processor;

import cn.sina.api.commons.util.ApiLogger;

import com.weibo.wejoy.data.constant.DataConstants;
import com.weibo.wejoy.data.constant.McqProcessType;
import com.weibo.wejoy.data.dao.MessageDao;
import com.weibo.wejoy.data.model.MetaMessagePB;
import com.weibo.wejoy.data.model.MetaMessagePBUtil;
import com.weibo.wejoy.data.storage.MemCacheStorage;



public class MeyouMcqProcessor extends McqProcessor {

	public void init() {
		if (messageDao == null || msgStore == null) {
			throw new RuntimeException("WesyncMcqProcessor some service not be set");
		}

		startReading();
	}

	@Override
	public void handleMsq(Object msg) {
		try {
			long start = System.currentTimeMillis();
			
			MetaMessagePB metaMessage = MetaMessagePBUtil.parseFromPB((byte[]) msg);
			
			ApiLogger.info("[wesync read mcq " + getReadKey() + "], id = " + metaMessage.id);
			
			String type = metaMessage.type;
			if (McqProcessType.SAVE_META_MESSAGE.value().equals(type)) {
				
				processAddMetaMessage(metaMessage);
			} else {
				ApiLogger.warn(new StringBuilder(256).append("WesyncMcqProcessor process unknow type! msgid:").append(metaMessage.id));
			}

			long end = System.currentTimeMillis();
			if (end - start > DataConstants.OP_TIMEOUT_L) {
				ApiLogger.warn(new StringBuilder(50).append("wesync process mcq slow, type=").append(type).append(",cost=").append(end - start));
			}

		} catch (Exception e) {
			ApiLogger.error(new StringBuilder(256).append("WesyncMcqProcessor process error! "), e);
		}
	}

	protected void processAddMetaMessage(MetaMessagePB metaMessage) {
		// 1 入DB
		this.messageDao.saveMessage(metaMessage.id, metaMessage.meta);

		// 2 update cache
		//metaMessage入mq时已经写过mc， 故此处用add， key有的话直接忽略
		this.msgStore.add(metaMessage.id, metaMessage.meta);
	}

	@Override
	protected String getStatMQReadFlag() {
		return "all_mq_read_msg_process";
	}

	@Override
	protected String getStatMQReadStatFlag() {
		return "all_mq_read_stat_msg_process";
	}

	private MessageDao messageDao;
	private MemCacheStorage msgStore;
	
	public void setMessageDao(MessageDao messageDao) {
		this.messageDao = messageDao;
	}

	public void setMsgStore(MemCacheStorage msgStore) {
		this.msgStore = msgStore;
	}
}
