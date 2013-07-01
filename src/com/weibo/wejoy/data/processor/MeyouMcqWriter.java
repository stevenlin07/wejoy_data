package com.weibo.wejoy.data.processor;

import java.util.List;
import java.util.Random;

import cn.sina.api.commons.cache.driver.VikaCacheClient;
import cn.sina.api.commons.util.ApiLogger;
import cn.sina.api.commons.util.StatLog;

import com.weibo.wejoy.data.constant.DataConstants;
import com.weibo.wejoy.data.constant.McqProcessType;
import com.weibo.wejoy.data.model.MetaMessagePB;

/**
 * 异步写wesync队列
 * 
 * 注意：
 * 由于写入的value是metaMessage的pb格式，在记录日志时，不打印value，只打印消息id
 * FIXME 如此做的话，修复数据时会存在问题？ how to do ??
 * 
 * 流程
 * <pre>
 * （1）dataService收到消息后写mcq和mc，返回
 * （2）processor读mq，异步写db，写索引，再次更新缓存
 * </pre>
 * 
 */
public class MeyouMcqWriter implements McqWriter {
	@Override
	public boolean addMetaMessage(MetaMessagePB metaMessage) {
		try {
			metaMessage.type = McqProcessType.SAVE_META_MESSAGE.value();
			byte[] bytesMsg = messageParser.toReadMqMsgAsBytes(metaMessage);

			writeMsg(metaMessage.id, bytesMsg);

			return true;
		} catch (RuntimeException e) {
			ApiLogger.error(new StringBuilder(256).append("WesyncMcqWriter addMetaMessage error, msgid=").append(metaMessage.id), e);
			throw e;
		}
	}

	/**
	 * 写给wesync队列
	 * 
	 * @param id
	 * @param msg
	 */
	protected void writeMsg(String id, byte[] msg){
		if (ApiLogger.isDebugEnabled()) {
			ApiLogger.debug("[write wesync mcq], id=" + id);
		}

		// for stat
		ApiLogger.info("Mcq write size = " + msg.length);
		
		//是否区分写mcq
		if (McqManager.DISTINCT_BY_SIZE.get() && wesyncMcqLargeWriters != null && wesyncMcqLargeWriters.size() > 0) {
			if (msg != null && msg.length < DataConstants.MCQ_NORMAL_BOUNDARY) {
				writeMsg(wesyncMcqNormalWriters, wesyncNormalReadAndWriteKey, msg, id);
			} else if(msg != null && msg.length > DataConstants.MCQ_LARGE_BOUNDARY){
				writeMsg(wesyncMcqLargeWriters, wesyncLargeReadAndWriteKey, msg, id);
			} else{
				writeMsg(wesyncMcqMediumWriters, wesyncMediumReadAndWriteKey, msg, id);
			}
			return;
		}
		
		//default write meta to large mcq
		writeMsg(wesyncMcqLargeWriters, wesyncLargeReadAndWriteKey, msg, id);
	}

	protected void writeMsg(List<VikaCacheClient> writers, String key, Object msg, String id) {
		long start = System.currentTimeMillis();
		if (writers == null || writers.size() == 0) {
			return;
		}
		int rd = random.nextInt(writers.size());
		/*
		 * 1、对每条消息轮询所有的mcq，如果处理成功则直接返回。 2、如果处理失败，则尝试写入下一个mcq。
		 * 3、如果所有的mcq均写入失败，则不做处理。
		 */
		boolean writeRs = false;
		for (int i = 0; i < writers.size(); i++) {
			int index = (i + rd) % writers.size();
			VikaCacheClient mqWriter = writers.get(index);

			try {
				if (mqWriter.set(key, msg)) {
					writeRs = true;
					StatLog.inc(getMQWriteKey(mqWriter.getServerPort(), key));
					if (ApiLogger.isDebugEnabled()) {
						ApiLogger.debug(new StringBuilder(256).append("mcq=").append(mqWriter.getServerPort()).append(", key=").append(key)
								.append(", id=").append(id));
					}
					break;
				}
			} catch (Exception e) {
				ApiLogger.warn(new StringBuilder(128).append("Warn: save msg to one mq false [try next], key=").append(key).append(", mq=")
						.append(mqWriter.getServerPort()).append(",id").append(id), e);
			}
			StatLog.inc(getMQWriteErrorKey(mqWriter.getServerPort(), key));
			ApiLogger.info(new StringBuilder(128).append("Info: save msg to mq false, key=").append(key).append(",mq=")
					.append(mqWriter.getServerPort()).append(",id").append(id));
		}
		long end = System.currentTimeMillis();
		if (end - start > DataConstants.OP_TIMEOUT_L) {
			ApiLogger.warn(key + " write to mcq too slow, t=" + (end - start));
		}

		if (!writeRs) {
			ApiLogger.error(new StringBuilder(128).append("Write mcq false, key=").append(key).append(", msg=").append(msg));
			throw new IllegalArgumentException(new StringBuilder(128).append("Write mcq false, key=").append(key).append(", id=")
					.append(id).toString());
		}
	}

	private String getMQWriteKey(String serverPort, String key) {
		return "write_mq_" + serverPort + "_" + key;
	}

	private String getMQWriteErrorKey(String serverPort, String key) {
		return getStatMQWriteErrorFlag() + "_" + serverPort + "_" + key;
	}

	private String getStatMQWriteErrorFlag() {
		return "err_write_msg_process";
	}

	protected Random random = new Random();
	
	protected McqMessageParser messageParser;
	protected String wesyncNormalReadAndWriteKey;
	protected String wesyncMediumReadAndWriteKey;
	protected String wesyncLargeReadAndWriteKey;

	protected List<VikaCacheClient> wesyncMcqNormalWriters;
    protected List<VikaCacheClient> wesyncMcqMediumWriters;
    protected List<VikaCacheClient> wesyncMcqLargeWriters;

	public void setMessageParser(McqMessageParser messageParser) {
		this.messageParser = messageParser;
	}

	public void setWesyncMcqNormalWriters(List<VikaCacheClient> wesyncMcqNormalWriters) {
		this.wesyncMcqNormalWriters = wesyncMcqNormalWriters;
	}

	public void setWesyncMcqMediumWriters(List<VikaCacheClient> wesyncMcqMediumWriters) {
		this.wesyncMcqMediumWriters = wesyncMcqMediumWriters;
	}

	public void setWesyncMcqLargeWriters(List<VikaCacheClient> wesyncMcqLargeWriters) {
		this.wesyncMcqLargeWriters = wesyncMcqLargeWriters;
	}

	public void setWesyncNormalReadAndWriteKey(String wesyncNormalReadAndWriteKey) {
		this.wesyncNormalReadAndWriteKey = wesyncNormalReadAndWriteKey;
	}

	public void setWesyncMediumReadAndWriteKey(String wesyncMediumReadAndWriteKey) {
		this.wesyncMediumReadAndWriteKey = wesyncMediumReadAndWriteKey;
	}

	public void setWesyncLargeReadAndWriteKey(String wesyncLargeReadAndWriteKey) {
		this.wesyncLargeReadAndWriteKey = wesyncLargeReadAndWriteKey;
	}
}
