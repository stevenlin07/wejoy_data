package com.weibo.wejoy.data.dao;

public interface MessageDao {

//	boolean saveMessage(String metaMsgId, MetaMessage metaMsg);
//	
//	MetaMessage getMetaMessageNonePb(String metaMsgId);
	
	boolean saveMessage(String metaMsgId, byte[] metaMsg);
	
	byte[] getMetaMessage(String metaMsgId);
	
}
