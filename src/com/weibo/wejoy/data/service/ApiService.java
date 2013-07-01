package com.weibo.wejoy.data.service;

import com.weibo.wesync.data.WeSyncMessage.Meta;


public interface ApiService {
	
	//http://i.api.weibo.com/2/direct_messages/vp/is_publishable.json
	/**
	 * 判断是否可以发送私信，同时返回是存在留言箱还是私信箱
	 * 
	 * @param fromuid
	 * @param touid
	 * @return  {"result":true,"topublic":false}
	 */
	public String isPublishable(String fromuid, String touid);
	
	public Meta getMetaMessage(String uid, long msgid);
}
