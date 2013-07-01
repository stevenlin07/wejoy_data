package com.weibo.wejoy.data.service;

import java.util.Map;

import com.weibo.wesync.data.DataStore;
import com.weibo.wesync.data.WeSyncMessage.Meta;

public interface DataStoreService extends DataStore {
	
	//Return multi-folder's max childId number
	public Map<String, Integer> getMaxChildIdMulti(String[] folderIds);
}
