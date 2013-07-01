package com.weibo.wejoy.data.service;

import com.weibo.wesync.data.WeSyncMessage.FileData;

public interface FileStoreService {
	//支持分片上传
	FileData saveFile(FileData fileData);

	byte[] getFile(String fid);
	
	//分片下载,同时支持完整下载
	byte[] getFileByIndex(FileData fileIndex);
}
