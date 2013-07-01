package com.weibo.wejoy.data.service.impl;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

import cn.sina.api.commons.util.ApiLogger;
import cn.sina.api.commons.util.ByteArrayPart;

import com.google.inject.Inject;
import com.weibo.wejoy.data.constant.DataConstants;
import com.weibo.wejoy.data.service.FileStoreService;
import com.weibo.wejoy.data.util.HttpClientUtil;
import com.weibo.wesync.data.WeSyncMessage.FileData;

public class FileStoreServiceImpl implements FileStoreService {
	
	/** 默认配置 */
	public static final String AUTH_SOURCE = "2841080378";
	public static final String AUTH_CUID = "2615079113";
	public static final String DEFAULT_TUID = "2420223547";

	//上传文件服务器接口，支持分片和单个文件方式
	private static final String MEYOU_UPLOAD_URL = "http://upload.api.weibo.com/2/mss/meyou_upload.json?source=2841080378&tuid=2420223547";
	//下载单个文件
	private static final String DOWNLOAD_URL = "http://upload.api.weibo.com/2/mss/mswget?source=2841080378";
	//分片下载，POST方式
	private static final String MEYOU_DOWNLOAD_URL = "http://upload.api.weibo.com/2/mss/meyouget?source=2841080378";

	@Inject
	public FileStoreServiceImpl(HttpClientUtil httpClient) {
		this.httpClient = httpClient;
	}
	
	/**
	 * 支持分片上传
	 */
	@Override
	public FileData saveFile(FileData fileData) {
		if(fileData == null || fileData.getSliceList() == null) return null;
		FileData retFileData = null;
		
		String fid = fileData.getId();
		Map<String, Object> paramsMap = new HashMap<String, Object>();
		// 文件存储系统只支持multipart方式上传
		String contentType = "multipart/form-data"; 
		paramsMap.put("Content-Type", contentType);
		//FIXME
		//存储层负责解析fid，然后将整个文件pb传给文件服务器，文件服务器解析pb文件
		paramsMap.put("file", new ByteArrayPart(fileData.toByteArray(), "file", "application/octet-stream"));

		Map<String, String> headers = new HashMap<String, String>();
		headers.put("cuid", AUTH_CUID);
		
		String meyouUploadUrl = new StringBuilder(128).append(MEYOU_UPLOAD_URL).append("&fid=").append(fid).toString();
		
		//TODO 返回下一个需上传的pb文件
		//TODO 如协议层需获取文件服务器返回内容，调整该方法
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		boolean result = executeMultiMethod(meyouUploadUrl, headers, paramsMap, fid, out);
		
		if (result) {
			try {
				retFileData = FileData.parseFrom(out.toByteArray());
			} catch (Exception e) {
				ApiLogger.error("FileStoreServiceImpl saveFile parseFrom filedata error, ", e);
			}
		}else{
			ApiLogger.error("FileStoreServiceImpl saveFile failed:"+out.toString());
		}
		return retFileData;
	}
	
	@Override
	public byte[] getFile(String fid) {		
		Map<String, String> headers = new HashMap<String, String>();
		headers.put("cuid", AUTH_CUID);

		String downloadUrl = new StringBuilder(128).append(DOWNLOAD_URL).append("&fid=").append(fid).toString();
		byte[] fileData = executeGetMethod(downloadUrl, headers, null, fid);

		return fileData;
	}
	
	/** 
	 * 分片下载
	 * 
	 * 需POST方式
	 */
	@Override
	public byte[] getFileByIndex(FileData fileIndex) {
		String fid = fileIndex.getId();

		Map<String, Object> paramsMap = new HashMap<String, Object>();
		// 文件存储系统只支持multipart方式上传
		String contentType = "multipart/form-data";
		paramsMap.put("Content-Type", contentType);
		// FIXME
		paramsMap.put("file", new ByteArrayPart(fileIndex.toByteArray(), "file", "application/octet-stream"));

		Map<String, String> headers = new HashMap<String, String>();
		// FIXME need uid ? not need uid?
		headers.put("cuid", AUTH_CUID);

		String meyouDownloadUrl = new StringBuilder(128).append(MEYOU_DOWNLOAD_URL).append("&fid=").append(fid).toString();

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		boolean result = executeMultiMethod(meyouDownloadUrl, headers, paramsMap, fid, out);

		if (!result) {
			ApiLogger.error("getFileByIndex error, fid = " + fid);
			ApiLogger.error("getFileByIndex error, reason = " + out.toString());
			return null;
		}

		return out.toByteArray();
	}
	
	private boolean executeMultiMethod(String url, Map<String, String> headers, Map<String, Object> paramsMap, String fid, ByteArrayOutputStream out) {
		boolean ret = false;

		try {
			long start = System.currentTimeMillis();

			// 注意此处 流的方式
			int httpcode = httpClient.postMulti(url, headers, paramsMap, out);

			if (200 != httpcode)
				return false;

			String rt = new String(out.toByteArray(), DataConstants.DEFAULT_CHARSET);
			// LogUtil.debug("upload file to filestorage, return ------" + rt);

			long end = System.currentTimeMillis();

			if (end - start > DataConstants.OP_TIMEOUT_L) {
				ApiLogger.warn("upload to fileserer too slow, t=" + (end - start));
			}

			//根据文件系统返回判断是否成功上传
			if (rt != null && !"".equals(rt)) {
				ret = true;
			}

		} catch (Exception e) {
			ApiLogger.error("when upload to fileserer ,error occured, fid= " + fid, e);
		}

		return ret;
	}
	
	private byte[] executeGetMethod(String url, Map<String, String> headers, Map<String, String> paramsMap, String fid) {
		byte[] data = null;

		try {
			long start = System.currentTimeMillis();

			ByteArrayOutputStream out = new ByteArrayOutputStream();

			int httpcode = httpClient.getByte(DOWNLOAD_URL, headers, paramsMap, out);
			if (200 != httpcode)
				return null;

			data = out.toByteArray();

			long end = System.currentTimeMillis();

			if (end - start > DataConstants.OP_TIMEOUT_L) {
				ApiLogger.warn("download from fileserver too slow,t=" + (end - start));
			}

			return data;
		} catch (Exception e) {
			ApiLogger.error("when download from fileserer ,error occured, fid= " + fid, e);
			return null;
		}
	}
	
	private HttpClientUtil httpClient;
}
