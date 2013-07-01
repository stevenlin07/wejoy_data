package com.weibo.wejoy.data.service.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jackson.map.JsonNode;

import cn.sina.api.commons.util.ApiLogger;
import cn.sina.api.commons.util.ApiUtil;
import cn.sina.api.commons.util.JsonWrapper;

import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import com.weibo.wejoy.data.constant.DataConstants;
import com.weibo.wejoy.data.service.ApiService;
import com.weibo.wejoy.data.util.HttpClientUtil;
import com.weibo.wejoy.data.util.Util;
import com.weibo.wejoy.data.util.tauth.TAuthUtil;
import com.weibo.wesync.data.MetaMessageType;
import com.weibo.wesync.data.WeSyncMessage.Meta;

public class ApiServiceImpl implements ApiService {
	private static final String AUTH_SOURCE = Util.getConfigProp("auth_source", "2841080378");
	
	private static final String DM_HOST_NAME= Util.getConfigProp("dm_host_url", "http://i2.api.weibo.com");
	
	private static final String DM_ISPUBLISHABLE_URL = DM_HOST_NAME+"/2/direct_messages/vp/is_publishable.json?check_saas=0&uid=";
	
	private static final String DM_SHOWBATCH_URL = DM_HOST_NAME+"/2/direct_messages/show_batch.json?source=";
	
	@Inject
	public ApiServiceImpl(HttpClientUtil httpclient){
		this.httpclient = httpclient;
		this.tauth = TAuthUtil.getInstance(this.httpclient);
	}
	
	@Override
	public String isPublishable(String fromuid, String touid) {
		Map<String, String> headers = new HashMap<String, String>();
		headers.put("authorization", tauth.getToken(fromuid,AUTH_SOURCE));
//		headers.put("cuid", fromuid); 
		ApiLogger.warn(fromuid+" in isPublishable with authorization header: "+ tauth.getToken(fromuid,AUTH_SOURCE)+ " with source = "+ AUTH_SOURCE);
		String dmIsPublishableUrl = new StringBuilder(128)
				.append(DM_ISPUBLISHABLE_URL).append(touid)
				.append("&source=").append(AUTH_SOURCE).toString();
		
		return this.httpclient.getAsync(dmIsPublishableUrl, headers, DataConstants.DEFAULT_CHARSET);
	}
	
	@Override
	public Meta getMetaMessage(String uid, long msgid) {
		Map<String, String> headers = new HashMap<String, String>();
		headers.put("authorization",tauth.getToken(uid,AUTH_SOURCE));
		ApiLogger.warn(uid +" in getMetaMessage with authorization header: "+ tauth.getToken(uid,AUTH_SOURCE)+ " with source = "+AUTH_SOURCE);
		String dmShowBatchUrl = new StringBuilder(128).append(DM_SHOWBATCH_URL)
				.append(AUTH_SOURCE).append("&dmids=").append(msgid).toString();
		
		String result = this.httpclient.getAsync(dmShowBatchUrl,  headers, DataConstants.DEFAULT_CHARSET);
		ApiLogger.warn(result);
		try{
			JsonWrapper resultJson = new JsonWrapper(result);
			Iterator<JsonNode> resultNodes = resultJson.getRootNode().getElements();
			
			JsonNode resultNode = null;
			while(resultNodes.hasNext()){
				resultNode = (JsonNode)resultNodes.next();
				break;
			}
			
			Meta meta = null;
			if (resultNode != null){
				int c_time = (int)(ApiUtil.parseDate(resultNode.getFieldValue("created_at").getTextValue(), new Date()).getTime()/1000);
				String content = resultNode.getFieldValue("text").getTextValue();
			
				meta = Meta.newBuilder()
					.setId(resultNode.getFieldValue("idstr").getTextValue())
					.setType(ByteString.copyFrom(new byte[]{MetaMessageType.text.toByte()}))
					.setContent(ByteString.copyFromUtf8(content))
					.setTime(c_time)
					.build();
			}
			return meta;
		}catch(Exception e){
			ApiLogger.warn("Error occured during parsing show_batch result json node"+result, e);
			return null;
		}
	}
	
	private HttpClientUtil httpclient;
	private static TAuthUtil tauth;
}
