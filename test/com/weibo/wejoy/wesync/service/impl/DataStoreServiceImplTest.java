package com.weibo.wejoy.wesync.service.impl;

import java.util.Date;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import cn.sina.api.commons.util.ApiUtil;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.weibo.wejoy.data.module.ServiceModule;
import com.weibo.wejoy.data.service.ApiService;
import com.weibo.wejoy.data.util.HttpClientUtil;
import com.weibo.wesync.data.FolderChild;
import com.weibo.wesync.data.FolderID;
import com.weibo.wesync.data.WeSyncMessage.Meta;

public class DataStoreServiceImplTest {
	private ApiService apiService;
	private HttpClientUtil httpClient;
	
	@Before
	public void setUp() throws Exception {
		Injector injector = Guice.createInjector(new ServiceModule());
		apiService = injector.getInstance(ApiService.class);
	}

	@Test
	public void testGetMetaMessage() {
		String metaMsgId = "1737089987-conv-2013667532-3562702150620300";
		if (metaMsgId.indexOf(FolderChild.SPLIT) > 0) {
			long dmMsgId = FolderChild.getScore(metaMsgId);
			String folderId = metaMsgId.substring(0, metaMsgId.lastIndexOf(FolderChild.SPLIT));
			String fromuid = FolderID.getUsername(folderId);
			
			Meta meta = this.apiService.getMetaMessage(fromuid, FolderChild.getScore(metaMsgId));
			
			Assert.assertNotNull(ApiUtil.formatDate(new Date(meta.getTime()*1000L), new Date().toGMTString()));
			System.out.println(ApiUtil.formatDate(new Date(meta.getTime()*1000L), new Date().toGMTString()));
			Assert.assertNotNull(meta.getContent());
			System.out.println("Meta Content is"+meta.getContent().toStringUtf8());
		} 
	}

}