package com.weibo.wejoy.data.module;

import org.dom4j.Element;

import cn.sina.api.commons.util.ApiLogger;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.weibo.wejoy.data.service.FileStoreService;
import com.weibo.wejoy.data.service.impl.DataStoreServiceImpl;
import com.weibo.wejoy.data.service.impl.FileStoreServiceImpl;
import com.weibo.wejoy.data.util.CommonUtil;
import com.weibo.wejoy.data.util.HttpClientUtil;
import com.weibo.wejoy.data.util.XmlUtil;
import com.weibo.wejoy.wesync.dataservice.impl.DataServiceImpl;
import com.weibo.wesync.DataService;
import com.weibo.wesync.GroupMessageService;
import com.weibo.wesync.data.DataStore;


public class ServiceModule extends AbstractBaseModule {
	
	private String configPath = "meyou-data.xml";
	
	@Override
	public String getConfigPath(){
		return configPath;
	}
	
	@Override
	public void doOtherInitialization(){

		bind(FileStoreService.class).to(FileStoreServiceImpl.class);
		bind(DataStore.class).to(DataStoreServiceImpl.class);
		//bind(DataService.class).toInstance(new DataServiceImpl(GuiceProvider.getInstance(DataStore.class)));
		bind(DataService.class).to(DataServiceImpl.class);
		
//		bind(GroupMessageService.class).to(GroupMessageServiceImpl.class);
//		bind(NoticeStore.class).to(NoticeStoreImpl.class);
	}
	
	@Provides
	@Singleton
	HttpClientUtil provideHttpClientUtil() {
		try {
			Element elem = XmlUtil.getRootElement(document);

			Element clientEle = XmlUtil.getElementByName(elem, "HttpClient");
			String maxConPerHost = XmlUtil.getAttByName(clientEle, "maxConPerHost");
			String conTimeOutMs = XmlUtil.getAttByName(clientEle, "conTimeOutMs");
			String soTimeOutMs = XmlUtil.getAttByName(clientEle, "soTimeOutMs");
			String maxSize = XmlUtil.getAttByName(clientEle, "maxSize");
			String minThread = XmlUtil.getAttByName(clientEle, "minThread");
			String maxThread = XmlUtil.getAttByName(clientEle, "maxThread");

			HttpClientUtil httpClient = new HttpClientUtil(CommonUtil.parseInteger(maxConPerHost), CommonUtil.parseInteger(conTimeOutMs),
					CommonUtil.parseInteger(soTimeOutMs), CommonUtil.parseInteger(maxSize), CommonUtil.parseInteger(minThread),
					CommonUtil.parseInteger(maxThread));

			return httpClient;
		} catch (Exception e) {
			ApiLogger.error("when provide HttpClientUtil, error occured,: ", e);
			throw new RuntimeException("provide HttpClientUtil error");
		}
	}
	
//	@Provides
//	@Singleton
//	GroupMessageService provideGroupMessageService() {
//		GroupMessageServiceImpl groupMessageService = new GroupMessageServiceImpl(GroupGuiceProvider.getInstance(LocalGroupMcqWriter.class));
//
//		return groupMessageService;
//	}


	public static void main(String[] args) {
		Injector injector = Guice.createInjector(new ServiceModule());
		System.out.println(injector.getInstance(GroupMessageService.class));
//		DataStore dataStore = injector.getInstance(DataStore.class);

//		HttpClientUtil httpClient = injector.getInstance(HttpClientUtil.class);
//		System.out.println(httpClient.getClass().getName());
//		bind(DataService.class).toInstance(new DataServiceImpl(GuiceProvider.getInstance(DataStore.class)));
		
//		DataService dataService = new DataServiceImpl(GuiceProvider.getInstance(DataStore.class));
//		DataService dataService = injector.getInstance(DataService.class);
//		
//		
//		Meta meta= Meta.newBuilder()
//				.setFrom("juliet")
//				.setTo("romeo")
//				.setType(ByteString.copyFrom( new byte[]{com.weibo.wesync.Command.FolderSync.toByte()}))
//				.build();
//		dataService.store(meta);
		
//		System.out.println(injector.getInstance(MeyouMcqProcessor.class));
	}
	
}
