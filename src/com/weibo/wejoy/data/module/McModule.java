package com.weibo.wejoy.data.module;

import org.dom4j.Element;

import cn.sina.api.commons.cache.driver.VikaCacheClient;
import cn.sina.api.commons.util.ApiLogger;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.weibo.wejoy.data.storage.MemCacheStorage;
import com.weibo.wejoy.data.storage.MemCacheStorageImpl;
import com.weibo.wejoy.data.util.XmlUtil;

public class McModule extends AbstractBaseModule {
	
	private String configPath = "meyou-data-storage-mc.xml";
	
	@Override
	public String getConfigPath(){
		return configPath;
	}

	private void initConfig(VikaCacheClient cacheClient, Element config) throws Exception {
		String minSpareConnections = XmlUtil.getAttByName(config, "minSpareConnections");
		String maxSpareConnections = XmlUtil.getAttByName(config, "maxSpareConnections");
		String consistentHashEnable = XmlUtil.getAttByName(config, "consistentHashEnable");
		String failover = XmlUtil.getAttByName(config, "failover");
		
		cacheClient.setMinSpareConnections(Integer.valueOf(minSpareConnections));
		cacheClient.setMaxSpareConnections(Integer.valueOf(maxSpareConnections));
		cacheClient.setConsistentHashEnable(Boolean.valueOf(consistentHashEnable));
		cacheClient.setFailover(Boolean.valueOf(failover));
		
		String serverPort = XmlUtil.getAttByName(config, "serverport");
		cacheClient.setServerPort(serverPort);
		
		String primitiveAsString = XmlUtil.getAttByName(config, "primitiveAsString");
		if(primitiveAsString != null){
			boolean isPrimitivieAsString = Boolean.valueOf(primitiveAsString);			
			cacheClient.setPrimitiveAsString(isPrimitivieAsString);
		}
		
		cacheClient.init();
	}
	
	@SuppressWarnings("rawtypes")
	@Provides
	@Singleton
	protected MemCacheStorage provideMemCacheStorageImpl() {
		MemCacheStorageImpl<Object> ms = new MemCacheStorageImpl<Object>();

		try {
			Element elem = XmlUtil.getRootElement(document);
			String expire = XmlUtil.getAttByName(elem, "expire");
			ms.setExpire(Long.valueOf(expire));
			VikaCacheClient cacheClientMaster = new VikaCacheClient();
			initConfig(cacheClientMaster, XmlUtil.getElementByName(elem, "master"));
			ms.setCacheClientMaster(cacheClientMaster);

			VikaCacheClient cacheClientSlave = new VikaCacheClient();
			initConfig(cacheClientSlave, XmlUtil.getElementByName(elem, "slave"));
			ms.setCacheClientSlave(cacheClientSlave);

			return ms;
		} catch (Exception e) {
			ApiLogger.error("when provide MemCacheStorage, error occured,: ", e);
			throw new RuntimeException("provide MemCacheStorage error");
		}
	}

	public static void main(String[] args) {
		Injector injector = Guice.createInjector(new McModule());
		MemCacheStorage<Object> msgStore = injector.getInstance(MemCacheStorage.class);
		
//		System.out.println(msgStore.getClass().getName());
//		MetaMessagePB msg = MeyouTestUtil.createMetaMessage();
//		boolean result = msgStore.set(msg.id, msg.meta);
//		System.out.println(result == true);
//		System.out.println(new String((byte[])msgStore.get(msg.id)));
		
	}
}
