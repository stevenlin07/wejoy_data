package com.weibo.wejoy.data.module;

import java.util.LinkedList;
import java.util.List;

import org.dom4j.Element;

import cn.sina.api.commons.redis.jedis.CustomJedisConfig;
import cn.sina.api.commons.redis.jedis.JedisMSServer;
import cn.sina.api.commons.redis.jedis.JedisPort;
import cn.sina.api.commons.util.ApiLogger;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.weibo.wejoy.data.storage.RedisStorage;
import com.weibo.wejoy.data.storage.RedisStorageImpl;
import com.weibo.wejoy.data.util.CommonUtil;
import com.weibo.wejoy.data.util.XmlUtil;

public class RedisModule extends AbstractBaseModule {
	
	private String configPath = "msgbox-data-storage-redis.xml";
	
	@Override
	public String getConfigPath(){
		return configPath;
	}
	
	public void doOtherInitialization() {
		//单个interface多个实现类，有两种实现方式,
		/**注意，下面的代码不要去掉
//		RedisStorage foldIdStorage = provideFoldId();
//		RedisStorage foldChangeStorage = provideFolderChange();
//		RedisStorage foldChildStorage = provideFolderChild();

//		bind(RedisStorage.class).annotatedWith(Names.named("folderId")).toInstance(foldIdStorage);
//		bind(RedisStorage.class).annotatedWith(Names.named("folderChild")).toInstance(foldChangeStorage);
//		bind(RedisStorage.class).annotatedWith(Names.named("folderChange")).toInstance(foldChildStorage);
 */
	}

	@Provides
	@Singleton
	@Named("folderId")
	RedisStorage provideFolderId() {
		try {
			Element root = XmlUtil.getRootElement(document);
			Element configEle = XmlUtil.getElementByName(root, "config");
			Element folderChangeEle = XmlUtil.getElementByName(root, "folderIdList");
			List<Element> serversEle = XmlUtil.getChildElementsByName(folderChangeEle, "server");

			CustomJedisConfig jedisConfig = provideJedisConfig(configEle);
			List<JedisPort> servers = provideJedisPort(jedisConfig, serversEle);

			RedisStorage redisStorage = createRedisStorage(root, servers);

			return redisStorage;
		} catch (Exception e) {
			ApiLogger.error("when provide RedisStorage, error occured,: ", e);
			throw new RuntimeException("provide RedisStorage error");
		}
	}
	
	
	@Provides
	@Singleton
	@Named("folderChild")
	RedisStorage provideFolderChild() {
		try {
			Element root = XmlUtil.getRootElement(document);
			Element configEle = XmlUtil.getElementByName(root, "config");
			Element folderChangeEle = XmlUtil.getElementByName(root, "folderChildList");
			List<Element> serversEle = XmlUtil.getChildElementsByName(folderChangeEle, "server");

			CustomJedisConfig jedisConfig = provideJedisConfig(configEle);
			List<JedisPort> servers = provideJedisPort(jedisConfig, serversEle);

			RedisStorage redisStorage = createRedisStorage(root, servers);

			return redisStorage;
		} catch (Exception e) {
			ApiLogger.error("when provide RedisStorage, error occured,: ", e);
			throw new RuntimeException("provide RedisStorage error");
		}
	}
	
	@Provides
	@Singleton
	@Named("folderChange")
	RedisStorage provideFolderChange() {
		try {
			Element root = XmlUtil.getRootElement(document);
			Element configEle = XmlUtil.getElementByName(root, "config");
			Element folderChangeEle = XmlUtil.getElementByName(root, "folderChangeList");
			List<Element> serversEle = XmlUtil.getChildElementsByName(folderChangeEle, "server");

			CustomJedisConfig jedisConfig = provideJedisConfig(configEle);
			List<JedisPort> servers = provideJedisPort(jedisConfig, serversEle);

			RedisStorage redisStorage = createRedisStorage(root, servers);

			return redisStorage;
		} catch (Exception e) {
			ApiLogger.error("when provide RedisStorage, error occured,: ", e);
			throw new RuntimeException("provide RedisStorage error");
		}
	}
	
	private CustomJedisConfig provideJedisConfig(Element configEle){
		String maxActive = XmlUtil.getAttByName(configEle, "maxActive");
		String maxIdle = XmlUtil.getAttByName(configEle, "maxIdle");
		String minIdle = XmlUtil.getAttByName(configEle, "minIdle");
		String whenExhaustedAction = XmlUtil.getAttByName(configEle, "whenExhaustedAction");
		String maxWait = XmlUtil.getAttByName(configEle, "maxWait");
		String lifo = XmlUtil.getAttByName(configEle, "lifo");
		
		String testOnBorrow = XmlUtil.getAttByName(configEle, "testOnBorrow");
		String testOnReturn = XmlUtil.getAttByName(configEle, "testOnReturn");
		String testWhileIdle = XmlUtil.getAttByName(configEle, "testWhileIdle");
		String numTestsPerEvictionRun = XmlUtil.getAttByName(configEle, "numTestsPerEvictionRun");
		String timeBetweenEvictionRunsMillis = XmlUtil.getAttByName(configEle, "timeBetweenEvictionRunsMillis");
		String softMinEvictableIdleTimeMillis = XmlUtil.getAttByName(configEle, "softMinEvictableIdleTimeMillis");
		String minEvictableIdleTimeMillis = XmlUtil.getAttByName(configEle, "minEvictableIdleTimeMillis");
		
		CustomJedisConfig jedisConfig = new CustomJedisConfig();
		jedisConfig.setMaxActive(CommonUtil.parseInteger(maxActive));
		jedisConfig.setMaxIdle(CommonUtil.parseInteger(maxIdle));
		jedisConfig.setMinIdle(CommonUtil.parseInteger(minIdle));
		jedisConfig.setWhenExhaustedAction((byte)CommonUtil.parseInteger(whenExhaustedAction));
		jedisConfig.setMaxWait(CommonUtil.parseLong(maxWait));
		jedisConfig.setLifo(CommonUtil.parseBoolean(lifo));
		
		jedisConfig.setTestOnBorrow(CommonUtil.parseBoolean(testOnBorrow));
		jedisConfig.setTestOnReturn(CommonUtil.parseBoolean(testOnReturn));
		jedisConfig.setTestWhileIdle(CommonUtil.parseBoolean(testWhileIdle));
		jedisConfig.setNumTestsPerEvictionRun(CommonUtil.parseInteger(numTestsPerEvictionRun));
		jedisConfig.setTimeBetweenEvictionRunsMillis(CommonUtil.parseLong(timeBetweenEvictionRunsMillis));
		jedisConfig.setSoftMinEvictableIdleTimeMillis(CommonUtil.parseLong(softMinEvictableIdleTimeMillis));
		jedisConfig.setMinEvictableIdleTimeMillis(CommonUtil.parseLong(minEvictableIdleTimeMillis));
		
		return jedisConfig;
	}
	
	private List<JedisPort> provideJedisPort(CustomJedisConfig jedisConfig, List<Element> serverEle) {
		List<JedisPort> servers = new LinkedList<JedisPort>();

		for (Element ele : serverEle) {
			String hashMin = XmlUtil.getAttByName(ele, "hashMin");
			String hashMax = XmlUtil.getAttByName(ele, "hashMax");
			String masterServer = XmlUtil.getAttByName(ele, "masterServer");
			String slaveServer = XmlUtil.getAttByName(ele, "slaveServer");

			JedisMSServer server = new JedisMSServer(jedisConfig, CommonUtil.parseInteger(hashMin), CommonUtil.parseInteger(hashMax));
			server.setMasterServer(masterServer);
			server.setSlaveServer(slaveServer);
			server.init();
			

			// for HA server
//			List<String> serversStr = new ArrayList<String>();
//			serversStr.add(masterServer);
//			serversStr.add(slaveServer);
//			server.setServers(serversStr);
//			servers.add(server);
//
//			server.setDoubleWrite(false);
			servers.add(server);
		}

		return servers;
	}

	
	private RedisStorage createRedisStorage(Element root, List<JedisPort> servers) {
		RedisStorageImpl redisStorage = new RedisStorageImpl();

		String consistNum = XmlUtil.getAttByName(root, "consistNum");
		String hashAlg = XmlUtil.getAttByName(root, "hashAlg");
		String suffix = XmlUtil.getAttByName(root, "suffix");
		String counterInfo = XmlUtil.getAttByName(root, "counterInfo");

		redisStorage.setConsistNum(CommonUtil.parseInteger(consistNum));
		redisStorage.setHashAlg(hashAlg);
		redisStorage.setSuffix(suffix);
		redisStorage.setCounterInfo(counterInfo);
		redisStorage.setJedisPort(servers);

		redisStorage.init();

		return redisStorage;
	}
	
	

	public static void main(String[] args) {
		
		Injector injector = Guice.createInjector(new RedisModule());
		RedisStorage foldIdStorage = injector.getInstance(Key.get(RedisStorage.class, Names.named("folderId")));
		RedisStorage foldChangeStorage = injector.getInstance(Key.get(RedisStorage.class, Names.named("folderChange")));
		RedisStorage foldChildStorage = injector.getInstance(Key.get(RedisStorage.class, Names.named("folderChild")));
		
		String folderId = "testconv";
		System.out.println("folderId: " + foldIdStorage.set(folderId, "10"));
		System.out.println("folderId: " + foldIdStorage.getLong(folderId));
		
		System.out.println(foldChildStorage.zadd(folderId, 55.0, "1352234231209"));
		System.out.println(foldChildStorage.zcard(folderId));
		System.out.println(foldChildStorage.zrangeWithScores(folderId, 0, -1).toString());
		
		
		
	}
}
