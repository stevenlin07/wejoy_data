package com.weibo.wejoy.data.module;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.dom4j.Element;

import cn.sina.api.commons.util.ApiLogger;
import cn.sina.api.data.dao.util.JdbcTemplate;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.weibo.wejoy.data.dao.MessageDao;
import com.weibo.wejoy.data.dao.ShareStrategy;
import com.weibo.wejoy.data.dao.impl.ClusterDatabases;
import com.weibo.wejoy.data.dao.impl.DateShareStrategy;
import com.weibo.wejoy.data.dao.impl.MessageDaoImpl;
import com.weibo.wejoy.data.util.CommonUtil;
import com.weibo.wejoy.data.util.XmlUtil;

public class DbModule extends AbstractBaseModule {
	
	private String configPath = "meyou-data-db.xml";
	protected String strategykey;	//this pointer will be used in method "getIdxJdbcTemplate" of class ClusterDatabases
	
	@Override
	public String getConfigPath(){
		return configPath;
	}
	
	@Provides
	@Singleton
	MessageDao provideMessageDao() {

		MessageDaoImpl messageDao = new MessageDaoImpl();
		try {
			messageDao.setClusterDatabases(GuiceProvider.getInstance(ClusterDatabases.class));
			return messageDao;
		} catch (Exception e) {
			ApiLogger.error("when provide MessageDao, error occured,: ", e);
			throw new RuntimeException("provide MessageDao error");
		}
	}
	
	@SuppressWarnings("unchecked")
	@Provides
	@Singleton
	protected ClusterDatabases provideClusterDatabases() {
		ClusterDatabases clusterDatabases = null;

		try {
			Element elem = XmlUtil.getRootElement(document);
			clusterDatabases = new ClusterDatabases();
			Map<String, ShareStrategy> strategyMap = new HashMap<String, ShareStrategy>();
			clusterDatabases.setShareStrategys(strategyMap);
			List<Element> strategys = XmlUtil.getChildElements(elem);

			for (Element strategy : strategys) {
				DateShareStrategy dateShareStrategy = new DateShareStrategy();
				strategykey = XmlUtil.getAttByName(strategy, "strategykey");
				String shareDBPrefix = XmlUtil.getAttByName(strategy, "shareDBPrefix");
				String shareDBCount = XmlUtil.getAttByName(strategy, "shareDBCount");
				dateShareStrategy.setShareDBCount(Integer.valueOf(shareDBCount));
				dateShareStrategy.setShareDBPrefix(shareDBPrefix);

				strategyMap.put(strategykey, dateShareStrategy);

				//db的config配置
				Element config = XmlUtil.getElementByName(strategy, "config");
				
				// master, slave配置
				Element jtl = XmlUtil.getElementByName(strategy, "jdbctemplate");

				Map<String, JdbcTemplate> wesyncJts = new HashMap<String, JdbcTemplate>();
				dateShareStrategy.setWesyncJts(wesyncJts);
				AtomicInteger counter = new AtomicInteger(1);

				int dbCount = CommonUtil.parseInteger(shareDBCount);
				Element masterConfig = XmlUtil.getElementByName(jtl, "master");
				ComboPooledDataSource masterDs = getDataSource(config);
				initDatasource(masterDs, masterConfig);

				Element slaveConfig = XmlUtil.getElementByName(jtl, "slave");
				ComboPooledDataSource slaveDs = getDataSource(config);
				initDatasource(slaveDs, slaveConfig);

				JdbcTemplate jtl0 = new JdbcTemplate();
				jtl0.setDataSource(masterDs);
				List<DataSource> slavelist = new ArrayList<DataSource>();
				slavelist.add(slaveDs);
				jtl0.setDataSourceSlaves(slavelist);
				for (int i = 0; i < dbCount; i++) {

					wesyncJts.put(shareDBPrefix + counter.getAndIncrement(), jtl0);
				}
			}
			return clusterDatabases;
		} catch (Exception e) {
			ApiLogger.error("when provide ClusterDatabases, error occured,: ", e);
			throw new RuntimeException("provide ClusterDatabases error");
		}

	}
	
	//TODO 类似这种db的基本配置，只应该加载一次，后期考虑优化
	private ComboPooledDataSource getDataSource(Element config) throws Exception {
		ComboPooledDataSource ds = new ComboPooledDataSource();
		
		String driver = XmlUtil.getAttByName(config, "driverClass");
		String minPoolSize = XmlUtil.getAttByName(config, "minPoolSize");
		String maxPoolSize = XmlUtil.getAttByName(config, "maxPoolSize");
		String idleConnectionTestPeriod = XmlUtil.getAttByName(config, "idleConnectionTestPeriod");
		String maxIdleTime = XmlUtil.getAttByName(config, "maxIdleTime");
		String breakAfterAcquireFailure = XmlUtil.getAttByName(config, "breakAfterAcquireFailure");
		String checkoutTimeout = XmlUtil.getAttByName(config, "checkoutTimeout");
		String acquireRetryAttempts = XmlUtil.getAttByName(config, "acquireRetryAttempts");
		String acquireRetryDelay = XmlUtil.getAttByName(config, "acquireRetryDelay");
		
		ds.setDriverClass(driver);
		ds.setMinPoolSize(Integer.valueOf(minPoolSize));
		ds.setMaxPoolSize(Integer.valueOf(maxPoolSize));
		ds.setIdleConnectionTestPeriod(Integer.valueOf(idleConnectionTestPeriod));
		ds.setMaxIdleTime(Integer.valueOf(maxIdleTime));
		ds.setBreakAfterAcquireFailure(Boolean.valueOf(breakAfterAcquireFailure));
		ds.setCheckoutTimeout(Integer.valueOf(checkoutTimeout));
		ds.setAcquireRetryAttempts(Integer.valueOf(acquireRetryAttempts));
		ds.setAcquireRetryDelay(Integer.valueOf(acquireRetryDelay));
		
		return ds;
	}
	
	private void initDatasource(ComboPooledDataSource ds, Element dselem) {
		ds.setJdbcUrl(XmlUtil.getAttByName(dselem, "url"));
		ds.setUser(XmlUtil.getAttByName(dselem, "user"));
		ds.setPassword(XmlUtil.getAttByName(dselem, "password"));
	}

	public static void main(String[] args) {
		Injector injector = Guice.createInjector(new DbModule());
		MessageDao messageDao = injector.getInstance(MessageDao.class);
		System.out.println(messageDao.getClass().getName());
		
//		MetaMessagePB msg = MeyouTestUtil.createMetaMessage();
//		System.out.println(msg.id);
//		System.out.println(messageDao.saveMessage(msg.id, msg.meta));
//		System.out.println(new String(messageDao.getMetaMessage(msg.id)));
	}
}
