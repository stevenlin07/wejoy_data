package com.weibo.wejoy.data.module;

import java.util.ArrayList;
import java.util.List;

import org.dom4j.Element;

import cn.sina.api.commons.cache.driver.VikaCacheClient;
import cn.sina.api.commons.util.ApiLogger;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.weibo.wejoy.data.dao.MessageDao;
import com.weibo.wejoy.data.processor.McqMessageParser;
import com.weibo.wejoy.data.processor.McqProcessor;
import com.weibo.wejoy.data.processor.McqWriter;
import com.weibo.wejoy.data.processor.MeyouMcqProcessor;
import com.weibo.wejoy.data.processor.MeyouMcqWriter;
import com.weibo.wejoy.data.storage.MemCacheStorage;
import com.weibo.wejoy.data.util.XmlUtil;


public class McqModule extends AbstractBaseModule {

	private String configPath = "meyou-data-mcq.xml";
	
	@Override
	public String getConfigPath(){
		return configPath;
	}

	@SuppressWarnings("unchecked")
	public List<VikaCacheClient> initMcqReadersAndWritersList(String tagName) {
		try {
			List<VikaCacheClient> mcqReadersAndWritersList = new ArrayList<VikaCacheClient>();
			Element rootEle = XmlUtil.getRootElement(document);
			Element mcqClients = XmlUtil.getElementByName(rootEle, "McqClients" + tagName);
			List<Element> mcqClientList = XmlUtil.getChildElements(mcqClients);

			for (Element mcqClient : mcqClientList) {
				VikaCacheClient vc = new VikaCacheClient();
				String maxSpareConnections = XmlUtil.getAttByName(mcqClient, "maxSpareConnections");
				String minSpareConnections = XmlUtil.getAttByName(mcqClient, "minSpareConnections");
				String compressEnable = XmlUtil.getAttByName(mcqClient, "compressEnable");
				String serverPort = XmlUtil.getAttByName(mcqClient, "serverPort");
				vc.setMaxSpareConnections(Integer.valueOf(maxSpareConnections));
				vc.setMinSpareConnections(Integer.valueOf(minSpareConnections));
				vc.setCompressEnable(Boolean.valueOf(compressEnable));
				vc.setServerPort(serverPort);
				vc.init();
				mcqReadersAndWritersList.add(vc);
			}

			return mcqReadersAndWritersList;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	@Provides
	@Singleton
	McqWriter provideMeyouMcqWriter() {
		MeyouMcqWriter mcqWriter = new MeyouMcqWriter();

		try {
			Element rootEle = XmlUtil.getRootElement(document);
			Element elemNoraml = XmlUtil.getElementByName(rootEle, "WesyncMcqProcessorNormal");
			String readAndWriteKeyNormal = XmlUtil.getAttByName(elemNoraml, "readAndWriteKey");
			mcqWriter.setWesyncNormalReadAndWriteKey(readAndWriteKeyNormal);

			Element elemMedium = XmlUtil.getElementByName(rootEle, "WesyncMcqProcessorMedium");
			String readAndWriteKeyMedium = XmlUtil.getAttByName(elemMedium, "readAndWriteKey");
			mcqWriter.setWesyncMediumReadAndWriteKey(readAndWriteKeyMedium);

			Element elemLarge = XmlUtil.getElementByName(rootEle, "WesyncMcqProcessorLarge");
			String readAndWriteKeyLarge = XmlUtil.getAttByName(elemLarge, "readAndWriteKey");
			mcqWriter.setWesyncLargeReadAndWriteKey(readAndWriteKeyLarge);

			mcqWriter.setMessageParser(GuiceProvider.getInstance(McqMessageParser.class));

			mcqWriter.setWesyncMcqNormalWriters(initMcqReadersAndWritersList("Normal"));
			mcqWriter.setWesyncMcqMediumWriters(initMcqReadersAndWritersList("Medium"));
			mcqWriter.setWesyncMcqLargeWriters(initMcqReadersAndWritersList("Large"));

			return mcqWriter;
		} catch (Exception e) {
			ApiLogger.error("initilizatize McqWriter error", e);
			throw new RuntimeException("initilizatize McqWriter error");
		}
	}
	
	@SuppressWarnings("unchecked")
	@Provides
	@Singleton
	@Named("normalProcessor")
	McqProcessor provideWesyncMcqNormalProcessor() {
		MeyouMcqProcessor mcqProcessor = new MeyouMcqProcessor();

		try {
			Element elem = XmlUtil.getElementByName(document, "WesyncMcqProcessorNormal");
			String readThreadCountEachMcq = XmlUtil.getAttByName(elem, "readThreadCountEachMcq");
			String readCountOnce = XmlUtil.getAttByName(elem, "readCountOnce");
			String waitTimeOnce = XmlUtil.getAttByName(elem, "waitTimeOnce");
			String readKey = XmlUtil.getAttByName(elem, "readAndWriteKey");
			mcqProcessor.setReadThreadCountEachMcq(Integer.valueOf(readThreadCountEachMcq));
			mcqProcessor.setReadCountOnce(Integer.valueOf(readCountOnce));
			mcqProcessor.setWaitTimeOnce(Integer.valueOf(waitTimeOnce));
			mcqProcessor.setReadKey(readKey);

			mcqProcessor.setMcqReaders(initMcqReadersAndWritersList("Normal"));
			mcqProcessor.setMessageDao(GuiceProvider.getInstance(MessageDao.class));
			mcqProcessor.setMsgStore(GuiceProvider.getInstance(MemCacheStorage.class));
			
			return mcqProcessor;
		} catch (Exception e) {
			ApiLogger.error("initilizatize McqProcessor error", e);
			throw new RuntimeException("initilizatize McqProcessor error");
		}
	}
	
	@SuppressWarnings("unchecked")
	@Provides
	@Singleton
	@Named("mediumProcessor")
	McqProcessor provideWesyncMcqMediumProcessor() {
		MeyouMcqProcessor mcqProcessor = new MeyouMcqProcessor();

		try {
			Element elem = XmlUtil.getElementByName(document, "WesyncMcqProcessorMedium");
			String readThreadCountEachMcq = XmlUtil.getAttByName(elem, "readThreadCountEachMcq");
			String readCountOnce = XmlUtil.getAttByName(elem, "readCountOnce");
			String waitTimeOnce = XmlUtil.getAttByName(elem, "waitTimeOnce");
			String readKey = XmlUtil.getAttByName(elem, "readAndWriteKey");
			mcqProcessor.setReadThreadCountEachMcq(Integer.valueOf(readThreadCountEachMcq));
			mcqProcessor.setReadCountOnce(Integer.valueOf(readCountOnce));
			mcqProcessor.setWaitTimeOnce(Integer.valueOf(waitTimeOnce));
			mcqProcessor.setReadKey(readKey);

			mcqProcessor.setMcqReaders(initMcqReadersAndWritersList("Medium"));
			mcqProcessor.setMessageDao(GuiceProvider.getInstance(MessageDao.class));
			mcqProcessor.setMsgStore(GuiceProvider.getInstance(MemCacheStorage.class));
			
			return mcqProcessor;
		} catch (Exception e) {
			ApiLogger.error("initilizatize McqProcessor error", e);
			throw new RuntimeException("initilizatize McqProcessor error");
		}
	}
	
	@SuppressWarnings("unchecked")
	@Provides
	@Singleton
	@Named("largeProcessor")
	McqProcessor provideWesyncMcqLargeProcessor() {
		MeyouMcqProcessor mcqProcessor = new MeyouMcqProcessor();

		try {
			Element elem = XmlUtil.getElementByName(document, "WesyncMcqProcessorLarge");
			String readThreadCountEachMcq = XmlUtil.getAttByName(elem, "readThreadCountEachMcq");
			String readCountOnce = XmlUtil.getAttByName(elem, "readCountOnce");
			String waitTimeOnce = XmlUtil.getAttByName(elem, "waitTimeOnce");
			String readKey = XmlUtil.getAttByName(elem, "readAndWriteKey");
			mcqProcessor.setReadThreadCountEachMcq(Integer.valueOf(readThreadCountEachMcq));
			mcqProcessor.setReadCountOnce(Integer.valueOf(readCountOnce));
			mcqProcessor.setWaitTimeOnce(Integer.valueOf(waitTimeOnce));
			mcqProcessor.setReadKey(readKey);

			mcqProcessor.setMcqReaders(initMcqReadersAndWritersList("Large"));
			mcqProcessor.setMessageDao(GuiceProvider.getInstance(MessageDao.class));
			mcqProcessor.setMsgStore(GuiceProvider.getInstance(MemCacheStorage.class));
			
			return mcqProcessor;
		} catch (Exception e) {
			ApiLogger.error("initilizatize McqProcessor error", e);
			throw new RuntimeException("initilizatize McqProcessor error");
		}
	}
	
	@Provides
	@Singleton
	McqMessageParser provideMcqMessageParser() {
		McqMessageParser messageParser = new McqMessageParser();

		return messageParser;
	}

	
	public static void main(String[] args) throws InterruptedException {
		Injector injector = Guice.createInjector(new McqModule());
//		McqWriter wesyncMcqWriter = injector.getInstance(McqWriter.class);
		McqProcessor processor = injector.getInstance(McqProcessor.class);
		processor.setSystemInitSuccess();
		
		Thread.sleep(5*1000);
//
//		MetaMessagePB msg = MeyouTestUtil.createMetaMessage();
//
//		wesyncMcqWriter.addMetaMessage(msg);
//		System.out.println(msg.id);

	}
}
