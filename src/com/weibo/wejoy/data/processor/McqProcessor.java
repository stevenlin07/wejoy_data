package com.weibo.wejoy.data.processor;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import cn.sina.api.commons.cache.driver.VikaCacheClient;
import cn.sina.api.commons.util.ApiLogger;
import cn.sina.api.commons.util.StatLog;

/**
 * mcq 处理mcq，为每个mcq建立独立线程进行读取。写mcq，随机选择mcq写入，如果写失败，轮询下一个，直到尝试完所有的mcq
 * 
 * @author fishermen
 * @author zhulei 对该类轻量化
 *
 */
public abstract class McqProcessor {
	
	protected  List<VikaCacheClient>  mcqReaders;

	protected static AtomicInteger processorId = new AtomicInteger(0);
	/**
	 * 系统是否启动成功，启动成功方可开始处理消息
	 */
	protected volatile static boolean systemInitSuccess = false;
	
	//读取的线程数
	protected int readThreadCountEachMcq = 3;
	
	//连续读取的数量
	protected int readCountOnce = 100;
	
	//连续读取若干数量 or 没有读取到后的等待时间间隔
	protected int waitTimeOnce = 100;	
	
	protected List<Thread> readThreads = new ArrayList<Thread>();
	
	protected String readKey = null;
	
	public void startReading(){
		for(VikaCacheClient mcqr : mcqReaders){				
			int i = 0;
			while(i++ < readThreadCountEachMcq){
				Thread t = createReadThread(mcqr);
				t.start();
				readThreads.add(t);
			}				
		}
		
		startExtWork();
	}
	
	protected void startExtWork(){
	}
	protected abstract void handleMsq(Object msg);
	
	protected abstract String getStatMQReadFlag();
	
	protected abstract String getStatMQReadStatFlag();	

	protected Thread createReadThread(final VikaCacheClient mqr){
		Thread t = new Thread("thread_" + processorId.addAndGet(1) + "_mq_" + mqr.getServerPort()){
			@Override
			public void run() {				
				readFrmMQ(mqr);
			}
		};
		t.setDaemon(true);		
		return t;
	}
	
	protected void readFrmMQ(VikaCacheClient mqReader){
		// wait a moment for system init.
		waitForInit();

		ApiLogger.info(new StringBuilder(64).append("Start mq reader!KEY:").append(getReadKey())
				.append("\tServer:").append(mqReader.getServerPort()));
		AtomicInteger continueReadCount = new AtomicInteger(0);
		while(true){
			try {
				Object msg = null;	
				
				while(getMcqSwitch() && (msg = mqReader.get(getReadKey())) != null){
					StatLog.inc(getStatMQReadFlag());
					StatLog.inc(getStatMQReadStatFlag());
					if(ApiLogger.isTraceEnabled()){
						StatLog.inc(getMQReadDataKey(mqReader.getServerPort(), getReadKey()));
					}
										
					try {
						handleMsq(msg);		
						
						if(continueReadCount.addAndGet(1) % readCountOnce == 0){							
							safeSleep(waitTimeOnce);
							continueReadCount.set(0);
							//StatLog.inc(getMQReadSleepKey(mqReader.getServerPort(), getReadKey()), waitTimeOnce);
						}
												
					} catch (Exception e) {
						ApiLogger.warn(new StringBuilder(128).append("Error: processing the msg frm mq error, msg=").append(msg), e);
					}
				}
				
				if (!getMcqSwitch()) {
					ApiLogger.info("McqProcessor is alive but not read message.");
				}
				
				safeSleep(waitTimeOnce);
				StatLog.inc(getStatMQReadStatFlag());				
				
				//should response thread interrupted
				if(Thread.interrupted()){
					ApiLogger.warn(new StringBuilder(32).append("Thread interrupted :").append(Thread.currentThread().getName()));
					break;
				}
			} catch (Exception e) {
				ApiLogger.error(new StringBuilder("Error: when reship mq. key:").append(getReadKey()), e);
				e.printStackTrace();
				System.out.println(e.getMessage());
			}
		}
	}
	

	/**
	 * 等待系统初始化成功
	 * 如果systemInitSuccess为true，或者已经尝试3min，则开始执行读操作
	 */
	protected void waitForInit() {
		int total = 0;
		String msg = null;
		try {
			while (!systemInitSuccess && total++ < 90) {
				safeSleep(2000);
				msg = new StringBuilder(64).append("[Mcq Process]wait for system init! systemInitSuccess:")
								.append(systemInitSuccess).append("\tcount:").append(total).toString();
				ApiLogger.info(msg);
				System.out.println(msg);
			}
			safeSleep(10 * 1000); 
		} catch (Exception e) {
			ApiLogger.error("Error:when waitForInit", e);
			System.err.println("eoor");
		}
	}
	
	protected void safeSleep(int millis){
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {			
		}
	}
		
	public void setMcqReaders(List<VikaCacheClient> mcqReaders) {
		this.mcqReaders = mcqReaders;
		for(VikaCacheClient mcqr : mcqReaders){
			//来自坑爹的提醒: 此处必须设置为false，因为放入的是字节流，不是string，否则程序跑不通，还找不出为什么
			mcqr.getClient().setPrimitiveAsString(false);				
		}
	}

	public void setReadThreadCountEachMcq(int readThreadCountEachMcq) {
		this.readThreadCountEachMcq = readThreadCountEachMcq;
	}

	public void setReadCountOnce(int readCountOnce) {
		this.readCountOnce = readCountOnce;
	}

	public void setWaitTimeOnce(int waitTimeOnce) {
		this.waitTimeOnce = waitTimeOnce;
	}

	public String getReadKey() {
		return readKey;
	}

	public void setReadKey(String readKey) {
		this.readKey = readKey;
	}

	public static void setProcessorId(AtomicInteger processorId) {
		McqProcessor.processorId = processorId;
	}

	private String getMQReadDataKey(String serverPort, String key){
		return "read_mq_data_" + serverPort + "_" + key;
	}

	/**
	 * 设置系统初始化成功状态
	 */
	public static void setSystemInitSuccess() {
		systemInitSuccess = true;
	}
	
	protected boolean getMcqSwitch() {
		return McqManager.IS_ALL_READ.get();
	}
	
}
