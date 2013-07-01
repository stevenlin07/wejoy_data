package com.weibo.wejoy.data.processor;


import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 
 */
public class McqManager {

	/**
	 * 是否从队列读取消息
	 */
	public static AtomicBoolean IS_ALL_READ = new AtomicBoolean(true);
	
	// 是否按照value大小拆分写不同队列，默认不拆分
	public static AtomicBoolean DISTINCT_BY_SIZE = new AtomicBoolean(true);

	/**
	 * 打印当前控制的状态，用于查询一些配置选项
	 */
	public static String status() {
		StringBuilder sb = new StringBuilder(64);
		sb.append("mq read status :");
		sb.append(IS_ALL_READ.get());
		return sb.toString();
	}
}
