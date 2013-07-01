package com.weibo.wejoy.data.util;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 
 * 此处放置服务控制和降级开关
 * 
 * @author liyuan7
 */
public class SwitchControll {
	
	public static AtomicBoolean isTrimChild = new AtomicBoolean(true);
	public static AtomicBoolean isTrimChange = new AtomicBoolean(true);

}
