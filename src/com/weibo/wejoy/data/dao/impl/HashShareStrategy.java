package com.weibo.wejoy.data.dao.impl;

import com.weibo.wejoy.data.util.CommonUtil;

/**
 * <pre>
 *  
 *  分库分表策略 :
 *     rang 		:  id % (shareDBCount * shareTableCount) 
 *  
 *     db 			:  if (shareDBCount == 1) db is shareDBPrefix
 *     				   else  db is shareDBPrefix + rang % shareDBCount + 1
 *  
 * 	   tableSuffix 	:  rang / shareDBCount + 1
 *   	
 * </pre>
 * 
 * @author maijunsheng
 *
 */
public class HashShareStrategy extends AbstractShareStrategy {
	
	public String getTableSuffix(String id) {
		long hash = CommonUtil.getHash4split(id, splitCount);
				
		return String.valueOf(hash / shareDBCount + 1);
	}

	@Override
	public String getDBName(String id) {
		if (shareDBCount == 1) return shareDBPrefix;
		
		long hash = CommonUtil.getHash4split(id, splitCount);

		return shareDBPrefix + ( hash % shareDBCount + 1);
	}

	public void setSplitCount(int splitCount) {
		this.splitCount = splitCount;
	}
	
	private int splitCount;
	
	public static void main(String[] args) {
		String id = "3455795_729600636.120898";

		HashShareStrategy tool = new HashShareStrategy();
		tool.shareDBCount = 16;
		tool.splitCount = 256;
		tool.shareDBPrefix = "meta_message_";

		System.out.println("dbName: " + tool.getDBName(id));
		System.out.println("dbName: " + tool.getTableSuffix(id));
	}
}
