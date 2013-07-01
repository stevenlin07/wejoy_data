package com.weibo.wejoy.data.dao.impl;

import com.weibo.wejoy.data.util.CommonUtil;

/**
 * <pre>
 *  
 *  分库分表策略 :
 *     db 		: 	   if (shareDBCount == 1) db is shareDBPrefix
 *     				   else  db is shareDBPrefix + ApiUtil.getHash4split(id, shareDBCount) + 1
 *  
 *     table    :
 * 	   suffix为， getDate(db) 
 * 			1106,      1105 .....
 *       	 ||         ||
 *      	 \/			\/
 *     		2011.06    2011.05	
 *     
 *     suffix = getDate(db) 
 *   	
 * </pre>
 *
 */
public class DateShareStrategy extends AbstractShareStrategy{

	@Override
	public String getDBName(String id) {
		if (shareDBCount == 1) return shareDBPrefix + "1";
		
		return shareDBPrefix + (CommonUtil.getHash4split(id, shareDBCount) + 1);
	}

	@Override
	public String getTableSuffix(String id) {
		String date = getTimeFromId(id);
		
		//原始时间格式：yyMMDD
		//需要时间格式：yyMM
		return date.substring(0, 6);
	}
	
	private String getTimeFromId(String id) {
//		int index = id.lastIndexOf(DataConstants.MESSAGE_ID_SEPARATOR);
//		String date = id.substring((index + 1), id.length());
		String date = id.substring(id.length() - 6 , id.length());

		return date;
	}
	
	public static void main(String[] args) {

		String id = "1603449647-prop-contact-701121104";

		DateShareStrategy tool = new DateShareStrategy();
		tool.shareDBCount = 4;
		tool.shareDBPrefix = "meta_message_";

		System.out.println("dbName: " + tool.getDBName(id));
		System.out.println("dbName: " + tool.getTableSuffix(id));

	}
}
