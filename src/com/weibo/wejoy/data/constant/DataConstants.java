package com.weibo.wejoy.data.constant;

public class DataConstants {

	public static long FILE_SIZE_THRESHOLD = 500 * 1024 * 1024; // 500k
	
	public  static final String DEFAULT_CHARSET = "utf-8";
	
	//消息id中时间分割符
	public static final String MESSAGE_ID_SEPARATOR = ".";
	
	
	/** 数据库表类型 **/
	public static final String DB_MESSAGE = "meta_message";
	
	/** 慢操作的时间区间定义 **/
	public static final int OP_TIMEOUT_H = 500;  
	public static final int OP_TIMEOUT_L = 100;
	
	//队列区分的分界值，默认4000B， 依据后期的统计结果调整
	public static final int MCQ_NORMAL_BOUNDARY = 4 * 1000;
	public static final int MCQ_LARGE_BOUNDARY = 8 * 1000;
	
	/** 存redis相关key */
	public static final String FOLDER_META_SUFFIX = ".m";
	public static final String FOLDER_CHILDREN_SUFFIX = ".c";
	public static final String FOLDER_CHANGE_SUFFIX = ".g";
	
	public static final String NOTICE_STORE_SUFFIX = ".n";
	public static final String OFFLINE_NOTICE_COUNTER_SUFFIX = ".o";
	
	
	
	
	/** 存储的消息索引最大条数，超过该条数的索引将被删除*/
	public static final int FOLDER_CHILDRE_LIMITED_SIZE = 200;
	/** 存储的未读消息索引最大条数，超过该条数的索引将被删除*/
	public static final int FOLDER_CHANGE_LIMITED_SIZE = 200;
	
	
	public static final int IDC_NUMBER = 100;
	public static final int IDC_INDEX = 1; //should be less than IDC_NUMBER
}
