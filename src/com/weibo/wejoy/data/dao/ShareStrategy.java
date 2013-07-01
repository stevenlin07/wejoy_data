package com.weibo.wejoy.data.dao;

import cn.sina.api.data.dao.util.JdbcTemplate;

public interface ShareStrategy {	
	String getDBName(String id);
	
	String getTableSuffix(String id);
	
	JdbcTemplate getIdxJdbcTemplate(String id);
}
