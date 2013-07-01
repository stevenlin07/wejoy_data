package com.weibo.wejoy.data.dao.impl;

import java.util.Map;

import cn.sina.api.data.dao.util.JdbcTemplate;

import com.weibo.wejoy.data.dao.ShareStrategy;

/**
 * Mapping hash and jdbcTemplate, you cat get the right jdbcTemplate by id via MblogClusterDaoSupport
 * 
 * @author fishermen
 *
 */
public class ClusterDatabases{
	public String getDBName(String id, String type) {
		ShareStrategy strategy = shareStrategys.get(type);
		
		if (strategy == null) return null;
		
		return strategy.getDBName(id);
	}
	
	public String getTableSuffix(String id, String type) {
		ShareStrategy strategy = shareStrategys.get(type);
		
		if (strategy == null) return null;
		
		return strategy.getTableSuffix(id);
	}
	

	public JdbcTemplate getIdxJdbcTemplate(String id, String type) {
		ShareStrategy strategy = shareStrategys.get(type);
		
		if (strategy == null) return null;
		
		return strategy.getIdxJdbcTemplate(id);
	}
	
	public void setShareStrategys(Map<String, ShareStrategy> shareStrategys) {
		this.shareStrategys = shareStrategys;
	}

	private Map<String, ShareStrategy> shareStrategys;
	

}
