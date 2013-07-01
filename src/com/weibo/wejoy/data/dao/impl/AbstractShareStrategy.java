package com.weibo.wejoy.data.dao.impl;

import java.util.Map;

import cn.sina.api.data.dao.util.JdbcTemplate;

import com.weibo.wejoy.data.dao.ShareStrategy;

public abstract class AbstractShareStrategy implements ShareStrategy{
	/**
	 * 获取分库的JdbcTemplate
	 * @param id	ID Hash策略
	 * @return	JdbcTemplate
	 */
	@Override
	public JdbcTemplate getIdxJdbcTemplate(String id){
		String dbname = getDBName(id);
		JdbcTemplate jt = wesyncJts.get(dbname);
		
		if(jt != null){
			return jt;
		}
		throw new IllegalArgumentException("Bad dbhost in ClusterDatabases.getIdxJdbcTemplate, dbname=" + dbname);
	}

	public void setShareTableCount(int shareTableCount) {
		this.shareTableCount = shareTableCount;
	}

	public void setShareDBPrefix(String shareDBPrefix) {
		this.shareDBPrefix = shareDBPrefix;
	}
	
	public void setShareDBCount(int shareDBCount) {
		this.shareDBCount = shareDBCount;
	}
	
	public void setWesyncJts(Map<String, JdbcTemplate> wesyncJts) {
		this.wesyncJts = wesyncJts;
	}
	
	protected String shareDBPrefix;
	protected int shareDBCount;
	protected int shareTableCount;
	protected Map<String, JdbcTemplate> wesyncJts;

}
