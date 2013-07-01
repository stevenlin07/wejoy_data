package com.weibo.wejoy.data.storage;

import java.util.Date;
import java.util.Map;

import cn.vika.memcached.CasValue;

public interface MemCacheStorage<T> {

	/**
	 * add value via key if key not exist
	 * 
	 * @param key
	 * @param value
	 * @param expdate
	 * @return
	 */
	public boolean add(String key, T value);

	/**
	 * add value via key if key not exist
	 * 
	 * @param key
	 * @param value
	 * @param expdate
	 * @return
	 */
	public boolean add(String key, T value, Date expdate);

	/**
	 * get value via key
	 * 
	 * @param rawKey
	 * @return
	 */
	public T get(String key);

	/**
	 * cas get value via key
	 * 
	 * @param key
	 * @return
	 */
	public CasValue<T> getCas(String key);

	/**
	 * get values via keys
	 * 
	 * @param keys
	 * @return
	 */
	public Map<String, T> getMulti(String[] keys);

	/**
	 * save key-value
	 * 
	 * @param key
	 * @param value
	 */
	public abstract boolean set(String key, T value);

	/**
	 * cas save key-value
	 * 
	 * @param key
	 * @param value
	 */
	public boolean setCas(String key, CasValue<T> value);

	/**
	 * cache value with expire date, some impl like database may ignore this
	 * parameter
	 * 
	 * @param key
	 * @param value
	 * @param expdate
	 * @return
	 */
	public boolean set(String key, T value, Date expdate);

	/**
	 * cas cache value with expire date, some impl like database may ignore this
	 * parameter
	 * 
	 * @param key
	 * @param value
	 * @param expdate
	 * @return
	 */
	public boolean setCas(String key, CasValue<T> value, Date expdate);

	/**
	 * delete value via key
	 * 
	 * @param key
	 * @return
	 */
	public boolean delete(String key);

}
