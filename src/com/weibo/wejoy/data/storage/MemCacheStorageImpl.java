package com.weibo.wejoy.data.storage;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import cn.sina.api.commons.cache.MemcacheClient;
import cn.sina.api.commons.util.ApiLogger;
import cn.vika.memcached.CasValue;

/**
 * memcache storage
 * 
 * <pre>
 * 	 两种使用场景 :
 * 
 * 		(1) cacheClientSlave不存在， 读写cacheClientMaster
 *      (2) cacheClientSlave存在， 
 *      	[2.1] 双写cacheClientMaster和cacheClientSlave，
 *        	[2.2] 优先读取cacheClientMaster，读不到读cacheClientSlave，并将读到的写入cacheClientMaster
 * </pre>
 * 
 * @author maijunsheng
 * 
 * @param <T>
 */
public class MemCacheStorageImpl<T> implements MemCacheStorage<T> {
	public static AtomicBoolean SLAVE_USED = new AtomicBoolean(true);

	private Date expireTime;

	// the using cache, which also is master
	private MemcacheClient cacheClientMaster;

	// the slave/standby cache, for double set，to avoid single point
	private MemcacheClient cacheClientSlave;

	@Override
	public boolean add(String key, T value) {
		return add(key, value, expireTime);
	}

	@Override
	public boolean add(String key, T value, Date expdate) {
		boolean rs = cacheClientMaster.add(key, value, expdate);
		if (rs && getCacheClientSlave() != null) {
			// 这里要使用set，原因是master add成功了，为了保证一致性，需要使用set来保证一定成功，因为add会有不成功的可能
			cacheClientSlave.set(key, value, expdate);
		}
		return rs;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T get(String key) {
		T value = (T) cacheClientMaster.get(key);

		if (value == null && getCacheClientSlave() != null) {
			value = (T) cacheClientSlave.get(key);

			if (value != null) {
				cacheClientMaster.add(key, value, expireTime);
			}
		}

		return value;
	}

	/**
	 * 
	 * @param key
	 * @return
	 */
	@SuppressWarnings("unchecked")
	@Override
	public CasValue<T> getCas(String key) {
		CasValue<T> value = (CasValue<T>) cacheClientMaster.gets(key);

		if (value == null && getCacheClientSlave() != null) {
			T svalue = (T) cacheClientSlave.get(key);

			if (svalue != null) {
				cacheClientMaster.add(key, svalue, expireTime);
			}

			value = (CasValue<T>) cacheClientMaster.gets(key);
		}

		return value;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, T> getMulti(String[] keys) {
		Map<String, Object> cache = cacheClientMaster.getMulti(keys);

		Map<String, T> result = new HashMap<String, T>(keys.length);
		copy(cache, result);

		if (keys.length > result.size() && getCacheClientSlave() != null) {
			int len1 = result.size();
			int expect = keys.length - len1;

			List<String> leftKeysList = new ArrayList<String>(expect);
			for (String key : keys) {
				if (!result.containsKey(key)) {
					leftKeysList.add(key);
				}
			}
			if (leftKeysList.isEmpty()) {
				return result;
			}

			String[] keys2 = new String[leftKeysList.size()];
			leftKeysList.toArray(keys2);

			Map<String, Object> cacheSlave = cacheClientSlave.getMulti(keys2);

			for (String key : keys2) {
				Object obj = cacheSlave.get(key);
				if (obj != null) {
					result.put(key, (T) obj);
				}
			}

			int len2 = result.size();
			if (len2 - len1 > 20)
				ApiLogger.debug("GetMulti from slave good, add " + (len2 - len1) + "/" + expect + " keys[0] " + keys[0]);
			else if (expect - (len2 - len1) > 20)
				ApiLogger.debug("GetMulti from slave bad, add " + (len2 - len1) + "/" + expect + " keys[0] " + keys[0]);
		}

		return result;
	}

	@Override
	public boolean set(String key, T value) {
		return set(key, value, expireTime);
	}

	/**
	 * set key and value to mc, if set cacheClient success, we also set
	 * cacheClientStandby
	 * 
	 * @see we not set cas for cacheClientStandby, for the value is read from
	 *      cacheClient
	 */
	@Override
	public boolean set(String key, T value, Date expdate) {
		boolean rs = cacheClientMaster.set(key, value, expdate);
		if (rs && getCacheClientSlave() != null) {
			cacheClientSlave.set(key, value, expdate);
		}
		return rs;
	}

	@Override
	public boolean setCas(String key, CasValue<T> cas) {
		return setCas(key, cas, expireTime);
	}

	/**
	 * cas set key and value to mc, if set cacheClient success, we also set
	 * cacheClientStandby
	 * 
	 * @see we not set cas for cacheClientStandby, for the value is read from
	 *      cacheClient
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean setCas(String key, CasValue<T> cas, Date expdate) {
		boolean rs = cacheClientMaster.setCas(key, (CasValue<Object>) cas, expdate);

		// 除master外，其他应该是cacheClientSlave.set()，不能用setCas，因为1）master的cas unique
		// key,不一定等于slave的cas unique key
		if (rs && getCacheClientSlave() != null) {
			cacheClientSlave.set(key, cas.getValue(), expdate);
		}
		return rs;
	}

	@Override
	public boolean delete(String key) {
		boolean rs = cacheClientMaster.delete(key);

		if (getCacheClientSlave() != null) {
			cacheClientSlave.delete(key);
		}

		return rs;
	}

	public MemcacheClient getCacheClient() {
		return cacheClientMaster;
	}

	public MemcacheClient getCacheClientMaster() {
		return cacheClientMaster;
	}

	public void setCacheClientMaster(MemcacheClient cacheClientMaster) {
		this.cacheClientMaster = cacheClientMaster;
	}

	/**
	 * expire: unit is minute
	 * 
	 * @param expire
	 */
	public void setExpire(long expire) {
		if (expire > 0) {
			this.expireTime = new Date(1000l * 60 * expire);
		}
	}

	public MemcacheClient getCacheClientSlave() {
		if (!SLAVE_USED.get())
			return null;

		return cacheClientSlave;
	}

	public void setCacheClientSlave(MemcacheClient cacheClientSlave) {
		this.cacheClientSlave = cacheClientSlave;
	}

	/**
	 * 将value不为null的key从source 拷贝到 target
	 * 
	 * @param source
	 * @param target
	 */
	@SuppressWarnings("unchecked")
	private void copy(Map<String, Object> source, Map<String, T> target) {
		Iterator<Entry<String, Object>> it = source.entrySet().iterator();

		while (it.hasNext()) {
			Entry<String, Object> entry = it.next();

			if (entry.getValue() != null) {
				target.put(entry.getKey(), (T) entry.getValue());
			}
		}
	}
}
