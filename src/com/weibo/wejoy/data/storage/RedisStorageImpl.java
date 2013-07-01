package com.weibo.wejoy.data.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.CRC32;

import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisException;
import cn.sina.api.commons.redis.jedis.CodecHandler;
import cn.sina.api.commons.redis.jedis.JedisPort;
import cn.sina.api.commons.util.ApiLogger;
import cn.sina.api.commons.util.StatLog;
import cn.sina.api.data.storage.redis.CountDownLatchHelper;

import com.weibo.wejoy.data.util.CommonUtil;


public class RedisStorageImpl implements RedisStorage {

	public void init() {
		StatLog.registerVar(getMethod);
		StatLog.registerVar(getmultiMethod);
		StatLog.registerVar(setMethod);
		StatLog.registerVar(delMethod);
		StatLog.registerVar(delMethod);
		StatLog.registerVar(decMethod);
	}

	@Override
	public boolean isAlive() {
		for (JedisPort server : servers) {
			if (!server.isAlive()) {
				return false;
			}
		}
		return true;
	}

	public String getString(final String key) {
		StatLog.inc(getMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					final String value = server.get(redisKey);
					if (value != null) {
						return value;
					} else {
						return null;
					}
				}
			}
			return null;
		} catch (Exception e) {
			ApiLogger.warn("get " + key + " " + e);
			return null;
		}
	}

	@Override
	public Long hget(final String key, String hashKey) {
		StatLog.inc(getMethod);

		try {
			final int num = getHashNum(hashKey);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					final String value = server.get(redisKey);
					if (value != null) {
						return Long.parseLong(value);
					} else {
						return null;
					}
				}
			}
			return null;
		} catch (Exception e) {
			ApiLogger.warn("get " + key + " " + e);
			return null;
		}
	}

	@Override
	public boolean hset(final String key, String hashKey, String value) {
		StatLog.inc(setMethod);

		try {
			final int num = getHashNum(hashKey);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					return server.set(redisKey, value);

				}
			}
			return false;
		} catch (Exception e) {
			ApiLogger.warn("get " + key + " " + "value " + value + "" + e);
			return false;
		}
	}

	@Override
	public Map<String, byte[]> getMulti(final String[] keys) {
		StatLog.inc(getmultiMethod);

		return getMultiPaiallel(keys);
	}

	@Override
	public Map<String, Long> getMulti(final String[] keys, String[] hashKeys) {
		StatLog.inc(getmultiMethod);

		return getMultiPaiallel(keys, hashKeys);
	}

	public Map<String, byte[]> getMultiPaiallel(final String[] keys) {
		try {
			// split by hash
			final Map<JedisPort, List<String>> splitMap = new HashMap<JedisPort, List<String>>();
			for (final String key : keys) {
				final int num = getHashNum(key);
				String redisKey = getRedisKey(key);

				for (final JedisPort server : servers) {
					if (server.contains(num)) {
						List<String> keyList = splitMap.get(server);
						if (keyList == null) {
							keyList = new ArrayList<String>();
							splitMap.put(server, keyList);
						}
						keyList.add(redisKey);
					}
				}
			}

			// parse result to long and combine
			final Map<String, byte[]> result = new HashMap<String, byte[]>();
			final Map<JedisPort, Map<String, String>> rets = CountDownLatchHelper.redisMget2(splitMap);

			for (final JedisPort server : splitMap.keySet()) {
				final Map<String, String> ret = rets.get(server);
				if (ret == null) {
					continue;
				}
				for (final String key : ret.keySet()) {
					final String value = ret.get(key);
					if (value == null) {
						continue;
					}
					result.put(key, CommonUtil.encode(value));
				}
			}

			return result;
		} catch (Exception e) {
			ApiLogger.warn("getMulti " + Arrays.toString(keys) + " " + e);
			return null;
		}
	}

	public Map<String, Long> getMultiPaiallel(final String[] keys, final String[] hashKeys) {
		try {
			// split by hash
			final Map<JedisPort, List<String>> splitMap = new HashMap<JedisPort, List<String>>();
			for (int i = 0; i < keys.length; i++) {
				String key = keys[i];
				String hashKey = hashKeys[i];
				final int num = getHashNum(hashKey);
				String redisKey = getRedisKey(key);

				for (final JedisPort server : servers) {
					if (server.contains(num)) {
						List<String> keyList = splitMap.get(server);
						if (keyList == null) {
							keyList = new ArrayList<String>();
							splitMap.put(server, keyList);
						}
						keyList.add(redisKey);
					}
				}
			}

			// parse result to long and combine
			final Map<String, Long> result = new HashMap<String, Long>();
			final Map<JedisPort, Map<String, String>> rets = CountDownLatchHelper.redisMget2(splitMap);

			for (final JedisPort server : splitMap.keySet()) {
				final Map<String, String> ret = rets.get(server);
				if (ret == null) {
					continue;
				}
				for (final String key : ret.keySet()) {
					final String value = ret.get(key);
					if (value == null) {
						continue;
					}
					result.put(key, Long.parseLong(value));
				}
			}

			return result;
		} catch (Exception e) {
			ApiLogger.warn("getMulti " + Arrays.toString(keys) + " " + e);
			return null;
		}
	}

	@Override
	public boolean set(final String key, final byte[] value) {
		StatLog.inc(setMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					return server.set(CommonUtil.encode(redisKey), value);
				}
			}
			return false;
		} catch (Exception e) {
			ApiLogger.warn("set " + key + " to: " + value + " " + e);
			return false;
		}
	}
	
	// add log by liuzhao
	@Override
	public boolean set(final String key, final String value) {
		StatLog.inc(setMethod);
		ApiLogger.warn("Redis set,key:"+key+",value:"+value);
		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);
			ApiLogger.warn("Redis set,redisKey:"+redisKey+",num:"+num+",servers:"+servers);
			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					boolean ret = server.set(redisKey, value);
					ApiLogger.warn("Redis set,server:"+server+",num:"+num+",ret:"+ret);
					return ret;
				}
			}
			ApiLogger.warn("Redis set,servers is not found!");
			return false;
		} catch (Exception e) {
			ApiLogger.warn("set " + key + " to: " + value + " " + e);
			return false;
		}
	}

	@Override
	public boolean delete(final String key) throws JedisException {
		StatLog.inc(delMethod);

		try {

			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					return server.del(redisKey);
				}
			}
			return false;
		} catch (Exception e) {
			ApiLogger.warn("delete " + key + " " + e);
			return false;
		}
	}

	@Override
	public boolean hdelete(String key, String hashKey) {
		// TODO
		return false;
	}

	@Override
	public Map<String, String> hgetall(final String key) {
		StatLog.inc(getMethod);

		Map<String, String> value = null;
		try {

			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					value = server.hgetAll(redisKey);
				}
			}
			return value;
		} catch (Exception e) {
			ApiLogger.warn("delete " + key + " " + e);
			return null;
		}
	}
	
	
	public Long lpush(final String key, final String value) {
		StatLog.inc(setMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					return server.lpush(redisKey, value);
				}
			}
			return -1l;
		} catch (Exception e) {
			ApiLogger.warn("lpush: key=" + key + ", value=: " + value + " " , e);
			return -1l;
		}
	}

	public Long rpush(final String key, final String value){
		StatLog.inc(setMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					return server.rpush(redisKey, value);
				}
			}
			return -1l;
		} catch (Exception e) {
			ApiLogger.warn("rpush: key=" + key + ", value=: " + value + " " , e);
			return -1l;
		}
	}
	
	public Long lpush(final String key, final byte[] value) {
		StatLog.inc(setMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					return server.lpush(CodecHandler.encode(redisKey), value);
				}
			}
			return -1l;
		} catch (Exception e) {
			ApiLogger.warn("rpush: key=" + key + ", value=: " + value + " ", e);
			return -1l;
		}
	}
	
	public Long rpush(final String key, final byte[] value){
		StatLog.inc(setMethod);
		
		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);
			
			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					return server.rpush(CodecHandler.encode(redisKey), value);
				}
			}
			return -1l;
		} catch (Exception e) {
			ApiLogger.warn("rpush: key=" + key + ", value=: " + value + " " , e);
			return -1l;
		}
	}
	
	@Override
	public long lrem(final String key, int count, String value) {
		StatLog.inc(setMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					return server.lrem(redisKey, count, value);
				}
			}
			return -1l;
		} catch (Exception e) {
			ApiLogger.warn("rpush: key=" + key + ", value=: " + value + " ", e);
			return -1l;
		}
	}

	public List<byte[]> lrange(final String key, final int start, final int end) {
		StatLog.inc(setMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					return server.lrange(CodecHandler.encode(redisKey), start, end);
				}
			}
			return null;
		} catch (Exception e) {
			ApiLogger.warn("lrange: key=" + key + ", start=: " + start + ", end= " + end, e);
			return null;
		}
	}
	

	@Override
	public long hlen(String key) {
		StatLog.inc(getMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					return server.hlen(redisKey);
				}
			}
			return 0l;
		} catch (Exception e) {
			ApiLogger.warn("hlen" + key + e);
			return 0l;
		}
	}
	
	public Long incr(final String key) throws JedisException {
		StatLog.inc(incMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					Long result = server.incr(redisKey);

					return result;
				}
			}
			return null;
		} catch (Exception e) {
			ApiLogger.warn("incr " + key + " " + e);
			return null;
		}
	}

	public Long decr(final String key) throws JedisException {
		StatLog.inc(decMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					Long result = server.decr(redisKey).longValue();

					return result;
				}
			}
			return null;
		} catch (Exception e) {
			ApiLogger.warn("decr " + key + " " + e);
			return null;
		}
	}
	
	// add log by liuzhao
	@Override
	public Long getLong(final String key) {
		StatLog.inc(getMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);
			ApiLogger.warn("Redis getLong key:"+key+";redisKey:"+redisKey+";num:"+num+";servers:"+servers);
			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					final String value = server.get(redisKey);
					ApiLogger.warn("Redis getLong redisKey:"+redisKey+";value:"+value+";server:"+server);
					if (value != null) {
						return Long.parseLong(value);
					} else {
						return null;
					}
				}
			}
			ApiLogger.warn("Redis getLong key:"+key+" we can't find the rediskey from servers!");
			return null;
		} catch (Exception e) {
			ApiLogger.warn("get " + key + " " + e);
			return null;
		}
	}
	
	@Override
	public int llen(final String key) {
		StatLog.inc(getMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			Long value = null;
			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					value = server.llen(redisKey);
				}
			}
			return value == null ? 0 : value.intValue();
		} catch (Exception e) {
			ApiLogger.warn("get " + key + " " + e);
			return 0;
		}
	}
	
	@Override
	public boolean ltrim(String key, int start, int stop) {
		StatLog.inc(getMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					return server.ltrim(redisKey, start, stop);
				}
			}
			return false;
		} catch (Exception e) {
			ApiLogger.warn("ltrim " + key + " " + e);
			return false;
		}
	}
	
	@Override
	public Long zadd(final String key, final double score, final String member) {
		StatLog.inc(setMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					Long value = server.zadd(CodecHandler.encode(redisKey), score, CodecHandler.encode(member));
					return value == null ? -1 : value;
				}
			}
			return -1l;
		} catch (Exception e) {
			ApiLogger.warn("zadd, key: " + key + ",score:  " + score + ",member: " + member, e);
			return -1l;
		}
	}
	
	@Override
	public Long zrem(final String key, final String member) {
		StatLog.inc(setMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					Long  value =  server.zrem(CodecHandler.encode(redisKey), CodecHandler.encode(member));
					return value == null ? -1 : value;
				}
			}
			return -1l;
		} catch (Exception e) {
			ApiLogger.warn("zrem, key: " + key + ",member: " + member, e);
			return -1l;
		}
	}
	
	@Override
	public Long zremrangebyrank(final String key, int startIndex, int stopIndex) {
		StatLog.inc(setMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					Long value =  server.zremrangeByRank(CodecHandler.encode(redisKey), startIndex, stopIndex);
					return value == null ? -1 : value;
				}
			}
			return -1l;
		} catch (Exception e) {
			ApiLogger.warn("zremrangebyrank, key: " + key + ",startIndex: " + startIndex + ", stopIndex:" + stopIndex, e);
			return -1l;
		}
	}
	
	@Override
	public Long zcard(final String key) {
		StatLog.inc(setMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					Long value =  server.zcard(CodecHandler.encode(redisKey));
					return value == null ? -1 : value;
				}
			}
			return -1l;
		} catch (Exception e) {
			ApiLogger.warn("zcard, key: " + key, e);
			return -1l;
		}
	}
	
	@Override
	public Set<Tuple> zrangeWithScores(final String key, int startIndex, int stopIndex) {
		StatLog.inc(setMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					return server.zrangeWithScores(CodecHandler.encode(redisKey), startIndex, stopIndex);
				}
			}
			return null;
		} catch (Exception e) {
			ApiLogger.warn("zrangeWithScores, key: " + key + ",startIndex:" + startIndex + ", stopIndex:" + stopIndex, e);
			return null;
		}
	}
	
	@Override
	public Set<Tuple> zrevrangeWithScores(final String key, int startIndex, int stopIndex) {
		StatLog.inc(setMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					return server.zrevrangeWithScores(CodecHandler.encode(redisKey), startIndex, stopIndex);
				}
			}
			return null;
		} catch (Exception e) {
			ApiLogger.warn("zrevrangeWithScores, key: " + key + ",startIndex:" + startIndex + ", stopIndex:" + stopIndex, e);
			return null;
		}
	}
	
	@Override
	public Long zrank(final String key, final String member) {
		StatLog.inc(setMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					Long value =  server.zrank(CodecHandler.encode(redisKey), CodecHandler.encode(member));
					return value == null ? -1 : value;
				}
			}
			return -1L;
		} catch (Exception e) {
			ApiLogger.warn("zrank, key: " + key + ", member: " + member, e);
			return -1L;
		}
	}
	
	@Override
	public Long zrevrank(final String key, final String member) {
		StatLog.inc(setMethod);

		try {
			final int num = getHashNum(key);
			String redisKey = getRedisKey(key);

			for (final JedisPort server : servers) {
				if (server.contains(num)) {
					Long value = server.zrevrank(CodecHandler.encode(redisKey), CodecHandler.encode(member));
					return value == null ? -1 : value;
				}
			}
			return -1L;
		} catch (Exception e) {
			ApiLogger.warn("zrevrange, key: " + key, e);
			return -1L;
		}
	}

	public int getHashNum(final String key) {
		long mod = 0;

		switch (hashAlg) {
		case NONE:
			mod = Long.parseLong(key) % consistNum;
			break;
		case CRC32:
			final CRC32 crc32 = crc32Local.get();
			crc32.reset();
			crc32.update(key.getBytes());
			mod = crc32.getValue() % consistNum;
			break;
		case MD5:
			throw new IllegalArgumentException("hash alg not supported: " + hashAlg);
		case SHA1:
			throw new IllegalArgumentException("hash alg not supported: " + hashAlg);
		default:
			throw new IllegalArgumentException("hash alg error: " + hashAlg);
		}

		return (int) mod;
	}
	
	public static void main(String[] args) {
		long mod = 0;

		int consistNum = 1024;

//		String key = "42197-conv-25935.g";
		String key = "141027-root.g";
// 141027-root
		CRC32 crc32 = new CRC32();

		crc32.reset();
		crc32.update(key.getBytes());
		mod = crc32.getValue() % consistNum;

		System.out.println(mod);
	}

	public static enum HASHALGS {
		NONE, CRC32, MD5, SHA1
	}

	private int consistNum = 1024;
	private List<JedisPort> servers;
	private String hashAlgStr;
	private String suffix = "";
	private HASHALGS hashAlg = HASHALGS.NONE;

	private final ThreadLocal<CRC32> crc32Local = new ThreadLocal<CRC32>() {
		@Override
		protected synchronized CRC32 initialValue() {
			return new CRC32();
		}
	};

	// /////////////////

	public void setSuffix(String suffix) {
		this.suffix = suffix;
	}

	private String getRedisKey(String key) {
		return key + suffix;
	}

	public void setConsistNum(final int num) {
		consistNum = num;
	}

	public int getConsistNum() {
		return consistNum;
	}

	public void setJedisPort(final List<JedisPort> servers) {
		this.servers = servers;
	}

	public List<JedisPort> getJedisPort() {
		return servers;
	}

	public void setHashAlg(final String hashAlg) {
		if (hashAlg == null || hashAlg.length() <= 0) {
			return;
		}
		hashAlgStr = hashAlg;
		if (hashAlg.equalsIgnoreCase("crc32")) {
			this.hashAlg = HASHALGS.CRC32;
		} else if (hashAlg.equalsIgnoreCase("md5")) {
			this.hashAlg = HASHALGS.MD5;
		} else if (hashAlg.equalsIgnoreCase("sha1")) {
			this.hashAlg = HASHALGS.SHA1;
		} else {
			this.hashAlg = HASHALGS.NONE;
		}
	}

	public String getHashAlg() {
		return hashAlgStr;
	}

	public void setCounterInfo(String counterInfo) {
		getMethod = counterInfo + getMethod;
		getmultiMethod = counterInfo + getmultiMethod;
		setMethod = counterInfo + setMethod;
		delMethod = counterInfo + delMethod;
		incMethod = counterInfo + incMethod;
		decMethod = counterInfo + decMethod;
	}

	private String getMethod = ".get";
	private String getmultiMethod = ".getmulti";
	private String setMethod = ".set";
	private String delMethod = ".del";
	private String incMethod = ".inc";
	private String decMethod = ".dec";

}
