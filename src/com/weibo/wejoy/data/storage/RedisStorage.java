package com.weibo.wejoy.data.storage;

import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Tuple;

public interface RedisStorage {

	boolean isAlive();

	boolean set(String key, String value);
	
	boolean set(String key, byte[] value);

	String getString(String key);

	Map<String, byte[]> getMulti(String[] keys);

	boolean hset(final String key, String hashKey, String value);

	Long hget(final String key, String hashKey);

	Map<String, String> hgetall(final String key);

	long hlen(String key);

	Map<String, Long> getMulti(String[] keys, String[] hashKeys);

	boolean delete(String key);

	boolean hdelete(String key, String hashKey);
	
	
	public Long lpush(final String key, final String value);
	
	public Long lpush(final String key, final byte[] value);

	public Long rpush(final String key, final String value);
	
	public Long rpush(final String key, final byte[] value);
	
	public long lrem(final String key, int count, String value);

	public List<byte[]> lrange(final String key, final int start, final int end);
	
	public Long decr(final String key);

	public Long incr(final String key);
	
	public Long getLong(final String key);
	
	public int llen(String key);
	
	public boolean ltrim(String key, int start, int stop);
	

	public Long zadd(final String key, final double score, final String member);

	public Long zrem(final String key, final String member);

	public Long zremrangebyrank(final String key, int startIndex, int stopIndex);

	public Long zcard(final String key);

	public Set<Tuple> zrangeWithScores(final String key, int startIndex, int stopIndex);

	public Set<Tuple> zrevrangeWithScores(final String key, int startIndex, int stopIndex);

	public Long zrank(final String key, final String member);

	public Long zrevrank(final String key, final String member);
}
