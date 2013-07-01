package com.weibo.wejoy.data.util;

import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

public class CollectUtil {

	public static <T> SortedSet<T> emptySortedSet() {
		return new ConcurrentSkipListSet<T>();
	}
}
