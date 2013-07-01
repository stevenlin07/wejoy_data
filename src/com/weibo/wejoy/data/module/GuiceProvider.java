package com.weibo.wejoy.data.module;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

public class GuiceProvider<T> {

	private static Injector injector;

	static {
		injector = Guice.createInjector(new DbModule(), new McModule(), new McqModule(), new RedisModule(), new ServiceModule());
	}

	public static Injector getInjector() {
		return injector;

	}

	public static <T> T getInstance(Class<T> clazz) {
		return injector.getInstance(clazz);
	}
	
	public static <T> T getInstance(Class<T> clazz, String key) {
		return injector.getInstance(Key.get(clazz, Names.named(key)));
	}
}
