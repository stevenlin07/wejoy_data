package com.weibo.wejoy.wesync.service;

import com.google.inject.AbstractModule;
import com.weibo.wesync.DataService;
import com.weibo.wesync.DataServiceImpl;
import com.weibo.wesync.FakeGroupMessageService;
import com.weibo.wesync.FakeNoticeService;
import com.weibo.wesync.FakePrivacyService;
import com.weibo.wesync.GroupMessageService;
import com.weibo.wesync.NoticeService;
import com.weibo.wesync.PrivacyService;
import com.weibo.wesync.WeSyncService;
import com.weibo.wesync.WeSyncServiceImpl;
import com.weibo.wesync.data.FakeDataStore;

public class FakeWeSyncModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(DataService.class).toInstance(	new DataServiceImpl(new FakeDataStore()));
		bind(NoticeService.class).toInstance(new FakeNoticeService());
		bind(GroupMessageService.class).to(FakeGroupMessageService.class);
		bind(PrivacyService.class).to(FakePrivacyService.class);
		bind(WeSyncService.class).to(WeSyncServiceImpl.class);
	}

}
