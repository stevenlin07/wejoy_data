package com.weibo.wejoy.data.model;

import java.util.Date;

public class Message {

	public long msgid; // 消息id
	public long fromuid; // 消息发送者uid
	public long touid; // 消息接受者uid

	public int type; // 消息类型，例如文本，图片，音频，视频etc
	public byte[] content; // 文本时存内容，多媒体时存文件索引id
	public Date ctime; // 创建时间
}
