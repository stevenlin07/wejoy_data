package com.weibo.wejoy.data.model;

import java.util.Date;

import cn.sina.api.commons.util.ApiLogger;

import com.google.protobuf.InvalidProtocolBufferException;

public class MessagePBUtil {

	public static byte[] toPB(Message msg) {
		return toPbObject(msg).build().toByteArray();
	}

	public static MessageWrap.Message.Builder toPbObject(Message msg) {
		MessageWrap.Message.Builder b = MessageWrap.Message.newBuilder();
		b.setMsgid(msg.msgid);
		b.setFromuid(msg.fromuid);
		b.setTouid(msg.touid);
		b.setType(msg.type);
		b.setContent(new String(msg.content));
		
		if (msg.ctime != null)
			b.setCtime(msg.ctime.getTime());

		return b;
	}

	public static Message parseFromPB(byte[] bytes) {
		if (bytes != null) {
			try {
				MessageWrap.Message pb = MessageWrap.Message.parseFrom(bytes);
				return toObject(pb);
			} catch (InvalidProtocolBufferException e) {
				ApiLogger.warn("unexpected protobuf msg: ", e);
			}
		}

		return null;
	}

	public static Message toObject(MessageWrap.Message pb) {
		if (pb != null) {
			Message msg = new Message();

			msg.msgid = pb.getMsgid();
			msg.fromuid = pb.getFromuid();
			msg.touid = pb.getTouid();
			msg.type = pb.getType();
			msg.content = pb.getContent().getBytes();

			if (pb.hasCtime())
				msg.ctime = new Date(pb.getCtime());

			return msg;
		}
		return null;
	}
}
