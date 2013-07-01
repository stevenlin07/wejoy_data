package com.weibo.wejoy.data.model;

import cn.sina.api.commons.util.ApiLogger;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class MetaMessagePBUtil {

	public static byte[] toPB(MetaMessagePB msg) {
		return toPbObject(msg).build().toByteArray();
	}

	public static MetaMessagePBWrap.MetaMessage.Builder toPbObject(MetaMessagePB msg) {
		MetaMessagePBWrap.MetaMessage.Builder b = MetaMessagePBWrap.MetaMessage.newBuilder();

		b.setId(msg.id);
		b.setMeta(ByteString.copyFrom(msg.meta));
		b.setType(msg.type);

		return b;
	}

	public static MetaMessagePB parseFromPB(byte[] bytes) {
		if (bytes != null) {
			try {
				MetaMessagePBWrap.MetaMessage pb = MetaMessagePBWrap.MetaMessage.parseFrom(bytes);
				return toObject(pb);
			} catch (InvalidProtocolBufferException e) {
				ApiLogger.warn("unexpected protobuf msg: ", e);
			}
		}

		return null;
	}

	public static MetaMessagePB toObject(MetaMessagePBWrap.MetaMessage pb) {
		if (pb != null) {
			MetaMessagePB msg = new MetaMessagePB();

			msg.id = pb.getId();
			msg.meta = pb.getMeta().toByteArray();
			msg.type = pb.getType();

			return msg;
		}
		return null;
	}
}
