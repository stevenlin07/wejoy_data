package com.weibo.wejoy.data.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import cn.sina.api.commons.util.ApiLogger;

import com.weibo.wesync.data.FolderChange;

public class MeyouDataUtil {

	public static boolean decodeFolderChangeAdd(String isAdd) {
		return "1".equals(isAdd) ? true : false;
	}

	public static String encodeFolderChangeAdd(boolean isAdd) {
		return isAdd ? "1" : "0";
	}
	
	public static byte[] encodeFolderChange(FolderChange change) {
		if (change == null) return null;

		try {
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			ObjectOutput out = new ObjectOutputStream(bout);

			writeSafeUtf8(out, change.childId);
			out.writeBoolean(change.isAdd);
			out.flush();
			return bout.toByteArray();
		} catch (IOException e) {
			ApiLogger.error("encodeFolderChange error, ", e);
			return null;
		}
	}
	
	
	public static FolderChange decodeFolderChange(byte[] change) {
		if (change == null) return null;

		try {
			ByteArrayInputStream bait = new ByteArrayInputStream(change);
			ObjectInput oi = new ObjectInputStream(bait);

			FolderChange folderChange = new FolderChange(readSafeUtf8(oi), oi.readBoolean());
			
			return folderChange;
		} catch (IOException e) {
			ApiLogger.error("decodeFolderChange error, ", e);
			return null;
		}
	}
	

	public static void writeSafeUtf8(ObjectOutput out, String s) throws IOException {
		if (s == null) {
			out.writeShort(-1);
		} else {
			byte[] bb = s.getBytes("utf-8");
			out.writeShort(bb.length);
			out.write(bb);
		}
	}

	public static String readSafeUtf8(ObjectInput in) throws IOException {
		int len = in.readShort();
		if (len < 0)
			return null;

		byte[] bb = new byte[len];
		in.read(bb);
		return new String(bb, "utf-8");
	}


}
