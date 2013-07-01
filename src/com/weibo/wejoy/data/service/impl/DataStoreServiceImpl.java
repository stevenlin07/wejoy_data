package com.weibo.wejoy.data.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

import redis.clients.jedis.Tuple;
import cn.sina.api.commons.util.ApiLogger;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.weibo.wejoy.data.constant.DataConstants;
import com.weibo.wejoy.data.dao.MessageDao;
import com.weibo.wejoy.data.model.MetaMessagePB;
import com.weibo.wejoy.data.processor.McqWriter;
import com.weibo.wejoy.data.service.DataStoreService;
import com.weibo.wejoy.data.service.FileStoreService;
import com.weibo.wejoy.data.storage.MemCacheStorage;
import com.weibo.wejoy.data.storage.RedisStorage;
import com.weibo.wejoy.data.util.CollectUtil;
import com.weibo.wejoy.data.util.CommonUtil;
import com.weibo.wejoy.data.util.SwitchControll;
import com.weibo.wesync.data.FolderChange;
import com.weibo.wesync.data.FolderChild;
import com.weibo.wesync.data.FolderID;
import com.weibo.wesync.data.Group;
import com.weibo.wesync.data.MetaMessageType;
import com.weibo.wesync.data.WeSyncMessage.FileData;
import com.weibo.wesync.data.WeSyncMessage.Meta;
import com.weibo.wesync.data.WeSyncMessage.Unread;

public class DataStoreServiceImpl implements DataStoreService {

	private static double DEFAULT_SCORE = 0d;

	@Inject
	public DataStoreServiceImpl(@Named("folderId") RedisStorage folderIdStore,
			@Named("folderChange") RedisStorage changeStore,
			@Named("folderChild") RedisStorage childStore,
			@SuppressWarnings("rawtypes") MemCacheStorage msgStore,
			MessageDao messageDao, FileStoreService fileStore,
			McqWriter mcqWriter) {
		this.folderIdStore = folderIdStore;
		this.folderChange = changeStore;
		this.childStore = childStore;
		this.msgStore = msgStore;
		this.messageDao = messageDao;
		this.fileStore = fileStore;
		this.mcqWriter = mcqWriter;
//		this.offlineNoticeCounter = offlineNoticeBlackList;//off-line notice counter and offlineNoticeBlackList use identical redis
	}

	// liuzhao add log and fix logic bug
	@SuppressWarnings("unchecked")
	@Override
	public Meta getMetaMessage(String metaMsgId) {
		ApiLogger.warn("getMetaMessage,metaMsgId = " + metaMsgId);
		long start = System.currentTimeMillis();
		Meta msg = null;
		try {
			byte[] metaMsg = (byte[]) msgStore.get(metaMsgId);
			if (null == metaMsg) { // 如果没有在MC命中，就从DB中获取
				ApiLogger.warn("[MC GET FAIL],getMetaMessage,metaMsgId = "
						+ metaMsgId);
				metaMsg = messageDao.getMetaMessage(metaMsgId);
				if (null == metaMsg) { // 如果DB中也没有获取成功
					ApiLogger.error("[DB GET FAIL],getMetaMessage,metaMsgId = "
							+ metaMsgId);
				} else { // 如果DB命中成功，就存入MC，存入成功
					boolean flag = msgStore.add(metaMsgId, metaMsg);
					ApiLogger.warn("[DB GET SUCC],getMetaMessage,metaMsgId = "
							+ metaMsgId);
					if (flag) {
						ApiLogger
								.warn("[MC SET SUCC],getMetaMessage,metaMsgId = "
										+ metaMsgId);
					} else {
						ApiLogger
								.warn("[MC SET FAIL],getMetaMessage,metaMsgId = "
										+ metaMsgId);
					}
				}
			}
			msg = Meta.parseFrom(metaMsg);
		} catch (Exception e) {
			ApiLogger
					.error("getMetaMessage error, metaMsgId = " + metaMsgId, e);
		}
		long end = System.currentTimeMillis();
		if (end - start > DataConstants.OP_TIMEOUT_L) {
			ApiLogger.warn(new StringBuilder(256).append(
					getClass().getName() + " get , cost time=").append(
					end - start));
		}
		return msg;
	}

	/**
	 * 存储消息meta
	 * 
	 * <pre>
	 * 
	 * 说明：
	 * (1) 文本消息和小文件，meta即是消息本身
	 * (2) 对大文件，meta是消息的元信息，文件流会单独上传，对应dataMessage
	 * 
	 * 流程：
	 * (1) 消息落mcq
	 * (2) 写mc
	 * 由processor程序處理消息落地
	 * </pre>
	 */
	@Override
	public boolean addMetaMessage(Meta msg) {
		if (null == msg) {
			ApiLogger.warn("addMetaMessage, msg = null");
			return false;
		}
		// 1 write to mcq
		MetaMessagePB metaMessagePB = new MetaMessagePB();
		metaMessagePB.id = msg.getId();
		metaMessagePB.meta = msg.toByteArray();
		ApiLogger.warn("addMetaMessage, msg.id = " + msg.getId());
		
		// @by jichao, TODO, 测试环境mcq一直不通，暂时干掉
		boolean res1 = true;//mcqWriter.addMetaMessage(metaMessagePB);
		if (!res1) { // 如果将meta写入到mcq失败
			ApiLogger.warn("[MCQ SET FAIL], addMetaMessage, msg = "
					+ msg.getId());
			return false;
		} else {
			ApiLogger.warn("[MCQ SET SUCC], addMetaMessage, msg = "
					+ msg.getId());
		}
		// 2 wrte mc
		@SuppressWarnings("unchecked")
		boolean res2 = msgStore.set(msg.getId(), msg.toByteArray());
		if (!res2) { // 如果将meta写入到mc失败
			ApiLogger.warn("[MC SET FAIL], addMetaMessage, msg = "
					+ msg.getId());
			return false;
		} else {
			ApiLogger.warn("[MC SET SUCC], addMetaMessage, msg = "
					+ msg.getId());
		}
		return res2;
	}

	@Override
	public boolean removeMetaMessage(Meta msg) {
		// TODO
		return true;
	}

	@Override
	public FileData getFileByIndex(FileData fileIndex) {
		ApiLogger.warn("getFileByIndex,fileIndex = " + fileIndex);
		if (fileIndex == null)
			return null;

		FileData fileData = null;
		try {
			// TODO getFileByIndex
			fileData = FileData.parseFrom(fileStore.getFileByIndex(fileIndex));
		} catch (Exception e) {
			ApiLogger.error(
					"getFileByIndex error, fileId = " + fileIndex.getId(), e);
		}
		ApiLogger
				.warn("getFileByIndex,get fileIndex = " + fileIndex + " succ!");
		return fileData;
	}

	@Override
	public FileData getFileById(String fileId) {
		ApiLogger.warn("getFileById,fileId = " + fileId);
		FileData fileData = FileData.newBuilder().setId(fileId).build();

		try {
			// 覆盖掉老的文件
			fileData = FileData.parseFrom(fileStore.getFileByIndex(fileData));
		} catch (Exception e) {
			ApiLogger.error("getFileById error, fileId = " + fileId, e);
		}
		ApiLogger.warn("getFileById,fileId = " + fileId + " succ!");
		return fileData;
	}

	@Override
	public FileData storeFile(FileData fileData) {
		FileData retFileData = null;
		try {
			// TODO ApiLogger.warn("storeFile,fileId = "+fileId+" succ!"); 日志打进去
			retFileData = fileStore.saveFile(fileData);
		} catch (Exception e) {
			ApiLogger.error("storeFile error, fileId = " + fileData.getId(), e);
		}
		return retFileData;
	}

	// add log by liuzhao
	// 创建folder
	@Override
	public boolean createFolder(String folderId) {
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_META_SUFFIX);
		Long value = folderIdStore.getLong(key);
		if (value == null) {
			ApiLogger.warn("[REDIS GET FAIL],createFolder,folderId = "
					+ folderId + ",key = " + key + ",value = " + value
					+ ",[NEED CREAT]");
			boolean ret = folderIdStore.set(key, "0");
			if (ret) {
				ApiLogger.warn("[REDIS SET SUCC],createFolder new folderId="
						+ folderId);
			} else {
				ApiLogger.warn("[REDIS SET FAIL],createFolder new folderId="
						+ folderId);
			}
			return ret;
		} else {
			ApiLogger.warn("[REDIS GET SUCC],createFolder,folderId = "
					+ folderId + ",key = " + key + ",value = " + value);
		}
		return true;
	}

	@Override
	public int getMaxChildId(String folderId) {
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_META_SUFFIX);
		Long value = folderIdStore.getLong(key);
		if(null == value){
			ApiLogger.warn("[REDIS GET FAIL],getMaxChildId folderId="
					+ folderId + ",key = "+key+",value = null");
			return -1;
		}else{
			ApiLogger.warn("[REDIS GET SUCC],getMaxChildId folderId="
					+ folderId + ",key = "+key+",value = "+value);
			return value.intValue();
		}
	}

	private boolean resetMaxChildId(String folderId) {
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_META_SUFFIX);
		return folderIdStore.set(key, "-1");
	}

	@Override
	public Map<String, Integer> getMaxChildIdMulti(String[] folderIds) {
		String[] keys = new String[folderIds.length];
		for (int i = 0; i < keys.length; i++) {
			keys[i] = getRedisStorageKey(folderIds[i],
					DataConstants.FOLDER_META_SUFFIX);
		}

		Map<String, Integer> retMap = new HashMap<String, Integer>();

		Map<String, byte[]> valueMap = folderIdStore.getMulti(keys);
		for (int i = 0; i < keys.length; i++) {
			String key = keys[i];

			byte[] bValue = valueMap.get(key);
			if (bValue != null) {
				try {
					String strValue = CommonUtil.decode(bValue,
							DataConstants.DEFAULT_CHARSET);
					Integer iValue = Integer.parseInt(strValue);
					retMap.put(key, iValue);
				} catch (Exception e) {
					ApiLogger
							.error(String
									.format("%s.getMaxChildIdMulti() -> parse bytes:%s to Integer exception",
											getClass().getName(),
											Arrays.toString(bValue)), e);
				}
			}
		}

		return retMap;
	}

	@Override
	public long reserveChildId(String folderId) {
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_META_SUFFIX);

		Long maxChildId = folderIdStore.incr(key);

		maxChildId = (maxChildId == null) ? 0l : maxChildId;

		return maxChildId;
	}

	/**
	 * Operation on changes in Folder start
	 */
	public SortedSet<FolderChange> getFollowingChanges(String folderId,
			FolderChange change, int n) {
		if (change == null)
			return CollectUtil.emptySortedSet();

		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_CHANGE_SUFFIX);

		Long indexOf = folderChange.zrank(key, change.toString());

		if (indexOf == -1 || indexOf < 0)
			return CollectUtil.emptySortedSet();

		int start = indexOf.intValue() + 1;
		int end = start + n;

		SortedSet<FolderChange> changes = getFolderChanges(folderId, start,
				end, false);

		return changes;
	}

	// add log by liuzhao
	@Override
	public List<Unread> numberOfFolderChanges(List<String> folderIds) {
		if (folderIds == null || folderIds.isEmpty())
			return Collections.emptyList();
		List<Unread> unreadList = new LinkedList<Unread>();
		Unread.Builder builder = Unread.newBuilder();
		ApiLogger.warn("numberOfFolderChanges getUnread");
		for (String folderId : folderIds) {
			try {
				builder.setFolderId(folderId);
				int num = numberOfFolderChanges(folderId);
				builder.setNum(num);
				unreadList.add(builder.build());
				ApiLogger.warn("Unread, folderId=" + folderId + ";num:" + num);
			} catch (Exception e) {
				ApiLogger
						.warn("numberOfFolderChanges getUnread foldeId error, folderId=+"
								+ folderId);
			}
		}
		return unreadList;
	}

	@Override
	public SortedSet<FolderChange> getFolderChanges(String folderId) {
		return getFolderChanges(folderId, 0, -1, false);
	}

	@Override
	public SortedSet<FolderChange> getFolderChanges(String folderId,
			int beginIndex, int endIndex) {
		return getFolderChanges(folderId, beginIndex, endIndex, false);
	}

	private SortedSet<FolderChange> getFolderChanges(String folderId,
			int beginIndex, int endIndex, boolean trim) {
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_CHANGE_SUFFIX);

		Set<Tuple> value = folderChange.zrangeWithScores(key, beginIndex,
				endIndex);

		if (value == null || value.isEmpty())
			return CollectUtil.emptySortedSet();

		List<FolderChange> tempChanges = new ArrayList<FolderChange>(
				value.size());
		for (Tuple tuple : value) {
			tempChanges.add(FolderChange.fromString(tuple.getElement()));
		}

		// FolderChange实现了Comparator接口，按childId排序
		Collections.sort(tempChanges);

		SortedSet<FolderChange> changes = new ConcurrentSkipListSet<FolderChange>();

		int len = tempChanges.size();
		for (int i = 0; i < len; i++) {
			changes.add(tempChanges.get(i));
		}

		return changes;
	}

	@Override
	public int numberOfFolderChanges(String folderId) {
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_CHANGE_SUFFIX);

		Long value = folderChange.zcard(key);
		ApiLogger.warn("[REDIS GET],numberOfFolderChanges,key="+key+",value="+value);
		return value == null ? 0 : value.intValue();
	}

	@Override
	public boolean addFolderChange(String folderId, FolderChange change) {
		ApiLogger.info("??addFolderChange, folderId:" + folderId);
		
		if ( null == change)
			return false;

		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_CHANGE_SUFFIX);

		if (SwitchControll.isTrimChange.get()) {
			Long capicity = folderChange.zcard(key);
			int len = (capicity != null) ? capicity.intValue() : 0;
			boolean needTrim = (len + 1) > DataConstants.FOLDER_CHANGE_LIMITED_SIZE ? true
					: false;
			// 假设一次rem失败，忽略，下次rem的时候可以把超限的value截断
			if (needTrim) {
				int needTrimSize = (len + 1)
						- DataConstants.FOLDER_CHANGE_LIMITED_SIZE;
				int start = 0;
				int end = needTrimSize - 1;
				Long value = folderChange.zremrangebyrank(key, start, end);
				if (value == null || value == 0) {
					ApiLogger.warn("trim change failed, key=" + key);
				}
			}
		}

		Long value = folderChange.zadd(key, DEFAULT_SCORE, change.toString());

		boolean ret = (value != null && value > 0) ? true : false;
		
		if (ret) {
			ApiLogger.warn("[REDIS SET SUCC],addFolderChange,folderId = " + folderId
					+ ",changeId=" + change.childId+",key="+key+",value="+value);
			
			try {
				Meta meta = convertFolderChangeToMeta(folderId, change);
				
				if(meta != null) {
					ApiLogger.debug(meta.getSpanId() +"|"+ meta.getSpanSequenceNo());
					if(meta.getSpanSequenceNo() == -1 || meta.getSpanSequenceNo() == 0) {
						updateUnReadNum(folderId, true, 1);
					}
				}
			} catch (Exception e) {
				ApiLogger.error(e.getMessage(), e);
			}
		}else{
			ApiLogger.warn("[REDIS SET FAIL],addFolderChange,folderId = " + folderId
					+ ",changeId=" + change.childId+",key="+key+",value="+value);
		}
		
		return ret;
	}
	
	public Meta convertFolderChangeToMeta(String folderId, FolderChange fc){
		//Folder type specific process
		String groupHistoryFolderId = null;
		switch( FolderID.getType(folderId) ){
		case Group:
			String groupId = FolderID.getGroup(folderId);
			groupHistoryFolderId = Group.historyFolderId(groupId);
			break;
		case Root:
			ApiLogger.warn(this.getClass().getName() + "; get meta message in convertFolderChangeToMeta; find root folder:" + 
				folderId + ", ignore it");
			return null;
		default:
			; //do nothing
		}
		
		String metaFolderId = FolderID.getType(folderId).equals(FolderID.Type.Group) ? groupHistoryFolderId : folderId;
		
		String msgId;
		try{
			msgId = FolderChild.generateId(metaFolderId, Long.valueOf(fc.childId));
		}catch(java.lang.NumberFormatException e){
			//Maybe some meta from other folder, e.g. group chat 
			msgId = fc.childId;
		}
		
		Meta msg;
		try {
			if (fc.isAdd) {
				ApiLogger.warn(this.getClass().getName() + "; get meta message in convertFolderChangeToMeta; msgId:" + msgId
						+ "; folderId:" + folderId);
				msg = this.getMetaMessage(msgId);
				ApiLogger.warn(this.getClass().getName() + "; get meta message in convertFolderChangeToMeta; msgId:" + msgId
						+ "; folderId:" + folderId + "; msg:" + msg.getType() + "," + msg.getFrom() + "," + msg.getTo() + ","
						+ msg.getTime() + 
						(MetaMessageType.valueOf(msg.getType().byteAt(0)).equals(MetaMessageType.audio) ?
							"" :
							"," + msg.getContent()));
			} else {
				// TODO empty meta means delete?
				msg = Meta.newBuilder().setId(msgId).build();
			}
		}catch(Exception e){
			ApiLogger.warn("get meta message in convertFolderChangeToMeta error", e);
			msg = Meta.newBuilder().setId(msgId).build();
		}
		
		return msg;
	}

	private String getOfflineNoticeCounterRedisKey(String folderId){
		return folderId + DataConstants.OFFLINE_NOTICE_COUNTER_SUFFIX;
	}
	
	private void updateUnReadNum(String folderId, boolean isIncr, int modifyNum){
		folderId = FolderID.getUsername(folderId);
		String key = getOfflineNoticeCounterRedisKey(folderId);
		
		ApiLogger.warn("NOT yest implementation for offline Notice Counter");
//		ApiLogger.warn(this.getClass().getName() + "; offlineNoticeCounter:" + offlineNoticeCounter);
		
//		try {
//			synchronized (folderId.intern()) {
//				Long i = offlineNoticeCounter.getLong(key);
//				
//				//not use incrby and decrby of redis cause our redis client does not offer these api
//				if(i == null || i < 0){
//					i = 0l;
//				}
//				if(isIncr){
//					i += modifyNum;
//				} else {
//					i -= modifyNum;
//				}
//				
//				offlineNoticeCounter.set(key, i.toString());
//			}
//		} catch (Exception e) {
//			ApiLogger.error(e.getMessage(), e);
//		}

	}

	/**
	 * 删除一条FolderChange
	 * 
	 */
	@Override
	public boolean removeFolderChange(String folderId, FolderChange change) {
		if (change == null)
			return false;

		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_CHANGE_SUFFIX);

		Long value = folderChange.zrem(key, change.toString());

		boolean ret = (value != null && value > 0) ? true : false;

		if (!ret) {
			ApiLogger.warn("removeFolderChange error, folderId = " + folderId
					+ ", changeId=" + change.childId);
		} else {
			try {
				Meta meta = convertFolderChangeToMeta(folderId, change);
				if(meta != null) {
					ApiLogger.debug(this.getClass().getName() + " removeFolderChange; meta:" + meta + ";" + 
							meta.getSpanId() +"|"+ meta.getSpanSequenceNo());
					if(meta.getSpanSequenceNo() == -1 || meta.getSpanSequenceNo() == 0) {
						updateUnReadNum(folderId, false, 1);
					}
				}
			} catch (Exception e) {
				ApiLogger.error(e.getMessage(), e);
			}
		}
		
		return ret;
	}
	
	private int getFolderChangeNum(String folderId, int startIndex, int stopIndex){
		int counter = 0;
		
		try {
			SortedSet<FolderChange> changes = getFolderChanges(folderId, startIndex, stopIndex);
			for (FolderChange fc : changes) {
				Meta meta = convertFolderChangeToMeta(folderId, fc);
				if(meta != null) {
					ApiLogger.debug(this.getClass().getName() + " getFolderChangeNum; meta:" + meta + ";" + 
							meta.getSpanId() +"|"+ meta.getSpanSequenceNo());
					if(meta.getSpanSequenceNo() == -1 || meta.getSpanSequenceNo() == 0) {
						++counter;
					}
				}
			}
			
		} catch (Exception e) {
			ApiLogger.error(e.getMessage(), e);
		}
		
		return counter;
	}
	
//	private int getFolderChangeNum(String folderId, int startIndex, int stopIndex){
//		int counter = 0;
//		
//		try {
//			String key = getRedisStorageKey(folderId, DataConstants.FOLDER_CHANGE_SUFFIX);
//			
//			Set<Tuple> fcSet = folderChange.zrangeWithScores(key, startIndex, stopIndex);
//			for (Tuple tuple : fcSet) {
//				FolderChange fc = FolderChange.fromString(tuple.getElement());
//				Meta meta = convertFolderChangeToMeta(folderId, fc);
//				System.out.println(this.getClass().getName() + " getFolderChangeNum; meta:" + meta);
//				if(meta != null) {
//					System.out.println(this.getClass().getName() + " getFolderChangeNum; meta:" + meta + ";" + 
//							meta.getSpanId() +"|"+ meta.getSpanSequenceNo());
//					if(meta.getSpanSequenceNo() == -1 || meta.getSpanSequenceNo() == 0) {
//						++counter;
//					}
//				}
//			}
//			
//			System.out.println(this.getClass().getName() + " getFolderChangeNum; counter:" + counter);
//		} catch (Exception e) {
//			ApiLogger.error(e.getMessage(), e);
//		}
//		
//		return counter;
//	}	
	
	
	@Override
	public boolean removeFolderChanges(String folderId, int num) {
		int counter = this.getFolderChangeNum(folderId, 0, num - 1);
		
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_CHANGE_SUFFIX);
		
		Long value = folderChange.zremrangebyrank(key, 0, num - 1);
		
		boolean ret = (value != null && value > 0) ? true : false;
		ApiLogger.warn("removeFolderChanges");
		if (!ret) {
			ApiLogger.warn("removeFolderChanges error, folderId = " + folderId
					+ ", num=" + num);
		} else {
			updateUnReadNum(folderId, false, counter);
		}

		return ret;
	}

	@Override
	public boolean removePrecedingFolderChange(String folderId,
			FolderChange change) {
		
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_CHANGE_SUFFIX);

		// 明确场景：change为null时，之际返回false
		if (change == null) {
			return false;
		}

		Long index = folderChange.zrank(key, change.toString());

		// 未找到change索引，直接返回
		if (index == -1 || index < 0) {
			return false;
		}

		int counter = this.getFolderChangeNum(folderId, 0, index.intValue());
		
		Long value = folderChange.zremrangebyrank(key, 0, index.intValue());

		boolean ret = (value != null && value > 0) ? true : false;

		if (!ret) {
			ApiLogger.warn("removePrecedingFolderChange error, folderId = "
					+ folderId + ", FolderChangeId=" + change.childId);
		} else {
			updateUnReadNum(folderId, false, counter);
		}

		return ret;
	}

	// add log by liuzhao 20130116
	@Override
	public SortedSet<FolderChange> removeAllFolderChanges(String folderId) {
		int counter = this.getFolderChangeNum(folderId, 0, -1);
		
		SortedSet<FolderChange> changes = getFolderChanges(folderId, 0, -1);
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_CHANGE_SUFFIX);
		Long value = folderChange.zremrangebyrank(key, 0, -1);
		boolean op = (value != null && value >= 0) ? true : false;
		// 增加日志，判断是否取到未读
		ApiLogger.warn("removeAllFolderChanges :folderId:" + folderId
				+ ";value:" + value + ";op:" + op);
		
		
		if(op){
			updateUnReadNum(folderId, false, counter);
		}
		
		return changes;
	}

	/** Operation on change in Folder end */

	/**
	 * Operation on children in Folder start
	 */
	@Override
	public SortedSet<FolderChild> getChildren(String folderId) {
		return getChildren(folderId, 0, -1, false);
	}

	// 逆序排列
	@Override
	public SortedSet<FolderChild> getChildren(String folderId, int beginIndex,
			int endIndex) {
		return getChildren(folderId, beginIndex, endIndex, false);
	}

	private SortedSet<FolderChild> getChildren(String folderId, int beginIndex,
			int endIndex, boolean trim) {
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_CHILDREN_SUFFIX);

		Set<Tuple> value = childStore.zrangeWithScores(key, beginIndex,
				endIndex);

		if (value == null || value.isEmpty())
			return CollectUtil.emptySortedSet();

		List<FolderChild> tempChild = new ArrayList<FolderChild>(value.size());
		for (Tuple tuple : value) {
			tempChild.add(new FolderChild(tuple.getElement(), (long) tuple
					.getScore()));
		}

		// FolderChild实现了Comparator接口，按score排序
		Collections.sort(tempChild);

		SortedSet<FolderChild> childs = new ConcurrentSkipListSet<FolderChild>();

		// 按消息自然顺序排列
		int len = tempChild.size();
		for (int i = 0; i < len; i++) {
			childs.add(tempChild.get(i));
		}

		return childs;
	}

	@Override
	public int numberOfChildren(String folderId) {
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_CHILDREN_SUFFIX);

		Long value = childStore.zcard(key);

		return value == null ? 0 : value.intValue();
	}

	// add log by liuzhao
	// 将文件夹加入到root文件夹下
	@Override
	public boolean addChild(String folderId, String child, long score) {
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_CHILDREN_SUFFIX);

		if (SwitchControll.isTrimChild.get()) {
			Long capicity = childStore.zcard(key);
			int len = (capicity != null) ? capicity.intValue() : 0;
			boolean needTrim = (len + 1) > DataConstants.FOLDER_CHILDRE_LIMITED_SIZE ? true
					: false;
			// 假设一次rem失败，忽略，下次rem的时候可以把超限的value截断
			if (needTrim) {
				int needTrimSize = (len + 1)
						- DataConstants.FOLDER_CHILDRE_LIMITED_SIZE;
				int start = 0;
				int end = needTrimSize - 1;
				Long value = childStore.zremrangebyrank(key, start, end);
				if (value == null || value == 0) {
					ApiLogger.warn("trim child failed, key=" + key);
				}
			}
		}

		Long value = childStore.zadd(key, (double) score, child);

		boolean ret = (value != null && value > 0) ? true : false;
		ApiLogger.warn("addChild, folderId=" + folderId + ";child:" + child
				+ ";score:" + score);
		ApiLogger.warn("addChild, key=" + key + ";value:" + value + ";ret:"
				+ ret);
		return ret;
	}

	@Override
	public boolean removeChild(String folderId, String child) {
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_CHILDREN_SUFFIX);

		Long value = childStore.zrem(key, child);

		boolean ret = (value != null && value > 0) ? true : false;

		if (!ret) {
			ApiLogger.warn("removeChild error, folderId = " + folderId
					+ ", child=" + child);
		}

		return ret;
	}

	@Override
	public boolean removeChildren(String folderId, int num) {
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_CHILDREN_SUFFIX);

		Long value = childStore.zremrangebyrank(key, 0, num - 1);

		boolean ret = (value != null && value > 0) ? true : false;

		if (!ret) {
			ApiLogger.warn("removeChildren error, folderId = " + folderId
					+ ", num=" + num);
		}

		return ret;
	}

	/**
	 * sorted set中默认以递增的顺序排列
	 * 
	 */
	public boolean removePrecedingChildren(String folderId, FolderChild child) {
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_CHILDREN_SUFFIX);

		// 明确场景：change为null时，之际返回false
		if (child == null) {
			return false;
		}

		Long index = childStore.zrank(key, child.id);

		// 未找到change索引，直接返回
		if (index == -1 || index < 0) {
			return false;
		}

		Long value = childStore.zremrangebyrank(key, 0, index.intValue());

		boolean ret = (value != null && value > 0) ? true : false;

		if (!ret) {
			ApiLogger.warn("removePrecedingChildren error, folderId = "
					+ folderId + ", child=" + child);
		}

		return ret;
	}

	@Override
	public boolean removeAllChildren(String folderId) {
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_CHILDREN_SUFFIX);

		Long value = childStore.zremrangebyrank(key, 0, -1);
		boolean ret = (value != null && value > 0) ? true : false;

		if (!ret) {
			ApiLogger.warn("removeAllChildren error, folderId = " + folderId);
		}

		return ret;
	}

	@Override
	public SortedSet<FolderChild> getFollowingChildren(String folderId,
			String childId, int n) {
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_CHILDREN_SUFFIX);

		Long indexOf = childStore.zrank(key, childId);

		// 正常情况下，indexOf不应小于0
		if (indexOf == -1 || indexOf < 0)
			return CollectUtil.emptySortedSet();

		int start = indexOf.intValue() + 1;
		int end = start + n;

		SortedSet<FolderChild> children = getChildren(folderId, start, end,
				false);

		return children;
	}

	@Override
	public SortedSet<FolderChild> getPrecedingChildren(String folderId,
			String childId, int n) {
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_CHILDREN_SUFFIX);

		Long indexOf = childStore.zrank(key, childId);

		if (indexOf == -1 || indexOf < 0)
			return CollectUtil.emptySortedSet();

		// end需>0
		int end = indexOf.intValue() - 1;
		if (end < 0) {
			return CollectUtil.emptySortedSet();
		}
		int start = end - n;
		if (start < 0) {
			start = 0;
		}

		SortedSet<FolderChild> children = getChildren(folderId, start, end,
				false);

		return children;
	}

	// 判断是否群成员
	@Override
	public boolean isChild(String childId, String folderId) {
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_CHILDREN_SUFFIX);

		Long indexOf = childStore.zrank(key, childId);

		if (indexOf == -1 || indexOf < 0)
			return false;

		return true;
	}

	@Override
	public boolean destroyFolder(String folderId) {
		removeAllFolderChangesWithoutChanges(folderId);

		removeAllChildren(folderId);

		resetMaxChildId(folderId);

		return true;
	}

	private boolean removeAllFolderChangesWithoutChanges(String folderId) {
		String key = getRedisStorageKey(folderId,
				DataConstants.FOLDER_CHANGE_SUFFIX);

		Long value = folderChange.zremrangebyrank(key, 0, -1);
		return (value != null && value > 0) ? true : false;
	}

	/** Operation on children in Folder end */

	private String getRedisStorageKey(String key, String suffix) {
		return key + suffix;
	}

	private final RedisStorage folderIdStore;
	private final RedisStorage childStore;
	@SuppressWarnings("rawtypes")
	private final MemCacheStorage msgStore;
	private final MessageDao messageDao;
	private final FileStoreService fileStore;
	private final McqWriter mcqWriter;

	private RedisStorage folderChange;
//	private RedisStorage offlineNoticeCounter;

	@Override
	public SortedSet<FolderChange> getPrecedingChanges(String folderId,
			FolderChange change, int n) {
		if (change == null)
			return CollectUtil.emptySortedSet();

		SortedSet<FolderChange> changeSet = getFolderChanges(folderId);
		return getPrecedingElements(changeSet, change, n);
	}
	
	private <T> SortedSet<T> getPrecedingElements(SortedSet<T> all, T element, int n){
		int flag = getIndexByElement( all, element );
		int end = flag-1;
		if( end < 0 ) return null;
		
		int begin = flag-n;
		if( begin < 0 ) begin = 0;
		return getElementsByRange(all, begin, end);
	}
	
	private <T> int getIndexByElement( SortedSet<T> set, T element){
		//Redis command: ZRANK
		int index = 0;
		//FIXME maybe not exist?
//		if( !set.contains(element) ) return -1;
		
		Iterator<T> iter = set.iterator();
		while( iter.hasNext() ){
			if( iter.next().equals(element) ) break;
			index++;
		}
		return index;
	}
	
	private <T> SortedSet<T> getElementsByRange(SortedSet<T> total, int beginIndex, int endIndex){
		//Redis command: ZRANGE
		T beginElement = getElementByIndex( total, beginIndex );
		T endElement = getElementByIndex( total, endIndex );
		
		SortedSet<T> newSet = new ConcurrentSkipListSet<T>();
		SortedSet<T> subSet = null;
		if( null == beginElement && null == endElement ) {
			return null;
		}else if ( null == beginElement ){
			subSet = total.headSet(endElement);
			newSet.add(endElement);
		}else if ( null == endElement ){
			subSet = total.tailSet(beginElement);
		}else{
			subSet = total.subSet(beginElement, endElement);
			newSet.add(endElement);
		}
		if (null != subSet) newSet.addAll(subSet);
		
		return newSet;
	}
	
	private <T> T getElementByIndex(SortedSet<T> set, int index ){
		//Emulate Redis operation, where the minus index means traverse the set reversely
		if( set.isEmpty() ) return null;
				
		index = (index < 0 ? set.size()+index : index);
		if( index < 0 ) return null;
		if( index >= set.size()  ) return null;
				
		Iterator<T> iter = set.iterator();
		while( index-- > 0 ){
			iter.next();
		}
		return iter.next();
	}

}
