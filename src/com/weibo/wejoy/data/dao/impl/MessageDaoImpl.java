package com.weibo.wejoy.data.dao.impl;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import cn.sina.api.data.Constants;
import cn.sina.api.data.dao.util.JdbcTemplate;

import com.weibo.wejoy.data.constant.DataConstants;
import com.weibo.wejoy.data.dao.MessageDao;

public class MessageDaoImpl implements MessageDao {
//	@Override
//	public boolean saveMessage(String metaMsgId, MetaMessage metaMsg) {
//		try {
//			long t1 = System.currentTimeMillis();
//			String sql = SAVE_META_MESSAGE_NONE_PB;
//
//			getJt(metaMsgId, DataConstants.DB_MESSAGE).update(generateSql(sql, metaMsgId, DataConstants.DB_MESSAGE),
//					new Object[] { metaMsg.id, metaMsg.from, metaMsg.to, metaMsg.type.toByte(), metaMsg.content, metaMsg.time });
//
//			long t2 = System.currentTimeMillis();
//			if (t2 - t1 > Constants.OP_DB_TIMEOUT) {
//				long t = t2 - t1;
//				ApiLogger.warn(new StringBuilder(64).append("MessageDaoImpl saveMessage(metaMsg) too slow, t=").append(t).append("id=")
//						.append(metaMsg.id));
//			}
//			return true;
//
//		} catch (RuntimeException e) {
//			//FIXME content是byte[],不打到日志，异常修复时从缓存取
//			ApiLogger.error("MessageDaoImpl saveMessage(metaMsg) error, sql=" + new StringBuilder(128).append("id=").append(metaMsg.id).append(", from=").append(metaMsg.from).append(", to=").append(metaMsg.to).append(", type=").append(metaMsg.type.toByte()).append(", time=").append(metaMsg.time));
//			
//			throw e;
//		}
//	}
	
	@Override
	public boolean saveMessage(String metaMsgId, byte[] metaMsg) {
		try {
			long t1 = System.currentTimeMillis();
			String sql = SAVE_META_MESSAGE;

			getJt(metaMsgId, DataConstants.DB_MESSAGE).update(generateSql(sql, metaMsgId, DataConstants.DB_MESSAGE),
					new Object[] { metaMsgId, metaMsg});

			long t2 = System.currentTimeMillis();
			if (t2 - t1 > Constants.OP_DB_TIMEOUT) {
				long t = t2 - t1;
//				ApiLogger.warn(new StringBuilder(64).append("MessageDaoImpl saveMessage(metaMsg) too slow, t=").append(t).append("id=")
//						.append(metaMsgId));
			}
			return true;

		} catch (RuntimeException e) {
			//FIXME 消息是byte[],不打到日志，异常修复时从缓存取 ？
//			ApiLogger.error("MessageDaoImpl saveMessage(metaMsg) error, sql=" + new StringBuilder(128).append("id=").append(metaMsgId));
			
			throw e;
		}
	}

//	@Override
//	public MetaMessage getMetaMessageNonePb(String metaMsgId) {
//		long t1 = System.currentTimeMillis();
//
//		MetaMessage metaMsg = (MetaMessage) getJt(metaMsgId, DataConstants.DB_MESSAGE).query(
//				generateSql(GET_META_MESSAGE_BY_ID_NONE_PB, metaMsgId, DataConstants.DB_MESSAGE), new String[]{metaMsgId}, new ResultSetExtractor() {
//					public MetaMessage extractData(ResultSet rs) throws SQLException, DataAccessException {
//						if (rs.next()) {
//							return getMetaMessage(rs);
//						}
//						return null;
//					}
//				});
//
//		long t2 = System.currentTimeMillis();
//		if (t2 - t1 > Constants.OP_DB_TIMEOUT) {
//			long t = t2 - t1;
//			ApiLogger.warn(new StringBuilder(64).append("MessageDaoImpl getMetaMessage too slow, t=").append(t).append(", id=")
//					.append(metaMsgId));
//		}
//
//		return metaMsg;
//	}
	
	@Override
	public byte[] getMetaMessage(String metaMsgId) {
		long t1 = System.currentTimeMillis();


		byte[] metaMsg = (byte[]) getJt(metaMsgId, DataConstants.DB_MESSAGE).query(
				generateSql(GET_META_MESSAGE_BY_ID, metaMsgId, DataConstants.DB_MESSAGE), new String[]{metaMsgId} , new ResultSetExtractor() {
					public byte[] extractData(ResultSet rs) throws SQLException, DataAccessException {
						if (rs.next()) {
							return rs.getBytes("meta");
						}
						return null;
					}
				});

		long t2 = System.currentTimeMillis();
		if (t2 - t1 > Constants.OP_DB_TIMEOUT) {
			long t = t2 - t1;
//			ApiLogger.warn(new StringBuilder(64).append("MessageDaoImpl getMetaMessage too slow, t=").append(t).append(", id=")
//					.append(metaMsgId));
		}

		return metaMsg;
	}

//	private MetaMessage getMetaMessage(ResultSet rs) throws SQLException {
//		if (rs == null)
//			return null;
//
//		MetaMessage metaMsg = new MetaMessage();
//		metaMsg.id = rs.getString("id");
//		metaMsg.from = rs.getString("from");
//		metaMsg.to = rs.getString("to");
//		metaMsg.type = MetaMessage.Type.valueOf((byte) (rs.getInt("byte")));
//		metaMsg.time = rs.getInt("time");
//		metaMsg.content = rs.getBytes("content");
//
//		return metaMsg;
//	}

	private JdbcTemplate getJt(String id, String type) {
		return clusterDatabases.getIdxJdbcTemplate(id, type);
	}

	private String generateSql(String sql, String id, String type) {
		String dbname = clusterDatabases.getDBName(id, type);
		String tableSuffix = clusterDatabases.getTableSuffix(id, type);

		sql = sql.replace("$db$", dbname);
		sql = sql.replace("$suffix$", tableSuffix);

		return sql;
	}
	
	public void setClusterDatabases(ClusterDatabases clusterDatabases) {
		this.clusterDatabases = clusterDatabases;
	}

	private ClusterDatabases clusterDatabases;
	

	private static final String SAVE_META_MESSAGE = "insert into $db$.meta_message_$suffix$ (id, meta) values(?,?)";
	private static final String GET_META_MESSAGE_BY_ID = "select id, meta from $db$.meta_message_$suffix$ where id=?";

	private static final String SAVE_META_MESSAGE_NONE_PB = "insert into $db$.meta_message_$suffix$ (id, from, to, type, content, time) values(?,?,?,?,?,?)";
	private static final String GET_META_MESSAGE_BY_ID_NONE_PB = "select id, from, to, type, time, content from $db$.meta_message_$suffix$ where id=?";


}
