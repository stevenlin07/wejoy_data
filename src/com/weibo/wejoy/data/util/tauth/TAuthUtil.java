package com.weibo.wejoy.data.util.tauth;

import java.io.FileInputStream;
import java.net.URL;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.commons.codec.binary.Base64;

import cn.sina.api.commons.util.ApiLogger;
import cn.sina.api.commons.util.JsonWrapper;

import com.weibo.wejoy.data.util.HttpClientUtil;
import com.weibo.wejoy.data.util.Util;
import com.weibo.wejoy.data.util.tauth.TAuthStore.TauthToken;

public class TAuthUtil {
	private static final String configfile = "meyou_conf.properties";
	
	private static Properties prop = null;
	private static final ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
	private String appKey = null;
	private static TAuthUtil instance ;
	private HttpClientUtil httpClient;
	
	public static synchronized TAuthUtil getInstance(HttpClientUtil httpClient) {
		if(instance == null){
			synchronized(TAuthUtil.class){
				if (instance == null){
					instance  = new TAuthUtil(httpClient);
				}
			}
		}
		return instance;
	}
	
	public String getAppKey() {
		return appKey;
	}

	private TAuthUtil(HttpClientUtil httpClient) {
		this.httpClient= httpClient;
		
		FileInputStream in = null;
		prop = new Properties();
		
		try {
			URL url = TAuthUtil.class.getClassLoader().getResource(configfile);
			in = new FileInputStream(url.getFile());
			prop.load(in);
		}
		catch(Exception e) {
			ApiLogger.error(e.getMessage(), e);
		}
		finally {
			try {
				in.close();
			} 
			catch (Exception e) {
				// ignore
			}
		}
		
		String needtauth = Util.getConfigProp("needtauth", "true");
		
		if("true".equalsIgnoreCase(needtauth)) {
			TAuthStore.init();
			refreshTauthToken();
		
			timer.scheduleWithFixedDelay(new Runnable() {
				public void run() {
					refreshTauthToken();
				}
			}, 8, 8, java.util.concurrent.TimeUnit.HOURS);
		}
	}
	
	private void refreshTauthToken() {
		ApiLogger.info("refresh Tauth Token begin ...");
		
		List<TauthToken> list = TAuthStore.getTokens();
		
		for(TauthToken tt : list) {
			String date = tt.date; 
			String today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
			String tokenStr = null;
			String refreshTokenUrl = null;
			
			if (!today.equals(date)) {
				String ret = null;
				try {
					ApiLogger.info("refreshTauthTokenFromApi begin ...");
					Map params = new HashMap();
					params.put("app_secret", tt.secret);
					refreshTokenUrl = prop.getProperty("refreshTokenUrl", 
							"http://i.api.weibo.com/auth/tauth_token.json?source=" + tt.source);
					ret = this.httpClient.post(refreshTokenUrl, params, null, "utf-8");
					
					// retry
					if (ret == null || ret.trim().isEmpty())
						ret = this.httpClient.post(refreshTokenUrl, params, null, "utf-8");
					JsonWrapper json = new JsonWrapper(ret.trim());
					String newToken = null;
					
					if ((newToken = json.get("tauth_token"))!= null && !newToken.trim().isEmpty())
					{
						newToken = newToken.trim();
						tt.token = newToken;
						ApiLogger.info("refreshTauthTokenFromApi end! token change! source:" + tt.source + ", new token:" + newToken);
					} 
					else {
						String info = "refreshTauthTokenFromApi error! ret:" + ret + ", url:" + refreshTokenUrl; 
						ApiLogger.error(info);
					}
				} 
				catch (Exception e) {
					String info = "refreshTauthTokenFromApi error! ret:" + ret + ", url:" + refreshTokenUrl; 
					ApiLogger.error(info, e);
				}
			}
		}
		
		TAuthStore.saveToken(list);
	}
	
	private static ThreadLocal<MessageDigest> MD5 = new ThreadLocal<MessageDigest>() {
		@Override
		protected MessageDigest initialValue() {
			try {
				return MessageDigest.getInstance("MD5");
			} catch (Exception e) {
			}
			return null;
		}
	};
	
	private static String md5(String s) throws Exception {
		return  md5Digest((s == null ? "" : s).getBytes());
	}
	
	private static String md5Digest(byte[] data) throws Exception {
		MessageDigest md5 = MD5.get();
		md5.reset();
		md5.update(data);
		byte[] digest = md5.digest();
		return encodeHex(digest);
	}
	
	private static String encodeHex(byte[] bytes) {
		StringBuilder buf = new StringBuilder(bytes.length + bytes.length);
		for (int i = 0; i < bytes.length; i++) {
			if (((int) bytes[i] & 0xff) < 0x10) {
				buf.append("0");
			}
			buf.append(Long.toString((int) bytes[i] & 0xff, 16));
		}
		return buf.toString();
	}
	
	public String getToken(String uid, String source) {
		String tokenStr = TAuthStore.getToken(source);
		
		String token = null;
		
		try {
			String authStr = uid + ":" + md5(uid + tokenStr.trim());
			token = "Token " + new String(Base64.encodeBase64(authStr.getBytes("utf-8")), "utf-8");
		}
		catch(Exception e) {
			ApiLogger.error("get token failed caused by " + e.getMessage());
		}
		
		return token.trim();
	}
}
