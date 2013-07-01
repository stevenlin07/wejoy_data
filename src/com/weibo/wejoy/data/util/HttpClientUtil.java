package com.weibo.wejoy.data.util;

import static cn.sina.api.commons.util.ApiLogger.warn;
import static java.lang.String.format;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.net.URLCodec;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.StringPart;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.httpclient.params.HttpMethodParams;

import cn.sina.api.commons.thead.StandardThreadExecutor;
import cn.sina.api.commons.util.ApiHttpClient;
import cn.sina.api.commons.util.ApiLogger;
import cn.sina.api.commons.util.ByteArrayPart;
import cn.sina.api.commons.util.DefaultHttpClientAceessLog;
import cn.sina.api.commons.util.HttpManager;
import cn.sina.api.commons.util.Util;

public class HttpClientUtil {
	
	private MultiThreadedHttpConnectionManager connectionManager;
	private HttpClient client;
	private int maxSize;
	private String proxyHostPort;
	private int soTimeOut;
	private ExecutorService httpPool;
	private static final String DEFAULT_CHARSET = "utf-8";
	private ApiHttpClient.AccessLog accessLog = new DefaultHttpClientAceessLog();
	private static final URLCodec urlCodec = new URLCodec("utf-8");

	public HttpClientUtil() {
		this(150, 2000, 2000, 1048576);
	}

	public HttpClientUtil(int maxConPerHost, int conTimeOutMs, int soTimeOutMs, int maxSize) {
		this(maxConPerHost, conTimeOutMs, soTimeOutMs, maxSize, 1, 300);
	}

	public HttpClientUtil(int maxConPerHost, int conTimeOutMs, int soTimeOutMs, int maxSize, int minThread, int maxThread) {
		this.connectionManager = new MultiThreadedHttpConnectionManager();
		HttpConnectionManagerParams params = this.connectionManager.getParams();
		params.setMaxTotalConnections(600);
		params.setDefaultMaxConnectionsPerHost(maxConPerHost);
		params.setConnectionTimeout(conTimeOutMs);
		params.setSoTimeout(soTimeOutMs);
		this.soTimeOut = soTimeOutMs;

		HttpClientParams clientParams = new HttpClientParams();

		clientParams.setCookiePolicy("ignoreCookies");
		this.client = new HttpClient(clientParams, this.connectionManager);
		this.maxSize = maxSize;
		this.httpPool = new StandardThreadExecutor(minThread, maxThread);
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				httpPool.shutdown();
				connectionManager.shutdown();
			}
		}));
	}
	
	private final static int HTTP_OK = 200;
	private final static int HTTP_BAD = 400;
	
	public int postMulti(String url, Map<String, String> headers, Map<String, Object> nameValues, ByteArrayOutputStream out) {
		if (out == null) {
			out = new ByteArrayOutputStream();
		}

		return postMulti(url, headers, nameValues, out, DEFAULT_CHARSET);
	}

	public int postMulti(String url, Map<String, String> headers, Map<String, Object> nameValues, ByteArrayOutputStream out, String charset) {
		if (HttpManager.isBlockResource(url)) {
			ApiLogger.debug("multURLblockResource url=" + url);
			return HTTP_BAD;
		}
		PostMethod post = new PostMethod(url);
		addHeader(post, headers);
		Part[] parts = null;
		if (nameValues != null && !nameValues.isEmpty()) {
			parts = new Part[nameValues.size()];
			int i = 0;
			for (Map.Entry<String, Object> entry : nameValues.entrySet()) {
				if (entry.getValue() instanceof ByteArrayPart) {
					ByteArrayPart data = (ByteArrayPart) entry.getValue();
					parts[i++] = data;
				} else {
					parts[i++] = new StringPart(entry.getKey(), entry.getValue().toString(), DEFAULT_CHARSET);
				}
			}
		}
		post.setRequestEntity(new MultipartRequestEntity(parts, post.getParams()));
		
		return executeMethod4postMulti(url, post, out, charset);
	}
	
	private int executeMethod4postMulti(String url, HttpMethod method, ByteArrayOutputStream out, String charset) {
		long start = System.currentTimeMillis();
		int httpcode = 0;
		try {
			httpcode = doExecuteMethod(method, out);
			return httpcode;
		} catch (HttpClientUtilExcpetion e) {
			return HTTP_BAD;
		} finally {
			ApiLogger.debug(new StringBuilder(128).append("cost time = ").append(System.currentTimeMillis() - start).append(", url=").append(url)
					.append(", http code = ").append(method.getStatusLine() != null ? method.getStatusCode() : -1));
		}
	}
	
	public int getByte(String url, Map<String, String> headers, Map<String, String> params, ByteArrayOutputStream out) {
		if (out == null) {
			out = new ByteArrayOutputStream();
		}

		long start = System.currentTimeMillis();
		HttpMethod get = new GetMethod(url);
		addHeader(get, headers);
		addQueryStrings(get, params);
		int httpcode = 0;
		try {
			httpcode = doExecuteMethod(get, out);
			return httpcode;
		} catch (HttpClientUtilExcpetion e) {
			return HTTP_BAD;
		} finally {
			ApiLogger.debug(new StringBuilder(128).append("cost time = ").append(System.currentTimeMillis() - start).append(", url=").append(url)
					.append(", http code = ").append(get.getStatusLine() != null ? get.getStatusCode() : -1));

		}
	}

	public String postAsync( final String url, final Map<String, ?> nameValues, final Map<String, String> headers, final String charset) {
		Future<String> future = httpPool.submit(new Callable<String>() {
			public String call() throws Exception {
				return post(url, nameValues, headers, charset);
			}
		});
		
	    try {
	      return (String)future.get(this.soTimeOut, TimeUnit.MILLISECONDS);
	    } catch (Exception e) {
	      ApiLogger.warn(String.format("postAsync error url:%s post:%s msg:%s", new Object[] { url, mapToString(nameValues), e.getMessage() }));
	    }return "";
	}
	
	public String post(String url, Map<String, ?> nameValues, Map<String, String> headers, String charset) {
		PostMethod post = new PostMethod(url);
		HttpMethodParams params = new HttpMethodParams();
	    params.setContentCharset(charset);
	    post.setParams(params);
	    addHeader(post, headers);
	    
		if ((nameValues != null) && (!nameValues.isEmpty())) {
			List list = new ArrayList(nameValues.size());

			for (Map.Entry entry : nameValues.entrySet()) {
		        if ((entry.getKey() != null) && (!((String)entry.getKey()).isEmpty())) {
		          list.add(new NameValuePair((String)entry.getKey(), entry.getValue().toString()));
		        } else{
		        	try	{
		        		post.setRequestEntity(new StringRequestEntity(entry.getValue().toString(), "text/xml", "utf-8"));
		        	} catch (UnsupportedEncodingException e){
		        	}
		        }
		    }
			
			if (!list.isEmpty()) {
		        post.setRequestBody((NameValuePair[])list.toArray(new NameValuePair[list.size()]));
		    }
		}
		return executeMethod(url, post, mapToString(nameValues), charset);
	}
	
	public String getAsync(final String url, final Map<String, String> headers, final String charset) {
		Future<String> future = httpPool.submit(new Callable<String>(){
			public String call() throws Exception {
				return get(url, headers, charset);
			}
		});
		
	    try {
	        return (String)future.get(this.soTimeOut, TimeUnit.MILLISECONDS);
	    } catch (Exception e) {
	      ApiLogger.warn("getAsync error url:" + url + " msg:" + e.getMessage());
	      return "";
	    }
	}
	
	public String get(String url, Map<String, String> headers, String charset) {
	    if (HttpManager.isBlockResource(url)) {
	      ApiLogger.debug("getURL blockResource url=" + url);
	      return "";
	    }
	    HttpMethod get = new GetMethod(url);
	    HttpMethodParams params = new HttpMethodParams();
	    params.setContentCharset(charset);
	    params.setUriCharset(charset);
	    get.setParams(params);
	    addHeader(get, headers);
	    return executeMethod(url, get, null, charset);
	}
	
	private String executeMethod(String url, HttpMethod method, String postString, String charset) {
	    String result = null;
	    long start = System.currentTimeMillis();
	    int len = 0;
	    try {
		    ByteArrayOutputStream out = new ByteArrayOutputStream();
		    len = doExecuteMethod(method, out);
		    result = new String(out.toByteArray(), charset);
		    return result;
	    } catch (UnsupportedEncodingException e) {
	        ApiLogger.warn(String.format("ApacheHttpClient.executeMethod UnsupportedEncodingException url:%s charset:%s", new Object[] { url, charset }), e);
	        result = "";
	        return result;
	    } catch(HttpClientUtilExcpetion e){
	    	result = "";
		    return result;
	    } finally { 
	    	ApiLogger.warn(new StringBuilder(128).append("cost time = ").append(System.currentTimeMillis() - start).append(", url=").append(url)
					.append(", http code = ").append(method.getStatusLine() != null ? method.getStatusCode() : -1)); 
	    } 
	 }
	 
	private int doExecuteMethod(HttpMethod httpMethod, OutputStream out) throws HttpClientUtilExcpetion {
		long start = System.currentTimeMillis();
		int readLen = 0;
		int httpcode = 0;
		try {
			httpcode = client.executeMethod(httpMethod);
			if (System.currentTimeMillis() - start > this.soTimeOut) {
				throw new ReadTimeOutException(format("executeMethod so timeout time:%s soTimeOut:%s",
						(System.currentTimeMillis() - start), soTimeOut));
			}
			InputStream in = httpMethod.getResponseBodyAsStream();
			
			byte[] b = new byte[1024];
			int len = 0;
			while ((len = in.read(b)) > 0) {
				if (System.currentTimeMillis() - start > this.soTimeOut) {
					throw new ReadTimeOutException(format("read so timeout time:%s soTimeOut:%s", (System.currentTimeMillis() - start),
							soTimeOut));
				}
				out.write(b, 0, len);
				readLen += len;
				if (readLen > maxSize)
					throw new SizeException(format("size too big size:%s maxSize:%s", readLen, maxSize));
			}
			in.close();
		} catch (HttpClientUtilExcpetion ex) {
			warn(format("HttpClientUtilExcpetion url:%s message:%s", getHttpMethodURL(httpMethod), ex.getMessage()));
			throw ex;
		} catch (Exception ex) {
			warn(format("HttpClientUtil.doExecuteMethod error! msg:%s", ex.getMessage()));
		} finally {
			httpMethod.releaseConnection();
		}
		return httpcode;
	}
	 
	private String getHttpMethodURL(HttpMethod httpMethod) {
		try {
			return httpMethod.getURI().toString();
		} catch (URIException e) {
			return "";
		}
	}
	
	private static void addHeader(HttpMethod method, Map<String, String> headers) {
		if (headers != null && !headers.isEmpty()) {
			for (Map.Entry<String, String> entry : headers.entrySet()) {
				method.setRequestHeader(entry.getKey(), entry.getValue());
			}
		}
	}
	
	private static void addQueryStrings(HttpMethod method, Map<String, String> queryStrs) {
		if (queryStrs != null && !queryStrs.isEmpty()) {
			NameValuePair[] querys = new NameValuePair[queryStrs.size()];

			int i = 0;
			for (Map.Entry<String, String> entry : queryStrs.entrySet()) {
				querys[i++] = new NameValuePair(entry.getKey(), entry.getValue());
			}

			method.setQueryString(querys);
		}
	}
	
	private String mapToString(Map<String, ?> nameValues) {
	    StringBuffer sb = new StringBuffer();
	    if (nameValues == null) {
	      return sb.toString();
	    }
	    
	    for (Map.Entry entry : nameValues.entrySet()) {
	    	if ((entry.getValue() instanceof String)) {
	    		sb.append((String)entry.getKey() + "=" + entry.getValue() + "&");
	    	} else if ((entry.getValue() instanceof String[])) {
	    		String[] values = (String[])(String[])entry.getValue();
	    		for (String value : values) {
	    			sb.append((String)entry.getKey() + "=" + value + "&");
	    		}
	    	}
	    }
	    Util.trim(sb, '&');
	    return sb.toString();
	}
	
	public class HttpClientUtilExcpetion extends Exception {
		private static final long serialVersionUID = 5604686053507182177L;
	
		public HttpClientUtilExcpetion(String msg) {
			super(msg);
		}
	}
	
	public class ReadTimeOutException extends HttpClientUtilExcpetion {
		private static final long serialVersionUID = 1173052533486842516L;
	
		public ReadTimeOutException(String msg) {
			super(msg);
		}
	}
	
	public class SizeException extends HttpClientUtilExcpetion {
		private static final long serialVersionUID = 872657850967574460L;
	
		public SizeException(String msg) {
			super(msg);
		}
	}
}
