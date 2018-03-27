package com.intrence.cdg.net;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import com.intrence.config.collection.ConfigMap;
import com.intrence.config.configloader.ConfigMapUpdateHandler;
import com.intrence.cdg.bootstrap.SourcesConfigUpdateHandler;
import com.intrence.cdg.exception.CdgBackendException;
import com.intrence.cdg.net.header.HeaderCreator;
import com.intrence.cdg.net.header.HeaderFactory;
import com.intrence.cdg.net.signature.SignCreator;
import com.intrence.cdg.net.signature.SignatureFactory;
import com.intrence.cdg.streamer.provider.HTTPStreamProvider;
import com.intrence.cdg.streamer.provider.StreamProvider;
import com.intrence.cdg.util.Constants;
import oauth.signpost.OAuthConsumer;
import oauth.signpost.commonshttp.CommonsHttpOAuthConsumer;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;


@Component
public class WebFetcher implements ConfigMapUpdateHandler {
    public static final Integer CONNECTION_TIMEOUT = 10;
    public static final Integer SOCKET_TIMEOUT = 10;

    private static final String USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17";
    private static final String RATE_LIMIT_CONFIG = "max_records_per_sec";
    //550 proxy ip connections+ other 150 connection are for sources that are not required any proxy
    //and to the proxy server
    private static final Integer DEFAULT_MAX_CONNECTIONS= 700;
    //we may not make more than fixed number of proxy ips for a route
    private static final Integer DEFAULT_MAX_CONNECTIONS_PER_ROUTE = 550;
    private static final Set<String> HTTP_METHODS_WITH_BODY = ImmutableSet.of(HttpPost.METHOD_NAME, HttpPut.METHOD_NAME);
    private static final Logger LOGGER = Logger.getLogger(WebFetcher.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private Map<String, OAuthConsumer> oauthConsumerMap = new HashMap<>();
    private Map<String, RateLimiter> rateLimiterMap = new ConcurrentHashMap<>();
    private Map<String, Integer> connectionTimeoutMap = new ConcurrentHashMap<>();
    private Map<String, Integer> socketTimeoutMap = new ConcurrentHashMap<>();

    private ConfigMap configMap;
    private String pxProxyJson;
    private ConfigMap sourcesConfigMap;
    private PoolingHttpClientConnectionManager connectionManager;
    protected CloseableHttpClient httpClient;
    private ScheduledExecutorService httpConnectionMonitoringScheduler;
    private HttpUtils.IdleConnectionMonitorThread connectionMonitorThread;
    private List<String> proxies;
    private ParserWebFetcher parserWebFetcher;

    @Autowired
    public WebFetcher(ConfigMap configMap, ConfigMap sourcesConfigMap){
        this.configMap = configMap;
        this.sourcesConfigMap = sourcesConfigMap;
    }

    @PostConstruct
    public void initWebFetcher() throws IOException{
        this.proxies = loadProxies();
        this.oauthConsumerMap = initOauthConsumers();
        this.rateLimiterMap = initRateLimiters();
        this.pxProxyJson = getProxyInfoFromServer();
        initTimeouts();
        this.initHttpClient();
        SourcesConfigUpdateHandler.registerConfigUpdateListener(this);
        parserWebFetcher = ParserWebFetcher.getInstance(oauthConsumerMap, rateLimiterMap, connectionTimeoutMap,
                socketTimeoutMap, proxies, pxProxyJson, httpClient, sourcesConfigMap, configMap);
    }

    @PreDestroy
    public void shutDown() throws IOException {
        connectionMonitorThread.shutdown();
        httpClient.close();
        connectionManager.close();
        httpConnectionMonitoringScheduler.shutdown();
    }

    protected void initHttpClient() {
        connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setValidateAfterInactivity(100);
        connectionManager.setMaxTotal(DEFAULT_MAX_CONNECTIONS); //Total max concurrent HTTP connections to all HTTPRoutes served by manager
        connectionManager.setDefaultMaxPerRoute(DEFAULT_MAX_CONNECTIONS_PER_ROUTE); // Total max connections assigned to a HTTTPRoute
        SSLConnectionSocketFactory sslsf = HttpUtils.getCustomSslConnectionSocketFactory();
        httpClient = HttpClients.custom()
                .setUserAgent(USER_AGENT)
                .setSSLSocketFactory(sslsf)
                .setConnectionManager(connectionManager)
                .setDefaultRequestConfig(getDefaultRequestConfigBuilder().build())
                .build();

        httpConnectionMonitoringScheduler = Executors.newScheduledThreadPool(1);
        connectionMonitorThread = new HttpUtils.IdleConnectionMonitorThread(connectionManager);
        httpConnectionMonitoringScheduler.scheduleAtFixedRate(connectionMonitorThread, 10, 60,
                TimeUnit.SECONDS);

    }

    protected Map<String, OAuthConsumer> initOauthConsumers() {
        Map<String, OAuthConsumer> map = new HashMap<>();
        for (String source: sourcesConfigMap.keySet()) {
            ConfigMap sourceMap = sourcesConfigMap.getMap(source);
            if (sourceMap != null) {
                ConfigMap oauthMap = sourceMap.getMap(Constants.OAUTH, null);
                String key = null;
                String secret = null;
                if (oauthMap != null && (key=oauthMap.getString("key", null))!=null &&
                        (secret=oauthMap.getString("secret", null))!=null) {
                    OAuthConsumer oauthConsumer = new CommonsHttpOAuthConsumer(key, secret);
                    map.put(source, oauthConsumer);
                } else if (oauthMap != null) {
                    throw new IllegalArgumentException(String.format("Source %s does not have key and secret to establish oauth, check config", source));
                }
            }
        }
        return map;
    }

    protected Map<String, RateLimiter> initRateLimiters() {
        Map<String, RateLimiter> rateLimiterMap = new HashMap<>();
        for (String source: sourcesConfigMap.keySet()) {
            Integer reqsPerSec = sourcesConfigMap.getMap(source).getInteger(RATE_LIMIT_CONFIG, 1);
            rateLimiterMap.put(source, RateLimiter.create(reqsPerSec));
        }
        return rateLimiterMap;
    }

    protected Double getRateLimit(String source) {
        RateLimiter rateLimiter = this.rateLimiterMap.get(source);
        if (rateLimiter != null) {
            return rateLimiter.getRate();
        } else {
            return null;
        }
    }

    /**
     * This method called once during WebFetcher initialization to cache all source specific connection and socket
     * timeouts
     */
    private void initTimeouts() {
        for (String source : sourcesConfigMap.keySet()) {
            Integer connectionTimeout = sourcesConfigMap.getMap(source).getInteger(Constants.CONNECTION_TIMEOUT_KEY,
                    CONNECTION_TIMEOUT);
            connectionTimeoutMap.put(source, connectionTimeout);

            Integer socketTimeout = sourcesConfigMap.getMap(source).getInteger(Constants.SOCKET_TIMEOUT_KEY,
                    SOCKET_TIMEOUT);
            socketTimeoutMap.put(source, socketTimeout);
        }
    }

    /**
     * This function downloads the response as stream.
     *
     * @param source unique id of the row
     * @param fetchReq unique id of the row
     * @return RequestResponse for given fetchRequest
     * @throws Exception
     */
    public RequestResponse getStreamResponse(String source, FetchRequest fetchReq) throws Exception{
        return fetchContent(source, fetchReq, true);
    }

    /**
     * @param source unique id of the row
     * @param fetchReq unique id of the row
     * @return RequestResponse for given fetchRequest
     * @throws Exception
     */
    public RequestResponse getResponse(String source, FetchRequest fetchReq) throws Exception {
        return fetchContent(source, fetchReq, false);
    }

    private RequestResponse fetchContent(String source, FetchRequest fetchReq, boolean isStreamResponse) throws Exception {

        HttpClientContext context;
        //set proxy if required
        if (checkProxyRequired(source)) {
            context = getContextWithProxy(fetchReq.getWorkRequest(), source);
        } else {
            context = getContextWithoutProxy(fetchReq.getWorkRequest(), source);
        }

        RequestBuilder requestBuilder;
        String signedURL = signRequest(source, fetchReq.getWorkRequest());
        switch (fetchReq.getMethodType()) {
            case HttpGet.METHOD_NAME: requestBuilder =  RequestBuilder.get(signedURL);
                break;
            case HttpPost.METHOD_NAME: requestBuilder =  RequestBuilder.post(signedURL);
                break;
            case HttpPut.METHOD_NAME: requestBuilder =  RequestBuilder.put(signedURL);
                break;
            default: throw new NotImplementedException("Http Method not yet supported");

        }

        buildRequest(source, requestBuilder, context);
        addDynamicHeaders(source, requestBuilder);
        if (HTTP_METHODS_WITH_BODY.contains(fetchReq.getMethodType())) {
            addBody(source, fetchReq, requestBuilder);
        }
        HttpUriRequest httpUriRequest = requestBuilder.build();

        //apply oauth signing
        if (checkOauthRequired(source)) {
            oauthConsumerMap.get(source).sign(httpUriRequest);
        }

        //rate limit
        rateLimiterMap.get(source).acquire();
        LOGGER.info(String.format("sending request to url=%s", fetchReq.getWorkRequest()));
        // wait for response to finish fetching or expire

        ExecutorService executorService  = Executors.newSingleThreadExecutor();
        try {
            Future<RequestResponse> future = isStreamResponse ?
                    executorService.submit(new FetchStreamContentWorker(source, httpUriRequest, fetchReq, context)) :
                    executorService.submit(new FetchPageContentWorker(httpUriRequest, fetchReq, context));
            return future.get(socketTimeoutMap.get(source) + connectionTimeoutMap.get(source) + 1, TimeUnit.SECONDS);
        } finally {
            executorService.shutdownNow();
        }
    }

    protected void buildRequest(String source, RequestBuilder requestBuilder, HttpClientContext context) throws Exception{
        ConfigMap headers = sourcesConfigMap.getMap(source).getMap(Constants.REQUEST_HEADERS, null);
        requestBuilder.setConfig(context.getRequestConfig());
        setHeaders(headers, requestBuilder);
    }

    private void setHeaders(ConfigMap headers, RequestBuilder requestBuilder) {
        if (headers == null) {
            return;
        }
        for (String headerKey : headers.keySet()) {
            requestBuilder.addHeader(headerKey, headers.getString(headerKey));
        }
    }

    private void addDynamicHeaders(String source, RequestBuilder requestBuilder) throws Exception{
        HeaderCreator headerCreator = HeaderFactory.getHeaderCreator(sourcesConfigMap, source, this);
        if(headerCreator != null) {
            headerCreator.addDynamicHeader(source, requestBuilder);
        }
    }

    private void addBody(String source, FetchRequest fetchReq, RequestBuilder requestBuilder) {
        ConfigMap postHeaders = sourcesConfigMap.getMap(source).getMap(Constants.POST_REQUEST_HEADERS, null);
        setHeaders(postHeaders, requestBuilder);
        //set POST body
        if (StringUtils.isBlank(fetchReq.getHttpBody())) {
            LOGGER.warn(String.format("Event=MissingBody for POST/PUT request=%s for source=%s", fetchReq.getWorkRequest(), source));
            return;
        }
        StringEntity stringEntity = new StringEntity(fetchReq.getHttpBody(), Constants.DEFAULT_CHARSET);
        if (postHeaders == null) {
            LOGGER.warn(String.format("Event=MissingHeader for POST/PUT request=%s for source=%s", fetchReq.getWorkRequest(), source));
        } else {
            if (postHeaders.containsKey(HttpHeaders.CONTENT_TYPE)) {
                stringEntity.setContentType(postHeaders.getString(HttpHeaders.CONTENT_TYPE));
            } else {
                LOGGER.warn(String.format("Event=MissingHeader Content-Type is missing for POST/PUT request=%s for source=%s. Default content-type:text/plain will be added", fetchReq.getWorkRequest(), source));
            }
        }
        requestBuilder.setEntity(stringEntity);
    }

    protected boolean checkOauthRequired(String source) {
        return sourcesConfigMap.getMap(source).containsKey(Constants.OAUTH);
    }

    protected boolean checkProxyRequired(String source) {
        return sourcesConfigMap.getMap(source).getBoolean(Constants.IS_PROXY_REQUIRED, false);
    }

    private RequestConfig.Builder getDefaultRequestConfigBuilder(){
        return  RequestConfig.custom()
                .setConnectTimeout(CONNECTION_TIMEOUT * 1000)
                .setSocketTimeout(SOCKET_TIMEOUT * 1000)
                .setRedirectsEnabled(false)
                .setRelativeRedirectsAllowed(false)
                .setConnectionRequestTimeout(CONNECTION_TIMEOUT * 1000);
    }

    private RequestConfig.Builder getCustomRequestConfigBuilder(String source){
        return  RequestConfig.custom()
                .setConnectTimeout(connectionTimeoutMap.get(source) * 1000)
                .setSocketTimeout(socketTimeoutMap.get(source) * 1000)
                .setRedirectsEnabled(false)
                .setRelativeRedirectsAllowed(false)
                .setConnectionRequestTimeout(connectionTimeoutMap.get(source) * 1000);
    }

    protected HttpClientContext getContextWithProxy(String urlStr, String source) throws Exception {

        String proxyResponse = pxProxyJson; //going to pxproxy server by default
        if(!configMap.getBoolean("use_proxy_service", false) || urlStr.contains("https://")){
            proxyResponse = getLocalProxyInfo(urlStr);
        }

        JsonNode proxyJson = MAPPER.readTree(proxyResponse);

        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(new AuthScope(proxyJson.get("proxy").asText(), proxyJson.get("port").asInt()),
                new UsernamePasswordCredentials(proxyJson.get("user").asText(), proxyJson.get("pw").asText()));

        HttpClientContext localContext = HttpClientContext.create();

        localContext.setCredentialsProvider(credsProvider);
        HttpHost proxy = new HttpHost(proxyJson.get("proxy").asText(), proxyJson.get("port").asInt());
        LOGGER.info(String.format("Event=ProxyStats proxy=%s, port=%d for url=%s", proxyJson.get("proxy").asText(),
                proxyJson.get("port").asInt(),urlStr));

        RequestConfig config = getCustomRequestConfigBuilder(source)
                .setProxy(proxy)
                .build();
        localContext.setRequestConfig(config);
        return localContext;
    }

    protected HttpClientContext getContextWithoutProxy(String urlStr, String source) throws Exception {
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider = provideCredentials(credsProvider, urlStr, source);
        HttpClientContext localContext = HttpClientContext.create();
        localContext.setCredentialsProvider(credsProvider);

        RequestConfig config = getCustomRequestConfigBuilder(source).build();
        localContext.setRequestConfig(config);
        return localContext;
    }

    CredentialsProvider provideCredentials(CredentialsProvider credentialsProvider, String urlStr, String source){
        String username, password;
        try {
            if (sourcesConfigMap.getMap(source).containsKey(Constants.AUTH_KEY)) {
                username = sourcesConfigMap.getMap(source).getMap(Constants.AUTH_KEY).get(Constants.USERNAME).toString();
                password = sourcesConfigMap.getMap(source).getMap(Constants.AUTH_KEY).get(Constants.PASSWORD).toString();
                Credentials credentials = new UsernamePasswordCredentials(username, password);
                URL url = new URL(urlStr);
                credentialsProvider.setCredentials(new AuthScope(url.getHost(), url.getPort()), credentials);
            }
        }
        catch (Exception e){
            LOGGER.error(String.format("Event=setUsernamePassword  -  Exception in getting auth info. for source =%s ",source), e);
        }
        return credentialsProvider;
    }

    protected String getLocalProxyInfo(String url) throws CdgBackendException {
        //Currently url would not be used
        String proxy =  proxies.get(ThreadLocalRandom.current().nextInt(0,proxies.size()));
        if(!StringUtils.isBlank(proxy)){
            String[] proxyData = proxy.split(":");
            if(proxyData.length==4) {
                JSONObject proxyJson = new JSONObject();
                proxyJson.put("proxy", proxyData[0]);
                proxyJson.put("port", proxyData[1]);
                proxyJson.put("user", proxyData[2]);
                proxyJson.put("pw", proxyData[3]);

                return proxyJson.toString();
            }
        }

        throw new CdgBackendException("No proxy available");
    }

    private String getProxyInfoFromServer() {

        JSONObject proxyJson = new JSONObject();
        try {
            proxyJson.put("proxy", configMap.getString("pxproxy_host", null));
            proxyJson.put("port", configMap.getInteger("pxproxy_port", null));
            proxyJson.put("user", configMap.getString("pxproxy_user", null));
            proxyJson.put("pw", configMap.getString("pxproxy_pw", null));
        }
        catch (Exception e){
            LOGGER.error("Event=getProxyInfoFromServer  -  Exception in getting pxproxy info. " + e.getMessage());
        }
        return proxyJson.toString();
    }

    protected List<String> loadProxies() throws IOException{
        List<String> proxyList = new ArrayList<>();
        InputStream i = this.getClass().getClassLoader().getResourceAsStream("proxies");
        BufferedReader r = new BufferedReader(new InputStreamReader(i));
        String line;
        while ((line = r.readLine()) != null) {
            proxyList.add(line);
        }
        i.close();
        return proxyList;
    }

    private String signRequest(String source, String url) throws Exception {
        SignCreator signCreator = SignatureFactory.getSignCreator(sourcesConfigMap, source);
        return signCreator != null ? signCreator.signURL(url) : url;
    }

    public final class FetchStreamContentWorker implements Callable<RequestResponse> {

        private String source;
        private HttpUriRequest httpUriRequest;
        private HttpClientContext context;
        private FetchRequest fetchReq;


        public FetchStreamContentWorker(String source, HttpUriRequest httpUriRequest, FetchRequest fetchReq, HttpClientContext context) {
            this.source = source;
            this.httpUriRequest = httpUriRequest;
            this.context = context;
            this.fetchReq = fetchReq;
        }

        @Override
        public RequestResponse call() throws Exception {
            RequestResponse requestResponse = null;
            StreamProvider streamProvider = null;
            HttpEntity entity = null;
            CloseableHttpResponse response = null;
            try {
                response = executeHttp(httpUriRequest, context);
                int status = response.getStatusLine().getStatusCode();
                if (Constants.HTTP_REDIRECTION_STATUS_CODES.contains(status)) {
                    Header redirectedUrl = response.getFirstHeader("location");
                    entity = response.getEntity();
                    requestResponse = new RequestResponse(fetchReq, EntityUtils.toString(entity), status, redirectedUrl.getValue());
                    closeConnection(entity, response, httpUriRequest);
                } else {
                    ConfigMap streamMap = sourcesConfigMap.getMap(source).getMap(Constants.STREAM);
                    Integer batchSize = streamMap.getInteger(Constants.STREAM_BATCH_SIZE, 0);
                    String recordIdentifier = streamMap.getString(Constants.STREAM_RECORD_IDENTIFIER, null);
                    String compressionType = getCompressionType(source);
                    if (StringUtils.isBlank(compressionType)) {
                        streamProvider = new HTTPStreamProvider(response, httpUriRequest, batchSize, recordIdentifier);
                    } else {
                        streamProvider = new HTTPStreamProvider(response, httpUriRequest, batchSize, recordIdentifier, compressionType);
                    }
                    requestResponse = new RequestResponse(fetchReq, streamProvider, status);
                }
            } catch (Exception e) {
                closeConnection(entity, response, httpUriRequest);
                throw new CdgBackendException("Exception in fetching stream response.");
            }
            return requestResponse;
        }

    }

    private String getCompressionType(String source){
        String compressionType=null;
        if(sourcesConfigMap.getMap(source).containsKey(Constants.COMPRESSION_TYPE)) {
            compressionType = sourcesConfigMap.getMap(source).getString(Constants.COMPRESSION_TYPE);
        }
        return compressionType;
    }

    public final class FetchPageContentWorker implements Callable<RequestResponse> {
        HttpUriRequest httpUriRequest;
        FetchRequest fetchReq;
        HttpClientContext context;

        public FetchPageContentWorker(HttpUriRequest httpUriRequest, FetchRequest fetchReq, HttpClientContext context) {
            this.httpUriRequest = httpUriRequest;
            this.fetchReq = fetchReq;
            this.context = context;
        }

        @Override
        public RequestResponse call() throws Exception {
            // Send http GET request through proxy without following redirects
            CloseableHttpResponse response = null;
            HttpEntity entity = null;
            try {
                response = executeHttp(httpUriRequest, context);
                int status = response.getStatusLine().getStatusCode();
                String responseContent = null;
                entity = response.getEntity();
                if (entity != null) {
                    responseContent = EntityUtils.toString(entity);
                }
                if (Constants.HTTP_REDIRECTION_STATUS_CODES.contains(status)) {
                    Header redirectedUrl = response.getFirstHeader("location");
                    return new RequestResponse(fetchReq, responseContent, status, redirectedUrl.getValue());
                } else {
                    return new RequestResponse(fetchReq, responseContent, status);
                }
            } finally {
                closeConnection(entity, response, httpUriRequest);
            }
        }
    }
    /**
     * This method updates only rate limit, connection and socket timeout for every source.
     *
     * @param updatedSourcesConfigMap updated source config pushed from config-central whenever there is any change in
     *            sources.config.yml file
     *
     */
    @Override
    public void handleUpdate(ConfigMap updatedSourcesConfigMap) {

        LOGGER.debug("Updating rate limit and connection timeout config");
        for (String source : updatedSourcesConfigMap.keySet()) {
            // update rate limiters
            Integer reqsPerSec = updatedSourcesConfigMap.getMap(source).getInteger(RATE_LIMIT_CONFIG, 1);
            RateLimiter rateLimiter = this.rateLimiterMap.get(source);

            if (rateLimiter.getRate() != reqsPerSec) {
                LOGGER.info(String.format("Rate limited is updated for source=%s newRate=%s", source, reqsPerSec));
                rateLimiter.setRate(reqsPerSec);
            }

            // update source specific connection timeout
            Integer connectionTimeout = updatedSourcesConfigMap.getMap(source)
                    .getInteger(Constants.CONNECTION_TIMEOUT_KEY, CONNECTION_TIMEOUT);
            if (connectionTimeoutMap.get(source) != connectionTimeout) {
                connectionTimeoutMap.put(source, connectionTimeout);
            }

            // update source specific socket timeout
            Integer socketTimeout = updatedSourcesConfigMap.getMap(source).getInteger(Constants.SOCKET_TIMEOUT_KEY,
                    SOCKET_TIMEOUT);
            if (socketTimeoutMap.get(source) != socketTimeout) {
                socketTimeoutMap.put(source, socketTimeout);
            }
        }
    }

    private CloseableHttpResponse executeHttp(HttpUriRequest httpUriRequest, HttpClientContext context) throws Exception {
        CloseableHttpResponse response = httpClient.execute(httpUriRequest, context);
        if(response.getStatusLine() == null) {
            throw new IOException("response.getStatusLine() is null");
        }
        return response;
    }

    private void closeConnection(HttpEntity entity, CloseableHttpResponse response, HttpUriRequest httpUriRequest) throws IOException {
        if (entity != null) {
            EntityUtils.consume(entity);
        }
        if (response != null) {
            response.close();
        }
        if (httpUriRequest != null) {
            ((HttpRequestBase)httpUriRequest).releaseConnection();
        }
    }

}
