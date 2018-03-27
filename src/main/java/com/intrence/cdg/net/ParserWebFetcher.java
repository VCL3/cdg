package com.intrence.cdg.net;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.RateLimiter;
import com.intrence.config.collection.ConfigMap;
import com.intrence.cdg.util.Constants;
import oauth.signpost.OAuthConsumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 *
 * This is a light-weight web-fetcher initialized from WebFetcher in Dora module.
 *
 */
public class ParserWebFetcher {
    public static final Integer CONNECTION_TIMEOUT = 10;
    public static final Integer SOCKET_TIMEOUT = 10;
    private static final Logger LOGGER = Logger.getLogger(ParserWebFetcher.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private Map<String, OAuthConsumer> oauthConsumerMap = new HashMap<>();
    private Map<String, RateLimiter> rateLimiterMap = new ConcurrentHashMap<>();
    private Map<String, Integer> connectionTimeoutMap = new ConcurrentHashMap<>();
    private Map<String, Integer> socketTimeoutMap = new ConcurrentHashMap<>();
    private static ParserWebFetcher subDealWebFetcher;

    private ConfigMap configMap;
    private String pxProxyJson;
    private ConfigMap sourcesConfigMap;
    protected CloseableHttpClient httpClient;
    private List<String> proxies;

    private ParserWebFetcher(Map<String, OAuthConsumer> oauthConsumerMap, Map<String, RateLimiter> rateLimiterMap,
                             Map<String, Integer> connectionTimeoutMap, Map<String, Integer> socketTimeoutMap, List<String> proxies,
                             String pxProxyJson, CloseableHttpClient httpClient, ConfigMap sourcesConfigMap, ConfigMap configMap) {
        this.proxies = proxies;
        this.oauthConsumerMap = oauthConsumerMap;
        this.rateLimiterMap = rateLimiterMap;
        this.pxProxyJson = pxProxyJson;
        this.connectionTimeoutMap = connectionTimeoutMap;
        this.socketTimeoutMap = socketTimeoutMap;
        this.sourcesConfigMap = sourcesConfigMap;
        this.configMap = configMap;
        this.httpClient = httpClient;
    }

    public synchronized static ParserWebFetcher getInstance(Map<String, OAuthConsumer> oauthConsumerMap,
            Map<String, RateLimiter> rateLimiterMap, Map<String, Integer> connectionTimeoutMap,
            Map<String, Integer> socketTimeoutMap, List<String> proxies, String pxProxyJson,
            CloseableHttpClient httpClient, ConfigMap sourcesConfigMap, ConfigMap configMap) {
        if (subDealWebFetcher == null) {
            subDealWebFetcher = new ParserWebFetcher(oauthConsumerMap, rateLimiterMap, connectionTimeoutMap,
                    socketTimeoutMap, proxies, pxProxyJson, httpClient, sourcesConfigMap, configMap);
        }
        return subDealWebFetcher;
    }

    public static ParserWebFetcher getInstance() {
        return subDealWebFetcher;
    }

    public String getResponse(String source, String subDealUrl) throws Exception {

        HttpClientContext context;
        // set proxy if required
        if (checkProxyRequired(source)) {
            context = getContextWithProxy(subDealUrl, source);
        } else {
            context = HttpClientContext.create();
            context.setRequestConfig(getCustomRequestConfigBuilder(source).build());
        }

        HttpGet httpGet = new HttpGet(subDealUrl);
        addHeaders(source, httpGet);
        httpGet.setConfig(context.getRequestConfig());

        // apply oauth signing
        if (checkOauthRequired(source)) {
            oauthConsumerMap.get(source).sign(httpGet);
        }

        // rate limit
        rateLimiterMap.get(source).acquire();

        LOGGER.info(String.format("sending request to url=%s", subDealUrl));
        // wait for response to finish fetching or expire

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executorService.submit(new FetchPageContentWorker(httpGet, context));
            return future.get(socketTimeoutMap.get(source) + connectionTimeoutMap.get(source) + 1, TimeUnit.SECONDS);
        } finally {
            executorService.shutdownNow();
        }
    }

    protected boolean checkOauthRequired(String source) {
        return sourcesConfigMap.getMap(source).containsKey(Constants.OAUTH);
    }

    protected void addHeaders(String source, HttpGet httpGet) {
        ConfigMap headers = sourcesConfigMap.getMap(source).getMap(Constants.REQUEST_HEADERS, null);
        if (headers == null) {
            return;
        }

        for (String headerKey : headers.keySet()) {
            httpGet.addHeader(headerKey, headers.getString(headerKey));
        }
    }

    protected boolean checkProxyRequired(String source) {
        return sourcesConfigMap.getMap(source).getBoolean(Constants.IS_PROXY_REQUIRED, false);
    }

    private RequestConfig.Builder getCustomRequestConfigBuilder(String source) {
        return RequestConfig.custom().setConnectTimeout(connectionTimeoutMap.get(source) * 1000)
                .setSocketTimeout(socketTimeoutMap.get(source) * 1000).setRedirectsEnabled(false)
                .setRelativeRedirectsAllowed(false)
                .setConnectionRequestTimeout(connectionTimeoutMap.get(source) * 1000);
    }

    protected HttpClientContext getContextWithProxy(String urlStr, String source) throws Exception {

        String proxyResponse = pxProxyJson; // going to pxproxy server by default
        if (!configMap.getBoolean("use_proxy_service", false) || urlStr.contains("https://")) {
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
                proxyJson.get("port").asInt(), urlStr));

        RequestConfig config = getCustomRequestConfigBuilder(source).setProxy(proxy).build();
        localContext.setRequestConfig(config);
        return localContext;
    }

    protected String getLocalProxyInfo(String url) throws Exception {
        // Currently url would not be used
        String proxy = proxies.get(ThreadLocalRandom.current().nextInt(0, proxies.size()));
        if (!StringUtils.isBlank(proxy)) {
            String[] proxyData = proxy.split(":");
            if (proxyData.length == 4) {
                Map<String, String> proxyMap = new HashMap<>();
                proxyMap.put("proxy", proxyData[0]);
                proxyMap.put("port", proxyData[1]);
                proxyMap.put("user", proxyData[2]);
                proxyMap.put("pw", proxyData[3]);

                return MAPPER.writeValueAsString(proxyMap);
            }
        }

        throw new Exception("No proxy available");
    }

    public final class FetchPageContentWorker implements Callable<String> {
        HttpGet httpGet;
        HttpClientContext context;

        public FetchPageContentWorker(HttpGet httpGet, HttpClientContext context) {
            this.httpGet = httpGet;
            this.context = context;
        }

        @Override
        public String call() throws Exception {
            // Send http GET request through proxy without following redirects
            CloseableHttpResponse response = null;
            HttpEntity entity = null;
            try {
                response = httpClient.execute(httpGet, context);

                if (response.getStatusLine() == null) {
                    throw new IOException("response.getStatusLine() is null");
                }

                int status = response.getStatusLine().getStatusCode();
                String responseContent = null;

                entity = response.getEntity();
                if (entity != null) {
                    responseContent = EntityUtils.toString(entity);
                }

                return responseContent;
            } finally {
                if (entity != null) {
                    EntityUtils.consume(entity);
                }

                if (response != null) {
                    response.close();
                }

                if (httpGet != null) {
                    httpGet.releaseConnection();
                }
            }
        }
    }
}
