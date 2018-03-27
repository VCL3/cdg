package com.intrence.cdg.net.header;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.intrence.config.collection.ConfigMap;
import com.intrence.cdg.exception.CdgBackendException;
import com.intrence.cdg.net.FetchRequest;
import com.intrence.cdg.net.RequestResponse;
import com.intrence.cdg.net.WebFetcher;
import com.intrence.cdg.util.Constants;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.Calendar;

public class FarfetchHeaderCreator implements HeaderCreator {

    private static final Logger LOGGER = Logger.getLogger(FarfetchHeaderCreator.class);

    private static final String DYNAMIC_HEADERS = "dynamic_headers";
    private static final String CLIENT_ID = "client_id";
    public static final String BRAND = "brand";
    public static final String SCOPE = "scope";
    public static final String AUTH_URL = "auth_url";
    public static final String SESSION_HANDLE_KEY = "/session_handle";
    public static final String ACCESS_TOKEN_KEY = "access_token";
    public static final String TOKEN_EXPIRE_TIME_KEY = "token_expire_time";
    public static final String BEARER = "Bearer ";

    private String clientId;
    private String brand;
    private String scope;
    private String authURL;

    private String accessToken;
    private long tokenExpireTime;

    private WebFetcher webFetcher;

    public FarfetchHeaderCreator(ConfigMap sourcesConfigMap, WebFetcher webFetcher) {
        if (sourcesConfigMap == null || sourcesConfigMap.size() == 0 || sourcesConfigMap.getMap(Constants.FARFETCH_ID, null) == null) {
            throw new IllegalArgumentException("ConfigMap for grubhub can't be null or empty !!");
        }
        ConfigMap authenticationMap = sourcesConfigMap.getMap(Constants.FARFETCH_ID).getMap(DYNAMIC_HEADERS);
        clientId = authenticationMap.getString(CLIENT_ID);
        brand = authenticationMap.getString(BRAND);
        scope = authenticationMap.getString(SCOPE);
        authURL = authenticationMap.getString(AUTH_URL);
        this.webFetcher = webFetcher;
        if (StringUtils.isBlank(authURL) || StringUtils.isBlank(clientId)) {
            throw new IllegalArgumentException("refresh token or client id can't be null or empty !!");
        }
    }

    @Override
    public void addDynamicHeader(String source, RequestBuilder requestBuilder) throws Exception{
        if(requestBuilder.getUri().toString().equals(authURL))
            return;
        if (tokenExpireTime < Calendar.getInstance().getTimeInMillis() || accessToken == null) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(BRAND, brand);
            jsonObject.put(SCOPE, scope);
            jsonObject.put(CLIENT_ID, clientId);
            FetchRequest fetchRequest = new FetchRequest(authURL, 1, HttpPost.METHOD_NAME, jsonObject.toString());
            RequestResponse response = webFetcher.getResponse(source, fetchRequest);

            ObjectMapper MAPPER = (new ObjectMapper()).registerModule(new JodaModule());
            JsonNode root = MAPPER.readTree(response.getResponse());
            if (root != null) {
                JsonNode dataNode = root.at(SESSION_HANDLE_KEY);
                accessToken = dataNode.get(ACCESS_TOKEN_KEY).textValue();
                tokenExpireTime = dataNode.get(TOKEN_EXPIRE_TIME_KEY).asLong();
                LOGGER.debug(String.format("Created token = %s for source = %s with expiry = %s", accessToken, source, Long.toString(tokenExpireTime)));
            }
        }
            if (accessToken == null) {
                throw new CdgBackendException("Not able to create access token for grubhub");
            }
        requestBuilder.addHeader(HttpHeaders.AUTHORIZATION, BEARER + accessToken);
    }
}
