/**
 * Created by wliu on 11/8/17.
 */
package com.intrence.cdg.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.intrence.cdg.task.TaskRule.Status;

import java.util.*;

public class Constants {
    public static final Set<String> HG_SUPPORTED_SOURCES = ImmutableSet.of("tripadvisor", "yelp");
    public static final String SEED_PAGE_KEY = "seed";
    public static final String DEEPLINK = "deeplink";
    public static final String DEAL_PAGE_KEY = "deal";
    public static final String PLACE_PAGE_KEY = "place";
    public static final String CITY_PAGE_KEY = "city";
    public static final String CATEGORY_PAGE_KEY = "category";

    // Currency
    public static final String USD_SYMBOL = "$";
    public static final Set<String> availableCurrencySymbols = new HashSet<>(Arrays.asList(USD_SYMBOL));

    // Time
    public static final String DAYS_KEY = "days";
    public static final String HOURS_KEY = "hours";
    public static final String MINUTES_KEY = "minutes";
    public static final String MAIN_DEAL_KEY = "main_deal";

    // Symbol
    public static final String EMPTY_STRING = "";
    public static final String SPACE = " ";
    public static final String COMMA = ",";
    public static final String DOT = ".";
    public static final String UNDER_SCORE = "_";
    public static final char PIPE = '|';
    public static final String HAT = "^";
    public static final String EQUAL = "=";
    public static final String AMPERSAND = "&";
    public static final String HYPHEN = "-";
    public static final char TAB = '\t';
    public static final char SEMICOLON = ';';
    public static final char QUOTE = '"';
    public static final String FORWARD_SLASH = "/";
    public static final String NEW_LINE = "\n";

    // HTML
    public static final String HTTP = "http";
    public static final String MD5_STRING = "md5";

    public static final String CSV_HEADERS = "headers";
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    public static final String NEXT_URL_KEY = "nextUrl";

    public static final Set<Integer> HTTP_STATUS_CODES_TO_RETRY = new HashSet<>(Arrays.asList(403,407,429,500,502,503,504));
    public static final Set<Integer> HTTP_REDIRECTION_STATUS_CODES = new HashSet<>(Arrays.asList(301,302,303,305,307));


    //
    public static final String BATCHED_REQUESTS = "/batched_requests";
    public static final String SEED_URLS = "/seed_urls";
    public static final String SOURCE = "source";
    public static final String ESCAPED_CRAWLER = "/crawler";
    public static final String TYPE = "type";
    public static final String PAGE_TYPES = "/page_types";
    public static final String ESCAPED_TYPE = "/type";
    public static final String PAGE_TYPE = "/pageType";
    public static final String ESCAPED_PARSER_CLASS = "/parserClass";
    public static final String RULES_BY_URL_PATTERN = "/rulesByUrlPattern";
    public static final String RULES_BY_OFFLINE = "/rulesByOffline";
    public static final String ESCAPED_URL_PATTERN = "/urlPattern";
    public static final String ESCAPED_RULES = "/rules";
    public static final String PARSER_CLASS = "parserClass";
    public static final String RULES = "rules";
    public static final String URL_PATTERN = "urlPattern";
    public static final String EXTRACTOR_CLASS = "extractorClass";
    public static final String CRAWLER = "crawler";
    public static final String ESCAPED_SOURCE = "/source";
    public static final String PARSER = "/parser";
    public static final String BODY = "/body";
    public static final String METHOD_TYPE = "/method_type";

    // Boolean
    public static final String TRUE = "true";
    public static final String FALSE = "false";
    public static final String YES = "yes";
    public static final String NO = "no";

    // Dev environments
    public static final List<String> DEV_ENVIRONMENTS = ImmutableList.of("testing", "development", "integration");

    // Task Type
    public static final String REFRESH_OPERATION = "refresh";
    public static final String SEARCH_OPERATION = "search";
    public static final String FEED_OPERATION = "feed";
    public static final String API_INTEGRATION = "api";
    public static final String CRAWL_INTEGRATION = "crawl";
    public static final String FILE_INTEGRATION = "file";
    public static final String FTP_INTEGRATION = "ftp";
    public static final String DB_INTEGRATION = "db";
    public static final String STREAM_OPERATION = "stream";
    public static final String COMPRESSION_TYPE = "compression";
    public static final String GZIP = "gzip";
    public static final String SOAP_INTEGRATION = "soap";

    // Source keys
    public static final String DOMAIN = "domain";
    public static final String ROBOT_CHECK = "robot_check";
    public static final String FILE_NAMES = "file_names";
    public static final String FILE_PROCESSOR_CLASS = "file_processor_class";
    public static final String SKIP_FILE_PROCESS = "skip_file_process";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String AUTH_KEY = "auth";

    public static final String CONNECTION_TIMEOUT_KEY = "connection_timeout";
    public static final String SOCKET_TIMEOUT_KEY = "socket_timeout";
    public static final String TASK_BUCKET = "task_bucket";

    //Factual Refreshtask Constants
    public static final String LAST_PROCESSED_PLACE_ID = "lastProcessedPlaceId";
    public static final String NO_OF_RECORDS_PROCESSED = "recordsProcessed";
    public static final String NO_OF_RECORDS_PROCESSED_OFFSET = "recordsProcessedOffset";
    public static final String TOTAL_NO_OF_RECORDS_TO_BE_PROCESSED = "totalRecordsToBeProcessed";

    public static final String LIMIT = "limit";
    public static final String COUNTRY = "country";
    public static final String REGION = "region";
    public static final String LOCALITY = "locality";
    public static final String POSTCODE = "postcode";
    public static final String CATEGORY = "category";
    public static final String NAME= "name";

    public static final String FACTUAL_TABLE_PARAM = "factual_table";
    public static final String HIVE_LIMIT_PARAM = "limit";

    public static final String LIMIT_STRING = "limit";
    public static final String OFFSET_STRING = "offset";
    public static final String DEFAULT_OFFSET = "0";
    public static final String DEFAULT_LIMIT = "100";
    public static final String MODIFIER_ID = "modifier_id";
    public static final String USER_ID = "user_id";

    public static final List<String> SUPPORTED_COUNTRIES = Arrays.asList("US","FR","DE","GB","ES");
    public static final String DEFAULT_COUNTRY = "US";
    public static final String SUPPORTED_SOURCES = "sources";
    public static final String SOURCE_OPERATION_TYPES = "search_types";
    public static final String SOURCE_INTEGRATION_TYPE = "source_type";
    public static final String OAUTH = "oauth";
    public static final String IS_PROXY_REQUIRED = "is_proxy_required";

    public static final String URLS_FETCHED_COUNT = "urls_fetched";
    public static final String REQUEST_HEADERS = "headers";
    public static final String POST_REQUEST_HEADERS = "post_headers";
    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";


    public static final Map<Status, Set<Status>> ALLOWED_STATUS_TRANSITIONS= createMap();
    private static Map<Status, Set<Status>> createMap() {
        Map<Status, Set<Status>> statusMap = new HashMap<>();
        statusMap.put(Status.DRAFT,     new HashSet<>(Arrays.asList(Status.DRAFT, Status.RUNNABLE, Status.CANCELLED)));
        statusMap.put(Status.RUNNABLE,  new HashSet<>(Arrays.asList(Status.RUNNABLE, Status.CANCELLED)));
        statusMap.put(Status.RUNNING,   new HashSet<>(Arrays.asList(Status.RUNNING, Status.PAUSED, Status.CANCELLED)));
        statusMap.put(Status.PAUSED,    new HashSet<>(Arrays.asList(Status.PAUSED, Status.RESUME, Status.CANCELLED)));
        statusMap.put(Status.CANCELLED, new HashSet<>(Arrays.asList(Status.CANCELLED, Status.RUNNABLE)));
        statusMap.put(Status.ERROR, new HashSet<>(Arrays.asList(Status.ERROR, Status.RESUME, Status.RUNNABLE, Status.CANCELLED)));
        // As we support recurring rules with intermediate status as "FINISHED" when a run finishes; so we should be able to cancel such rules if needed.
        statusMap.put(Status.FINISHED, new HashSet<>(Arrays.asList(Status.CANCELLED, Status.RUNNABLE, Status.FINISHED)));
        return Collections.unmodifiableMap(statusMap);
    }


    public static final int EXTID_FETCHREQ_PRIORITY = 1;
    public static final int FACTUAL_API_FETCHREQ_PRIORITY = 2;

    //For file related task
    public static final String COMMOM_LOCAL_DOWNLOAD_DIRECTORY = "/var/groupon/dora/";
    public static final String OFFLINE_CRAWL_PREFIX = "offline-crawl:";

    //For Stream related task
    public static final String STREAM = "stream";
    public static final String STREAM_TYPE = "type";
    public static final String STREAM_RECORD_IDENTIFIER = "record_identifier";
    public static final String STREAM_BATCH_SIZE = "batch_size";

    //For request signing
    public static final String API_KEY = "api_key";
    public static final String SECRET = "secret";
    public static final String HMAC_SHA_256 = "HmacSHA256";
    public static final String ASCII = "ASCII";
    public static final String SIGNATURE_KEY = "signature";

    //Source Ids
    public static final String BEST_BUY_ID = "best_buy";
    public static final String FACEBOOK_ID = "facebook";
    public static final String FARFETCH_ID = "farfetch";

    //sources config path
    public static final String CDG_SOURCES_CONFIG_PATH = "com/groupon/merchantdata/config/sources.config.yml";
    public static final String CDG_SOURCES_CONFIG_LOCALHOST_PATH = "com/intrence/config/sources.config.yml";
}
