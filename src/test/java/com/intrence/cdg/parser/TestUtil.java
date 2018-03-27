package com.intrence.cdg.parser;

import com.intrence.cdg.net.FetchRequest;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.joda.time.DateTime;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class TestUtil {
    
    private static DateTime dateTime = DateTime.now();
    private static final String TEST_PAGE_POSTFIX = "_test_page.html";

    public static Parser getParser(String source, String url) {
        try {
            FetchRequest request = new FetchRequest(url, 2);
            return ParserFactory.createParser(source, getTestHtmlResponse(source), request);
        } catch (Exception e) {
            throw new RuntimeException("parser or test html page not found " + e.getMessage());
        }
    }

    public static Parser getPostFetchRequestParser(String source, String url, String body) {
        try {
            FetchRequest request = new FetchRequest(url, 1, HttpPost.METHOD_NAME, body);
            return ParserFactory.createParser(source, getTestHtmlResponse(source), request);
        } catch (Exception e) {
            throw new RuntimeException("parser or test html page not found " + e.getMessage());
        }
    }

    public static Parser getParser(String source, String url, Map<String, String> inputParamters) {
        try {
            FetchRequest request = new FetchRequest(url, 2, HttpGet.METHOD_NAME, null, inputParamters);
            return ParserFactory.createParser(source, getTestHtmlResponse(source), request);
        } catch (Exception e) {
            throw new RuntimeException("parser or test html page not found " + e.getMessage());
        }
    }

    public static Parser getParser(String source, String url, String fileNamePrefix) {
        try {
            FetchRequest request = new FetchRequest(url, 2);
            return ParserFactory.createParser(source, getTestHtmlResponse(fileNamePrefix), request);
        } catch (Exception e) {
            throw new RuntimeException("parser or test html page not found " + e.getMessage());
        }
    }

    public static String getTestHtmlResponse(String source) throws Exception{
        URL urlResource = TestUtil.class.getClassLoader().getResource(source + TEST_PAGE_POSTFIX);
        return new String(Files.readAllBytes(Paths.get(urlResource.toURI())));
    }

}
