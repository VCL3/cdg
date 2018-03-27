package com.intrence.cdg.util;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class FormatUtils {

    private static final String SIMPLE_DATE_FORMAT            = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final String CURRENT_DATE_FORMAT_IN_NUMBER = "yyyyMMddHHmm00";

    public static String formatDates(String dateString, String format) {
        if (StringUtils.isBlank(dateString) || StringUtils.isBlank(format)) {
            return dateString;
        }
        DateTimeFormatter toDateFormat = DateTimeFormat.forPattern(format);
        DateTimeFormatter fromDateFormat = DateTimeFormat.forPattern(Constants.DEFAULT_DATE_FORMAT);
        DateTime dateTime = new DateTime(fromDateFormat.parseDateTime(dateString));
        return toDateFormat.print(dateTime);
    }

    public static String md5DigestCreator(String data) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        String digest = null;
        try {
            if (StringUtils.isNotBlank(data)) {
                MessageDigest md5 = MessageDigest.getInstance(Constants.MD5_STRING);
                byte[] bytesOfMessage = data.getBytes(Constants.ASCII);
                md5.update(bytesOfMessage);
                digest = new String(Hex.encodeHex(md5.digest()));
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return digest;
    }

    /**
     * Method to generate current time in string
     */
    public static String getCurrentTime() {
        String data;
        Calendar c = Calendar.getInstance();
        SimpleDateFormat df = new SimpleDateFormat(SIMPLE_DATE_FORMAT);
        data = df.format(c.getTime());
        return data;
    }

    /**
     * Method to generate current time in number
     */
    public static String getCurrentTimeAsNumber() {

        DateFormat simpleDateFormat = new SimpleDateFormat(CURRENT_DATE_FORMAT_IN_NUMBER);
        Date now = new Date();
        return simpleDateFormat.format(now);

    }

    public static String convertHttpToHttps(String url) {
        if (url.contains("http") && !url.contains("https")) {
            url = url.replace("http", "https");
        }
        return url;
    }

}
