package com.intrence.cdg.streamer.provider;

import com.google.common.collect.ImmutableSet;
import com.intrence.cdg.util.Constants;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.UnsupportedSchemeException;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public class HTTPStreamProvider extends StreamProvider {

    private HttpUriRequest httpUriRequest;
    private CloseableHttpResponse response;
    private static final ImmutableSet<String> supportedCompressionTypes = new ImmutableSet.Builder<String>()
            .add(Constants.GZIP).build();
    private HttpEntity entity;
    private static final Logger LOGGER = Logger.getLogger(HTTPStreamProvider.class);

    public HTTPStreamProvider(CloseableHttpResponse response, HttpUriRequest httpUriRequest, Integer batchSize, String recordIdentifier, String compressionType) throws Exception {
        super(decompressInputStream(response.getEntity().getContent(), compressionType), batchSize, recordIdentifier);
        entity = response.getEntity();
    }

    public HTTPStreamProvider(CloseableHttpResponse response, HttpUriRequest httpUriRequest, Integer batchSize, String recordIdentifier) throws Exception {
        super(response.getEntity().getContent(), batchSize, recordIdentifier);
        entity = response.getEntity();
    }



    /*
        Closes the opened HTTP connections when streaming is over.
     */
    @Override
    public void close() throws Exception {
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

    private static InputStream decompressInputStream(InputStream inputStream, String compresstionType) throws Exception{
        if(StringUtils.isNotBlank(compresstionType) && supportedCompressionTypes.contains(compresstionType)) {
            try {
                switch (compresstionType.toLowerCase()) {
                    case Constants.GZIP:
                        inputStream = new GZIPInputStream(inputStream);
                    default:
                        break;
                }
            } catch (IOException e) {
                LOGGER.error("IOException while converting InputStream to GzipInputStream", e);
            }
        }
        else {
            throw new UnsupportedSchemeException(String.format("Unsupported CompressionType=%s, supported compressionTypes are = %s", compresstionType, supportedCompressionTypes.toString()));
        }
        return inputStream;
    }
}
