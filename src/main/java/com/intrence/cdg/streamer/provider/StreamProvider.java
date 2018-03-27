package com.intrence.cdg.streamer.provider;

import com.intrence.config.ConfigProvider;
import com.intrence.cdg.util.Constants;

import java.io.InputStream;

/**
 * Created by ajijhotiya on 12/12/16.
 */

/*
    StreamProvider wraps the InputStream and batchSize need to be used by Streamer class for reading the data as Stream.
 */
public abstract class StreamProvider {

    private static final int DEFAULT_BATCH_SIZE = ConfigProvider.getConfig().getInteger(Constants.STREAM_BATCH_SIZE);

    private InputStream inputStream;
    private Integer batchSize;
    private String recordIdentifier;

    protected StreamProvider(InputStream inputStream, Integer batchSize, String recordIdentifier) {
        this.inputStream = inputStream;
        if (batchSize == null || batchSize <= 0) {
            this.batchSize = DEFAULT_BATCH_SIZE;
        } else {
            this.batchSize = batchSize;
        }
        this.recordIdentifier = recordIdentifier;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public String getRecordIdentifier() {
        return recordIdentifier;
    }

    public abstract void close() throws Exception;
}
