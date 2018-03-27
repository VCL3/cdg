package com.intrence.cdg.streamer.provider;

import org.apache.commons.lang3.NotImplementedException;

import java.io.InputStream;

/**
 * Created by ajijhotiya on 12/12/16.
 */
public class FileStreamProvider extends StreamProvider {

    protected FileStreamProvider(InputStream inputStream, Integer batchSize, String recordIdentifier) {
        super(inputStream, batchSize, recordIdentifier);
    }

    @Override
    public void close() throws Exception {
        throw new NotImplementedException("Not implemented for CSVExtractor");
    }
}
