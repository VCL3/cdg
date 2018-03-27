package com.intrence.cdg.streamer;


import com.intrence.cdg.exception.CdgBackendException;
import com.intrence.cdg.streamer.provider.StreamProvider;
import org.apache.commons.lang3.NotImplementedException;

/**
 *      Created by ajijhotiya on 22/11/16.
 *
 *      This is concrete implementation of JSON Streamer. This class contains the logic of reading JSON data as stream
 *      from file.
 */
public class JSONStreamer extends Streamer {

    protected JSONStreamer(StreamProvider streamProvider) throws CdgBackendException {
        super(streamProvider);
    }

    @Override
    public boolean hasNext() throws CdgBackendException {
        throw new NotImplementedException("Not implemented for CSVExtractor");
    }

    @Override
    public String next() throws CdgBackendException {
        throw new NotImplementedException("Not implemented for CSVExtractor");
    }

    @Override
    public void close() throws CdgBackendException {
        throw new NotImplementedException("Not implemented for CSVExtractor");
    }
}
