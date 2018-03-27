package com.intrence.cdg.streamer;

import com.intrence.cdg.exception.CdgBackendException;
import com.intrence.cdg.streamer.provider.StreamProvider;
import org.apache.log4j.Logger;

import java.io.InputStream;

/**
 *  Created by ajijhotiya on 22/11/16.
 *
 *  Abstract Class for data Streaming
 */
public abstract class Streamer {

    private static final Logger LOGGER = Logger.getLogger(Streamer.class);

    protected StreamProvider streamProvider;
    protected InputStream inputStream;
    protected String recordIdentifier;
    protected int batchSize;
    protected int lineCounter;
    protected int recordCounter;

    protected Streamer(StreamProvider streamProvider) throws CdgBackendException {
        if (streamProvider == null || streamProvider.getInputStream() == null) {
            throw new CdgBackendException("Event=InitStreamer  -  StreamProvider or Inputstream can't be null !!");
        }
        this.streamProvider = streamProvider;
        this.inputStream = streamProvider.getInputStream();
        this.batchSize = streamProvider.getBatchSize();
        this.recordIdentifier = streamProvider.getRecordIdentifier();
        this.recordCounter = 0;
        this.lineCounter = 0;
    }

    /**
     *
     * @return boolean status, which tells if content is available for reading by Streamer.
     */
    public abstract boolean hasNext() throws CdgBackendException;

    /**
     *
     * @return Content read by Streamer.
     */
    public abstract String next() throws CdgBackendException;

    /**
     *
     * @return Closes the StreamProvider.
     */
    public void close() throws CdgBackendException {
        try {
            if (streamProvider != null) {
                streamProvider.close();
            }
        } catch (Exception e) {
            LOGGER.error(String.format("Event=CloseStreamProvider  -  Exception in closing StreamProvider"), e);
        }
    };

    protected void logInfo(int prvRecordCounter, int prvLineCounter) {
        LOGGER.info(String.format("Event=ReadStreamer  -  Reading lines from [%s to %s] and reading records from [%s to %s]",
                prvLineCounter, lineCounter, prvRecordCounter, recordCounter)
        );
    }

}
