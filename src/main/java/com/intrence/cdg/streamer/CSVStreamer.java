package com.intrence.cdg.streamer;

import com.intrence.cdg.exception.CdgBackendException;
import com.intrence.cdg.streamer.provider.StreamProvider;
import com.intrence.cdg.util.Constants;
import org.apache.log4j.Logger;

import java.util.Scanner;

/**
 *      Created by ajijhotiya on 22/11/16.
 *
 *      This is concrete implementation of CSV Streamer. This class contains the logic of reading csv data as stream
 *      from file.
 *      This class uses Scanner to read data as Stream.
 */
public class CSVStreamer extends Streamer {

    private static final Logger LOGGER = Logger.getLogger(Streamer.class);

    private Scanner scanner;
    private boolean isScannerClosed;

    public CSVStreamer(StreamProvider streamProvider) throws CdgBackendException {
        super(streamProvider);
        scanner = new Scanner(inputStream);
        isScannerClosed = false;
        if (hasNext()) {
            scanner.nextLine();
        }
        LOGGER.info(String.format("Event=InitCSVStreamer  -  CSVStreamer initialized successfully for batchSize=%s", batchSize)
        );
    }

    @Override
    public boolean hasNext() throws CdgBackendException {
        boolean hasNext = true;
        if (scanner == null || isScannerClosed) {
            hasNext = false;
        } else if (!scanner.hasNextLine()) {
            close();
            hasNext = false;
        }
        return hasNext;
    }

    @Override
    public String next() throws CdgBackendException {
        if (scanner == null || !hasNext()) {
            return null;
        }
        StringBuilder data = new StringBuilder();
        int size = 0, prvRecordCounter = recordCounter, prvLineCounter = recordCounter;

        while (hasNext() && size < batchSize) {
            data.append(scanner.nextLine()).append(Constants.NEW_LINE);
            size++;
            lineCounter++;
            recordCounter++;
        }
        logInfo(prvRecordCounter, prvLineCounter);
        return data.toString();
    }

    @Override
    public void close() throws CdgBackendException {
        if (scanner != null) {
            super.close();
            scanner.close();
            isScannerClosed = true;
            LOGGER.info(String.format("Event=CSVStreamer  -  CSVStreamer closed successfully for batchSize=%s", batchSize));
        }
    }
}
