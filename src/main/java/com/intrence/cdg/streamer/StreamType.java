package com.intrence.cdg.streamer;


import com.intrence.cdg.exception.CdgBackendException;
import com.intrence.cdg.streamer.provider.StreamProvider;

/**
 * Created by ajijhotiya on 16/12/16.
 */
public enum StreamType {

    CSV("csv", CSVStreamer.class) {
        @Override
        public Streamer getStreamerInstance(StreamProvider streamProvider) throws CdgBackendException {
            return new CSVStreamer(streamProvider);
        }
    },
    XML("xml", XMLStreamer.class) {
        @Override
        public Streamer getStreamerInstance(StreamProvider streamProvider) throws CdgBackendException {
            return new XMLStreamer(streamProvider);
        }
    },
    JSON("json", JSONStreamer.class) {
        @Override
        public Streamer getStreamerInstance(StreamProvider streamProvider) throws CdgBackendException {
            return new JSONStreamer(streamProvider);
        }
    };

    private String streamType;
    private Class<? extends Streamer> streamImplClass;

    StreamType(String streamType, Class<? extends Streamer> streamImplClass) {
        this.streamType = streamType;
        this.streamImplClass = streamImplClass;
    }

    public abstract Streamer getStreamerInstance(StreamProvider streamProvider) throws CdgBackendException;

    public static StreamType fromString(String streamType) {
        if (streamType != null) {
            for (StreamType streamValue : StreamType.values()) {
                if (streamType.equalsIgnoreCase(streamValue.streamType)) {
                    return streamValue;
                }
            }
        }
        throw new IllegalArgumentException(String.format("Invalid streamer type string %s", streamType));
    }

}
