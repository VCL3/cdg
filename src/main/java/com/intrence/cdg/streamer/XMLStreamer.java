package com.intrence.cdg.streamer;

import com.intrence.cdg.exception.CdgBackendException;
import com.intrence.cdg.streamer.provider.StreamProvider;
import com.intrence.cdg.util.Constants;
import org.apache.log4j.Logger;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;

/**
 *      Created by ajijhotiya on 22/11/16.
 *
 *      This is concrete implementation of XML Streamer. This class contains the logic of reading XML data as stream
 *      from file.
 */
public class XMLStreamer extends Streamer {

    private static final Logger LOGGER = Logger.getLogger(Streamer.class);

    private XMLStreamReader xmlStreamReader;
    private boolean isReaderClosed;
    private Transformer transformer;

    protected XMLStreamer(StreamProvider streamProvider) throws CdgBackendException {
        super(streamProvider);
        if(recordIdentifier.isEmpty()){
            throw new CdgBackendException("Event=InitXmlStreamer - Record Identifier cannot be null");
        }
        try {
            XMLInputFactory factory = XMLInputFactory.newInstance();
            xmlStreamReader = factory.createXMLStreamReader(inputStream);
            isReaderClosed = false;
            transformer = TransformerFactory.newInstance().newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        } catch (XMLStreamException xe) {
            LOGGER.debug(String.format("Exception=XMLStreamException while creating xml streamer for recordIdentifier=%s", recordIdentifier));
            throw new CdgBackendException("Event=InitXMLStreamer XMLStreamer Initialization failed", xe);
        } catch (TransformerException te) {
            throw new CdgBackendException("Exception=XMLTransformException Failure while converting xml stream to String", te);
        }
        LOGGER.info(String.format("Event=InitXMLStreamer  -  XMLStreamer initialized successfully for batchSize=%s", batchSize)
        );
    }

    @Override
    public boolean hasNext() throws CdgBackendException {
        boolean hasNext = true;
        try {
            if (xmlStreamReader == null || isReaderClosed) {
                hasNext = false;
            } else if (!xmlStreamReader.hasNext()) {
                close();
                hasNext = false;
            }
        }catch (XMLStreamException xe) {
            throw new CdgBackendException("Event=XMLStreamException XMLStreamer parsing failed", xe);
        }
            return hasNext;
    }

    @Override
    public String next() throws CdgBackendException {
        if (xmlStreamReader == null || !hasNext()) {
            return null;
        }
        StringBuilder data = new StringBuilder();
        int size = 0, event = 1;
        try {
            while (hasNext() && size < batchSize) {
                String name = "";
                try {
                    name = xmlStreamReader.getName().toString();
                } catch (Exception ex) {
                    LOGGER.debug("Exception when reading XMlElement from the stream", ex);
                }
                if (!name.equals(recordIdentifier)) {
                    event = xmlStreamReader.next();
                    continue;
                }
                //start copy if it is the start tag of root element (recordIdentifier)
                if (event == XMLStreamReader.START_ELEMENT) {
                    recordCounter++;
                    size++;
                    data.append(getNodeAsString(xmlStreamReader)).append(Constants.NEW_LINE);
                }
            }
        } catch (XMLStreamException xe) {
            throw new CdgBackendException("Event=XMLStreamException XMLStreamer parsing failed while streaming", xe);
        } catch (TransformerException te) {
            throw new CdgBackendException("Exception=XMLTransformException Failure while converting xml stream to String", te);
        }
        return data.toString();
    }

    private String getNodeAsString(XMLStreamReader xmlStreamReader) throws  TransformerException
    {
        StringWriter stringWriter = new StringWriter();
        transformer.transform(new StAXSource(xmlStreamReader), new StreamResult(stringWriter));
        return stringWriter.toString();
    }

    @Override
    public void close() throws CdgBackendException{
        if (xmlStreamReader != null) {
            super.close();
            try {
                xmlStreamReader.close();
            }catch (XMLStreamException xe) {
                throw new CdgBackendException("Event=XMLStreamException Exception while closing xmlStreamReader", xe);
            }
            isReaderClosed = true;
            LOGGER.info(String.format("Event=XMLStreamer  -  XMLStreamer closed successfully for batchSize=%s", batchSize));
        }
    }
}
