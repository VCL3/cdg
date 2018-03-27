package com.intrence.cdg.exception;

public class ThresholdReachedException extends CdgBackendException {
    private String detailMessage;
    private String identifier;

    public ThresholdReachedException(Throwable t)
    {
        super(t);
    }

    public ThresholdReachedException(String message, Throwable t)
    {
        super(message, t);
    }

    public ThresholdReachedException(String message)
    {
        super(message);
    }

    public ThresholdReachedException(String message, String identifier)
    {
        super(message);
        setIdentifier(identifier);
    }

    public void setMessage(String message)
    {
        this.detailMessage = message;
    }

    public String getMessage()
    {
        if (this.detailMessage == null) {
            return this.identifier + ":" + super.getMessage();
        }
        return this.detailMessage;
    }

    public String getIdentifier()
    {
        return this.identifier;
    }

    public void setIdentifier(String identifier)
    {
        this.identifier = identifier;
    }
}
