/**
 * Created by wliu on 11/16/17.
 */
package com.intrence.cdg.exception;

public class InternalServerException extends RuntimeException {
    public InternalServerException(String s) {
        super(s);
    }

    public InternalServerException(Throwable throwable) {
        super(throwable);
    }

    public InternalServerException(String s, Throwable throwable) {
        super(s, throwable);
    }
}