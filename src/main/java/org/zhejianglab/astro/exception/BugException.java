package org.zhejianglab.astro.exception;

public class BugException extends RuntimeException {

  private static final long serialVersionUID = -7034897190745766937L;

  public BugException(String message) {
    super(message);
  }

  public BugException(String message, Exception exception) {
    super(message + (null == exception ? "" : ", caused by: " + exception.getMessage()), exception);
  }
}
