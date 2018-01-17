package org.felix.ml.fe;

/**
 *
 * 1
 */
public class ReducerException extends Exception {
    /**
     *
     */
    private static final long serialVersionUID = 8402644465292860167L;
    private String countName;

    public ReducerException(String countName) {
        this.countName = countName;
    }

    public ReducerException(String countName, String message, Throwable cause) {
        super(message, cause);
        this.countName = countName;
    }

    public String getCountName() {
        return countName;
    }
}
