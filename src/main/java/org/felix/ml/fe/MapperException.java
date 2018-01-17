package org.felix.ml.fe;

/**
 *
 * 4
 */
public class MapperException extends Exception {
    /**
     *
     */
    private static final long serialVersionUID = 7626747174461370441L;
    private String countName;

    public MapperException(String countName) {
        this.countName = countName;
    }

    public MapperException(String countName, String message, Throwable cause) {
        super(message, cause);
        this.countName = countName;
    }

    public String getCountName() {
        return countName;
    }
}
