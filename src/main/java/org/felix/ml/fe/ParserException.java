package org.felix.ml.fe;

/**
 *
 *
 */
public class ParserException extends Exception {
    public static final int TYPE_EMPTY_VALUE = 1;
    public static final int TYPE_REPEAT = 2;
    public static final int TYPE_INVALID_VALUE = 3;
    /**
     *
     */
    private static final long serialVersionUID = 7453377324745810989L;
    private int type;
    private String key;

    public ParserException(int type, String message) {
        super(message);
        this.type = type;
    }

    public ParserException(int type, String key, String message) {
        super(message);
        this.type = type;
        this.key = key;
    }

    public ParserException(String field, Throwable cause) {
        super(cause);
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public String getTypeStr() {
        if (type == TYPE_EMPTY_VALUE)
            return "error_parser_emptye_value";
        else if (type == TYPE_REPEAT)
            return "error_parser_repeat_key";
        else
            return "error_parser_" + type;
    }
}
