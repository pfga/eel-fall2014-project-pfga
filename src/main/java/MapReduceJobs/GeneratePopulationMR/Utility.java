package FuzzySet;

import java.util.Arrays;
import java.util.List;


/**
 * This class contains the utility methods to be used.
 * @author ankit
 *
 */
public class Utility {

    public static String hash = "#";
    public static String comma = ",";
    public static String EMPTY_STRING = "";

    /**
     * It converts a comma separated string to list of strings.
     * @param str
     * @return
     */
    public static List<String> commaSeperatedToList(String str) {

        return Arrays.asList(str.split("\\s*,\\s*"));
    }

    /**
     * This checks any String whether its emtpty.
     *
     * @param stringObject
     * @return boolean
     */

    protected static boolean isEmpty(String stringObject) {
        boolean returnCode = true;

        if ((stringObject != null) && (stringObject.trim().length() > 0)) {
            returnCode = false;
        }

        return returnCode;
    }
}
