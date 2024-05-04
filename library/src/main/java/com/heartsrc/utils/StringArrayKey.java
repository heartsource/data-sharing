package com.heartsrc.utils;

import java.io.Serializable;
import java.util.Arrays;

/**
 * This class encapsulates a String[] in a manner they values in the
 * String[] can by used as the key in a Map by implementing the equals
 * and hashCode methods.  The String[] order IS used to determine equal.
 * So if the same values are in two arrays but different order they are
 * considered the same key (or equivalent)
 */
public class StringArrayKey implements Serializable {
    private String[] innerArray = null;
    private int hashCode = 33;
    public StringArrayKey(String[] array) {
        setArray(array);
    }

    /**
     * Cloned copy of internal String[] in the key.  Cloned to ensure
     * correct data encapsulation so the caller can not reset the values
     * in the inner String[]
     */
    public String[] getArray() {
        String[] cloned = null;
        if (innerArray != null) {
            if (innerArray.length > 0)
                cloned = Arrays.copyOf(innerArray, innerArray.length);
            else
                cloned = new String[0];
        }
        return cloned;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof StringArrayKey ) {
            StringArrayKey othKey = (StringArrayKey)obj;
            if ((innerArray == null) || (othKey.innerArray == null)) {
                if ((innerArray == null) && (othKey.innerArray == null)) {
                    return true;
                }
            } else {
                return Arrays.equals(innerArray, othKey.innerArray);
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
    private void setArray(String[] array) {
        this.innerArray = array;
        buildHashCode(array);
    }

    private void buildHashCode(String[] array) {
        String string = (array != null) ? Arrays.toString(array):"";
        hashCode = string.hashCode();
    }
}
