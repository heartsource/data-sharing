package com.heartsrc.data;

import java.io.Serializable;
import java.util.Arrays;

public class StringArrayKey implements Serializable {
    private String[] innerArray = null;
    private int hashCode = 33;
    public StringArrayKey(String[] array) {
        setArray(array);
    }
    public String[] getArray() {
        return innerArray;
    }

    public void setArray(String[] array) {
        this.innerArray = array;
        buildHashCode(array);
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
    private void buildHashCode(String[] array) {
        String string = (array != null) ? Arrays.toString(array):"";
        hashCode = string.hashCode();
    }
}
