package com.heartsrc.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StringArrayKeyTest {
    @Test
    public void testCloning() {
        String[] ary = {"a","b","c"};
        Assertions.assertEquals(ary, ary);  // Just playing safe in case assertEquals does not check for same instance
        StringArrayKey key = new StringArrayKey(ary);
        Assertions.assertNotEquals(ary, key.getArray());
        Assertions.assertArrayEquals(ary, key.getArray());
    }
}
