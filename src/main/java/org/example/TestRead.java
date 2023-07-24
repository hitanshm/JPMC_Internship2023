package org.example;

import com.datastax.driver.core.Row;

public class TestRead {

    Row row;
    public TestRead(Row r) {
        row = r;
    }
}