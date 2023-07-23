package org.example;

import com.datastax.driver.core.Row;

public class GenericRead {

    Row row;
    public GenericRead(Row r) {
        row = r;
    }
}
