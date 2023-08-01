package org.example;

import com.datastax.driver.core.Row;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Json {
    public String RowsToJson(List<Row> rows, List<String> columns) {
        List<String> json = new ArrayList<>();
        for (Row row : rows) {
            HashMap<String, Object> map = new HashMap<>();
            for (String column : columns) {
                map.put(column, row.getObject(column));
            }
            Gson gson = new Gson();
            Type typeObject = new TypeToken<HashMap>() {}.getType();
            String gsonData = gson.toJson(map, typeObject);
            json.add(gsonData);
        }
        return json.toString();
    }
}
