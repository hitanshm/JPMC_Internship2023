/*package org.example;

import com.google.gson.Gson;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import javax.xml.validation.Schema;
import java.io.File;
import java.io.IOException;
import java.util.TreeMap;

public class AvroToParquetConvertor {
    public static void main(String []args){
        System.out.println("Test");
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            TestOutPutData data =objectMapper.readValue(new File("C:\\JPMC project\\accountdetails.txt"),
            TestOutPutData.class);
            System.out.println("Value Stringified : "+objectMapper.writeValueAsString(data.getMlist().get(0)));
            Schema schema =parseParquet(sl);
            toConvertParquet(data, schema);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static Schema parseParquet(String json){
        TreeMap<String>, Object> properties = new TreeMap<>();
        Gson gson = new Gson();
        String schema = "{\"type\" : \"record\",\"namespace\":\"
    }
}*/
