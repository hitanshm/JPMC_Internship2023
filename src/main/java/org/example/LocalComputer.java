package org.example;

import com.datastax.driver.core.Row;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class LocalComputer {
    public static void writefile(String name, List<Row> input){
        try {
            FileWriter myWriter = new FileWriter(name);
            myWriter.write(input.toString());
            myWriter.close();
            System.out.println("Successfully wrote to the file.");
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
}
