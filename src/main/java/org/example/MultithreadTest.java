package org.example;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;

public class MultithreadTest {

    static MultithreadTest threadTester = new MultithreadTest();

    int batchSize = 2;
    //int numTables = Testing.getAmtOfTables;

    public static void main(String[] args) throws Exception {
        threadTester.doThing();


    }
    //public static int batchSize = Testing.amtOfTables;


    public void doThing() throws Exception{
        ExecutorService executorService = Executors.newFixedThreadPool(batchSize);
        List<Future<String>> resultFutures = new ArrayList<>();

        //Sample Objects to work with
        /*
        List<String> inputStrings = new ArrayList<>();
        inputStrings.add("Itema");
        inputStrings.add("Itemb");

         */

        List<String> tableArray = new ArrayList<>();
        tableArray.add("student");
        tableArray.add("test_table2");

        for (String tableName:tableArray){
            Callable<String> callableTask = () -> this.makeItUpperCaseAndStoreInDB(tableName);
            resultFutures.add(executorService.submit(callableTask));
        }

        List<String> responsesList = new ArrayList<>();
        for (Future<String> future:resultFutures){
            responsesList.add(future.get());
        }
        executorService.shutdown();

        System.out.println("Did any fail?: " + responsesList.stream().anyMatch(response -> response.equals("FAILURE")));

    }

    private String makeItUpperCaseAndStoreInDB(String inputStr){

        //do something

        System.out.println(inputStr);


        return "SUCCESS";
    }
}
