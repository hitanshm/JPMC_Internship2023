package org.example;

import org.example.Testing;

public class MultithreadThing implements Runnable{
    private int threadNumber;

    public MultithreadThing(int i) {
        this.threadNumber = i;
    }


    @Override
    public void run(){
        /*

        for (int i = 0; i<Testing.amtOfTables; i++){
            System.out.println("Thread exists for table " + Testing.table_names.get(i));

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }

         */



    }
}
