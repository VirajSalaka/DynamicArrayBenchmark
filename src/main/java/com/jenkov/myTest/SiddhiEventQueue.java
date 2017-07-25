package com.jenkov.myTest;

import extension.SiddhiLearner2;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by viraj on 7/24/17.
 */
public class SiddhiEventQueue {
    private Queue<Object []> sharedDataQueue =  new LinkedList<>();
    private static SiddhiEventQueue siddhiEventQueue;


    public synchronized static SiddhiEventQueue getQueue(){
        if(siddhiEventQueue==null){
            siddhiEventQueue= new SiddhiEventQueue();
        }
        return siddhiEventQueue;
    }

    private SiddhiEventQueue(){

    }

    public void addObjects(Object[] array){
        sharedDataQueue.add(array);
        publishItems();
    }

    ExecutorService executor = Executors.newSingleThreadExecutor();
    public void publishItems(){
        if(!sharedDataQueue.isEmpty()){
            executor.submit(() -> {
                SiddhiLearner2.getSiddhiLearner().publish(sharedDataQueue.poll());
            });
        }


    }

}
