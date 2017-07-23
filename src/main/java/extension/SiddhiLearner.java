
/** Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/


package extension;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

public class SiddhiLearner {
    private InputHandler inputHandler;
    private String query;
    private String inStreamDefinition;
    private SiddhiManager siddhiManager;
    private ExecutionPlanRuntime executionPlanRuntime;
    public static boolean isReCalculated;
    private static int calculatedSize;

    private static SiddhiLearner siddhiLearner;
    private long count;

    public static synchronized SiddhiLearner getSiddhiLearner(){
        if(siddhiLearner==null){
            siddhiLearner = new SiddhiLearner();
        }
        return siddhiLearner;
    }

    private SiddhiLearner() {

        inStreamDefinition = "define stream inputStream (arrSize int); " +
                "\n" +
                "define stream outputStream (calculatedSize long);";

        query = "\n" +
                "@info(name = 'query1') " + "from inputStream " +
                "\n" +
                "select sum(arrSize) as calculatedSize " +
                "\n" +
                "insert into outputStream";

        this.siddhiManager = new SiddhiManager();

        executionPlanRuntime = this.siddhiManager.
                createExecutionPlanRuntime(inStreamDefinition + query);


        //executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        initCallback();
        executionPlanRuntime.start();

    }

    private void initCallback() {
        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                for (Event ev : events) {

                    long calSize =  Math.round(Long.parseLong(ev.getData()[0].toString()));
                    isReCalculated = true;
                    if (calSize == 0) {
                        calculatedSize = 1;
                    } else {
                        calculatedSize = (int) (calSize/count);
                    }

                }
            }
        });
    }

    public void publish(Object[] obj) {
        try {
            inputHandler.send(obj);
        } catch (InterruptedException e) {

        }

    }

    public int getCalculatedSize() {
        return calculatedSize;
    }

    public void increment(){
        count++;
    }
}
