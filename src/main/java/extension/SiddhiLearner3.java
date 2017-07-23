package extension;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

/**
 * Created by viraj on 7/19/17.
 */
public class SiddhiLearner3 {
    private InputHandler inputHandler;
    private String query;
    private String query2;
    private String inStreamDefinition;
    private SiddhiManager siddhiManager;
    private ExecutionPlanRuntime executionPlanRuntime;
    public static boolean isReCalculated;
    private static int calculatedSize;

    private static SiddhiLearner3 siddhiLearner;


    public static synchronized SiddhiLearner3 getSiddhiLearner(){
        if(siddhiLearner==null){
            siddhiLearner = new SiddhiLearner3();
        }
        return siddhiLearner;
    }

    private SiddhiLearner3() {

        inStreamDefinition = "define stream inputStream (arrSize int); " +
                "\n" +
                "define stream maxOutputStream (maxSize int, maxOccurrance int);"+
                 "\n" +
                "define stream countOutputStream (maxSize int);";

        query = "\n" +
                "@info(name = 'query1') " + "from inputStream#window.lengthBatch(500) " +
                "\n" +
                "select max(arrSize) as maxSize, count(arrSize) as arrayCount " +
                "\n" +
                "insert into maxOutputStream";

        query2= "\n" +
                "@info(name = 'query2') " + "from inputStream#window.lengthBatch(500) " +
                "\n" +
                "select  " +
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
                        calculatedSize = (int) (calSize);
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
}
