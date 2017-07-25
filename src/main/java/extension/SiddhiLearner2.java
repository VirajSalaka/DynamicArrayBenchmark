package extension;

/**
 * Created by viraj on 7/18/17.
 */
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

public class SiddhiLearner2 {
    private InputHandler inputHandler;
    private String query;
    private String inStreamDefinition;
    private SiddhiManager siddhiManager;
    private ExecutionPlanRuntime executionPlanRuntime;
    public static boolean isReCalculated;
    private static int calculatedSize;

    private static SiddhiLearner2 siddhiLearner;


    public static synchronized SiddhiLearner2 getSiddhiLearner(){
        if(siddhiLearner==null){
            siddhiLearner = new SiddhiLearner2();
        }
        return siddhiLearner;
    }

    private SiddhiLearner2() {

        inStreamDefinition = "define stream inputStream (arrSize int, countArray int); " +
                "\n" +
                "define stream outputStream (calculatedSize long);";

        query = "\n" +
                "@info(name = 'query1') " + "from inputStream " +
                "\n" +
                "select sum(arrSize)/sum(countArray) as calculatedSize " +
                "\n" +
                "insert into outputStream";

        this.siddhiManager = new SiddhiManager();

        executionPlanRuntime = this.siddhiManager.
                createExecutionPlanRuntime(inStreamDefinition + query);

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
