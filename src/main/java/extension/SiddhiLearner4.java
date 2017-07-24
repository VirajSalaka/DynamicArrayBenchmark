package extension;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

/**
 * Created by viraj on 7/14/17.
 */
public class SiddhiLearner4 {
    private InputHandler sumInputHandler;
    private InputHandler countInputHandler;
    private String query;
    private String inStreamDefinition;
    private SiddhiManager siddhiManager;
    private ExecutionPlanRuntime executionPlanRuntime;
    public static boolean isReCalculated;
    private static int calculatedSize;
    //private long count =0;

    private static SiddhiLearner4 siddhiLearner;

    public static synchronized SiddhiLearner4 getSiddhiLearner(){
        if(siddhiLearner==null){
            siddhiLearner = new SiddhiLearner4();
        }
        return siddhiLearner;
    }

    private SiddhiLearner4(){
        inStreamDefinition = "define stream inputStream (arrSize int, countArray int); " +
                "\n" +
                "define stream outputStream (maxSize int);";

        query = "\n" +
                "@info(name = 'query1') " + "from inputStream#window.lengthBatch(50) " +
                "\n" +
                "select max(arrSize) as calculatedSize " +
                "\n" +
                "insert into outputStream";

        this.siddhiManager = new SiddhiManager();

        executionPlanRuntime = this.siddhiManager.
                createExecutionPlanRuntime(inStreamDefinition + query);


        //executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        sumInputHandler = executionPlanRuntime.getInputHandler("sumInputStream");
        countInputHandler = executionPlanRuntime.getInputHandler("countInputStream");

        initCallback();
        executionPlanRuntime.start();
    }


    private void initCallback() {
        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                for (Event ev : events) {

                    int calSize = (int) Math.round(Long.parseLong(ev.getData()[0].toString()));
                    calSize = 1000;
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

    public void arraySizePublish(Object[] obj) {
        try {
            sumInputHandler.send(obj);
        } catch (InterruptedException e) {

        }

    }

    public void countPublish(Object[] obj){
        try {
            countInputHandler.send(obj);
        } catch (InterruptedException e) {
        }
    }

    public int getCalculatedSize() {
        return calculatedSize;
    }

}
