package com.jenkov.myTest;

import extension.SiddhiLearner2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.StringJoiner;

/**
 * Created by viraj on 7/19/17.
 */
public class MyArray{
    protected static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
    public static int DEFAULT_ARRAY_SIZE = 1001;

    protected int size = 0;

    private long[] values;

    private static final Logger logger = LoggerFactory.getLogger(MyArray.class);



    // Private methods

    public MyArray(){

//        if(SiddhiLearner2.isReCalculated){
//            DEFAULT_ARRAY_SIZE = SiddhiLearner2.getSiddhiLearner().getCalculatedSize()+1;
//            SiddhiLearner2.isReCalculated = false;
//
//        }

        values = new long[DEFAULT_ARRAY_SIZE];
    }

    protected void prepareForAdd(long index, int currentArraySize) {
        int intIndex = (int) index;
        rangeCheck(index, size);
        ensureCapacity(intIndex + 1, currentArraySize);
        resetSize(intIndex);
    }

    protected void resetSize(int index) {
        if (index >= size) {
//            if(size == 0){
//                SiddhiLearner2.getSiddhiLearner().publish(new Object[]{index+1,1});
//            }else{
//                SiddhiLearner2.getSiddhiLearner().publish(new Object[]{index+1-size,0});
//            }

            //SiddhiLearnerUniqueTimeWin.getSiddhiLearner().arraySizePublish(new Object[]{(index+1-size)});
            size = index + 1;
        }
    }

    protected void rangeCheck(long index, int size) {
        if (index > MAX_ARRAY_SIZE || index < Integer.MIN_VALUE) {
            System.out.println("out of range Error");
        }

        if ((int) index < 0) {
            System.out.println(" no minus value ");
        }
    }
//
//    protected void rangeCheckForGet(long index, int size) {
//        rangeCheck(index, size);
//        if (index < 0 || index >= size) {
//            throw BLangExceptionHelper.getRuntimeException(
//                    RuntimeErrors.ARRAY_INDEX_OUT_OF_RANGE, index, size);
//        }
//    }

    protected void ensureCapacity(int requestedCapacity, int currentArraySize) {
        if ((requestedCapacity) - currentArraySize >= 0) {
            // Here the growth rate is 1.5. This value has been used by many other languages
            int newArraySize = currentArraySize + (currentArraySize >> 1);

            // Now get the maximum value of the calculate new array size and request capacity
            newArraySize = Math.max(newArraySize, requestedCapacity);

            //SiddhiLearner.getSiddhiLearner().publish(new Object[]{requestedCapacity-size});

            //SiddhiLearnerUniqueTimeWin.getSiddhiLearner().arraySizePublish(new Object[]{(requestedCapacity-size)});

            // Now get the minimum value of new array size and maximum array size
            newArraySize = Math.min(newArraySize, MAX_ARRAY_SIZE);
            grow(newArraySize);
        }
    }

    public long size() {
        return size;
    }

    public void grow(int newLength) {
        values = Arrays.copyOf(values, newLength);
    }

    public String stringValue() {
        StringJoiner sj = new StringJoiner(",", "[", "]");
        for (int i = 0; i < size; i++) {
            sj.add(Long.toString(values[i]));
        }
        return sj.toString();
    }

    public void add(long index, long value) {
        prepareForAdd(index, values.length);
        values[(int) index] = value;
    }
}
