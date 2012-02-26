package org.holidayworking.hadoop.wdpress67;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class InverseComparator extends WritableComparator {

    protected InverseComparator() {
        super(IntWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return - super.compare(a, b);
    }
}
