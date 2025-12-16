package ru.itmo.parallel.mapreduce.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

@Data
public class RevenueKeyWritable implements WritableComparable<RevenueKeyWritable> {

    private double revenue = 0.0;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(revenue);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        revenue = in.readDouble();
    }

    @Override
    public int compareTo(RevenueKeyWritable other) {
        return Double.compare(other.revenue, this.revenue);
    }

    @Override
    public String toString() {
        return String.format("%.2f", revenue);
    }
}

