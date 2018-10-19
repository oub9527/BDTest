package com.nd.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author: oubin
 * @date: 2018/10/17 15:33
 * @Description:
 */
public class WordCountSort extends WritableComparator {

    public WordCountSort() {
        super(Text.class, true);

    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        return -(a.toString().compareTo(b.toString()));
    }
}
