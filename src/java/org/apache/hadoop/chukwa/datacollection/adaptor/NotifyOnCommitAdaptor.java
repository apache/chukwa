package org.apache.hadoop.chukwa.datacollection.adaptor;


public interface NotifyOnCommitAdaptor extends Adaptor {
    abstract void committed(long commitedByte);
}
