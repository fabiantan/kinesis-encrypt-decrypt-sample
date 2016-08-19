package com.amazonaws.services.kinesis.consumer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

/**
* Used to create new record processors.
*/
public class KMSRecordProcessorFactory implements IRecordProcessorFactory {
    
    /**
* Constructor.
*/
    public KMSRecordProcessorFactory() {
        super();
    }

    /**
* {@inheritDoc}
*/
    @Override
    public IRecordProcessor createProcessor() {
        return new KMSRecordProcessor();
    }

}
