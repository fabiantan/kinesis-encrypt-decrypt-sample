package com.amazonaws.services.kinesis.consumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;
import java.util.UUID;
import java.util.Enumeration;

import com.amazonaws.auth.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;


public final class KMSKinesisApplication {
    

    private static KinesisClientLibConfiguration kinesisClientLibConfiguration;
    private static final Log LOG = LogFactory.getLog(KMSKinesisApplication.class);

    private static String applicationName;
    private static String streamName;
    private static InitialPositionInStream initialPositionInStream;
    private static String regionName;


    private KMSKinesisApplication() {
        super();
    }
    
    /**
* @param args Property file with config overrides (e.g. application name, stream name)
* @throws IOException Thrown if we can't read properties from the specified properties file
*/
    public static void main(String[] args) throws IOException {
        String propertiesFile = "application.properties";


        configure(propertiesFile);
        
        System.out.println("Starting " + applicationName);
        LOG.info("Running " + applicationName + " to process stream " + streamName);
        
        
        IRecordProcessorFactory recordProcessorFactory = new KMSRecordProcessorFactory();
        Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);

        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            LOG.error("Caught throwable while processing data.", t);
            exitCode = 1;
        }
        System.exit(exitCode);
    }
    
    private static void configure(String propertiesFile) throws IOException {
        System.out.println(propertiesFile);
        
        if (propertiesFile != null) {
            loadProperties(propertiesFile);
        } else {
            LOG.info("No properties file provided. Exiting...");
            System.exit(1);
        }
        
        // ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl" , "60");
        
        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        LOG.info("Using workerId: " + workerId);
       
        // Get credentials from IMDS. If unsuccessful, get them from the classpath.
        AWSCredentialsProvider credentialsProvider = null;
        try {
            credentialsProvider = new InstanceProfileCredentialsProvider();
            // Verify we can fetch credentials from the provider
            credentialsProvider.getCredentials();
            LOG.info("Obtained credentials from the IMDS.");
        } catch (AmazonClientException e) {
            LOG.info("Unable to obtain credentials from the IMDS, trying classpath properties", e);
            credentialsProvider = new DefaultAWSCredentialsProviderChain();
            // Verify we can fetch credentials from the provider
            credentialsProvider.getCredentials();
            LOG.info("Obtained credentials from the properties file.");
        }
        
        LOG.info("Using credentials with access key id: " + credentialsProvider.getCredentials().getAWSAccessKeyId());
        
        kinesisClientLibConfiguration = new KinesisClientLibConfiguration(applicationName, streamName,
         credentialsProvider, workerId).withInitialPositionInStream(initialPositionInStream).withRegionName(regionName);
    }

    /**
* @param propertiesFile
* @throws IOException Thrown when we run into issues reading properties
*/
    private static void loadProperties(String propertiesFile) throws IOException {

        FileInputStream inputStream = new FileInputStream(propertiesFile);
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
        } finally {
            inputStream.close();
        }

        Enumeration<?> propNames = properties.propertyNames();
        while (propNames.hasMoreElements()) {
            String key = propNames.nextElement().toString();


            if (key.equals("streamName")) {
                streamName = properties.getProperty(key);
            } else if (key.equals("applicationName")) {
                applicationName = properties.getProperty(key);
            } else if (key.equals("initialPositionInStream")) {
                initialPositionInStream = InitialPositionInStream.valueOf(properties.getProperty(key));
            } else if (key.equals("region")) {
                regionName = properties.getProperty(key);
            }

        }


    }

    public static String getRegionName() {
        return regionName;
    }

}
