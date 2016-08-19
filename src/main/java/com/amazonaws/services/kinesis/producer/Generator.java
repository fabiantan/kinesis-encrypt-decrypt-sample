package com.amazonaws.services.kinesis.producer;


import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.kms.DataKey;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamDescription;

import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;

import com.amazonaws.services.kinesis.utils.Tool;
import org.json.simple.JSONObject;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.amazonaws.regions.RegionUtils;

public class Generator {


    private static AmazonKinesisClient kinesis;
    private static final String STREAM_PARAM = "stream-name";
    private static final String REGION_PARAM = "region";
    private static final String KMS_PARAM = "kms-alias";
    private static final Logger log = LoggerFactory.getLogger(Generator.class);
    private static String streamName;
    private static String kmsAlias;
    private static String regionName;

    private static DataKey myKey;

    private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(2);

    private static void init() throws Exception {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */

        if (System.getProperty(STREAM_PARAM) == null) {
            log.error("You must provide a Stream Name");
            System.exit(1);
        } else {
            streamName = System.getProperty(STREAM_PARAM);
        }

        if (System.getProperty(KMS_PARAM) == null) {
            log.error("You must provide a KMS Alias");
            System.exit(1);
        } else {
            kmsAlias = System.getProperty(KMS_PARAM);
        }

        if (System.getProperty(REGION_PARAM) == null) {
            log.error("You must provide a Region Name");
            System.exit(1);
        } else {
            regionName = System.getProperty(REGION_PARAM);
        }

        AWSCredentialsProvider credentials = null;
        try {
            credentials = new DefaultAWSCredentialsProviderChain();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

        kinesis = new AmazonKinesisClient(credentials);

        try {
            kinesis.setRegion(RegionUtils.getRegion(regionName));
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

    public static void main(String[] args) throws Exception {
        init();


        // Describe the stream and check if it exists.
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName);
        try {
            StreamDescription streamDescription = kinesis.describeStream(describeStreamRequest).getStreamDescription();
            System.out.printf("Stream %s has a status of %s.\n", streamName, streamDescription.getStreamStatus());

            if ("DELETING".equals(streamDescription.getStreamStatus())) {
                System.out.println("Stream is being deleted. This sample will now exit.");
                System.exit(0);
            }

            // Wait for the stream to become active if it is not yet ACTIVE.
            if (!"ACTIVE".equals(streamDescription.getStreamStatus())) {
                waitForStreamToBecomeAvailable(streamName);
            }
        } catch (ResourceNotFoundException ex) {
            log.info(String.format("Stream %s does not exist. \n", streamName));
            System.exit(1);
        }

        // Separate thread that will rotate KMS Data Key seamlessly.
        EXECUTOR.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                refreshDataKey();
            }

        }, 0, 60, TimeUnit.SECONDS);


        ByteBuffer jsonData;

        // PutRecord Generator
        while (true) {

            long createTime = System.currentTimeMillis();

            if (myKey == null) {
                jsonData = refreshDataKeyAndEncryptData(true, String.format("testData-%d", createTime).getBytes());
            } else {
                jsonData = refreshDataKeyAndEncryptData(false, String.format("testData-%d", createTime).getBytes());
            }

            // make Kinesis Request
            makeRequest(createTime, jsonData);
        }

    }


    private static DataKey refreshDataKey () {
        myKey = DataKey.getRandomDataKey(kmsAlias, regionName);
        return myKey;
    }

    // Create new DataKey from KMS and Encrypt Data
    private static ByteBuffer refreshDataKeyAndEncryptData(boolean refreshFlag, byte[] data) {

            if (refreshFlag) {
                myKey = refreshDataKey();
            }

            Cipher cipher = null;
            try {
                cipher = Cipher.getInstance("AES");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (NoSuchPaddingException e) {
                e.printStackTrace();
            }

            log.info(String.format("Data Size (byte) before encoding/encryption: %d", data.length));
            log.info(String.format("Data Size (byte) after encoding: %d", Tool.toBase64(data).getBytes().length));

            ByteBuffer encryptedJsonData = EncryptDataAsJson(cipher, myKey, data);
            return encryptedJsonData;
    }

    // ENCRYPT Data and return a JSON object
    private static ByteBuffer EncryptDataAsJson(final Cipher cipher, final DataKey key, final byte[] data) {
        ByteBuffer jsonData = null;
        byte[] encryptedData = null;


        try {
            SecretKey mySymmetricKeyEncoded = new SecretKeySpec(key.getPlaintextKey().array(), "AES");
            cipher.init(Cipher.ENCRYPT_MODE, mySymmetricKeyEncoded);
            encryptedData = cipher.doFinal(data);

        } catch (InvalidKeyException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (BadPaddingException e) {
            e.printStackTrace();
        } catch (IllegalBlockSizeException e) {
            e.printStackTrace();
            System.exit(1);
        }

        // Encryted Key and Data Key base64 encoded to avoid parsing issues when creating JSON object
        jsonData = jsonBuilder(Tool.toBase64(key.getEncryptedKey()), Tool.toBase64(encryptedData));

        return jsonData;
    }

    // Create JSON object from Encrypted DataKey and Encrypted Data
    private static ByteBuffer jsonBuilder(String key, String data) {
        JSONObject obj = new JSONObject();

        obj.put("key", key);
        obj.put("data", data);

        log.info(String.format("Data Size after encryption/encoding:  %d", data.getBytes().length));
        log.info(String.format("Key Size after encryption/encoding:  %d", key.getBytes().length));

        return ByteBuffer.wrap(obj.toJSONString().getBytes());
    }

    // Make Kinesis Request
    private static void makeRequest(long partitionKey, ByteBuffer jsonData) {
        log.info(String.format("Total Payload (byte) after encoding/encryption with JSON format:  %d\n\n", jsonData.array().length));

        PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setStreamName(streamName);
        putRecordRequest.setData(jsonData);
        putRecordRequest.setPartitionKey(String.format("partitionKey-%d", partitionKey));
        PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);
        log.info(String.format("Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.",
                putRecordRequest.getPartitionKey(),
                putRecordResult.getShardId(),
                putRecordResult.getSequenceNumber()));

    }


    private static void waitForStreamToBecomeAvailable(String myStreamName) throws InterruptedException {
        log.info(String.format("Waiting for %s to become ACTIVE...  ", myStreamName));

        long startTime = System.currentTimeMillis();
        long endTime = startTime + TimeUnit.MINUTES.toMillis(10);
        while (System.currentTimeMillis() < endTime) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(20));

            try {
                DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
                describeStreamRequest.setStreamName(myStreamName);
                describeStreamRequest.setLimit(10);
                DescribeStreamResult describeStreamResponse = kinesis.describeStream(describeStreamRequest);

                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
                System.out.printf("\t- current state: %s\n", streamStatus);
                if ("ACTIVE".equals(streamStatus)) {
                    return;
                }
            } catch (ResourceNotFoundException ex) {
                // ResourceNotFound means the stream doesn't exist yet,
                // so ignore this error and just keep polling.
            } catch (AmazonServiceException ase) {
                throw ase;
            }
        }

        throw new RuntimeException(String.format("Stream %s never became active", myStreamName));
    }




}