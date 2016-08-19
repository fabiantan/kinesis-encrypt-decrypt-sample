package com.amazonaws.services.kinesis.consumer;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;
import java.nio.ByteBuffer;

import com.amazonaws.regions.RegionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;


import java.util.Base64;;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;

import org.json.simple.parser.ParseException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import java.util.HashMap;
import java.util.Map;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
//
//
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;

import com.amazonaws.services.kms.model.DecryptRequest;
import com.amazonaws.services.kms.AWSKMSClient;

/**
*
*/
public class KMSRecordProcessor implements IRecordProcessor {
    
    private static final Log LOG = LogFactory.getLog(KMSRecordProcessor.class);
    private String kinesisShardId;

    // Backoff and retry settings
    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 10;

    // Checkpoint about once a minute
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
    private long nextCheckpointTimeInMillis;
    
    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

    private JSONParser parser = new JSONParser();

    private Cipher cipher = null;

    private AWSKMSClient kms;

    /**
* Constructor.
*/
    public KMSRecordProcessor() {
        super();
    }
    
    /**
* {@inheritDoc}
*/
    @Override
    public void initialize(String shardId) {
        LOG.info("Initializing record processor for shard: " + shardId);
        this.kinesisShardId = shardId;


	    kms = new AWSKMSClient(new DefaultAWSCredentialsProviderChain());
        kms.setRegion(RegionUtils.getRegion(KMSKinesisApplication.getRegionName()));

        try {
            cipher = Cipher.getInstance("AES");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (NoSuchPaddingException e) {
            e.printStackTrace();
        }
    }

    /**
* {@inheritDoc}
*/
    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Processing " + records.size() + " records from " + kinesisShardId);
        
        // Process records and perform all exception handling.
        processRecordsWithRetries(records);
        
        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
        
    }

    private void processRecordsWithRetries(List<Record> records){
	for (Record record : records) {
		boolean processedSuccessfully = false;
		for (int i = 0; i < NUM_RETRIES; i++) {
		
			try {
                processSingleRecord(record);
                processedSuccessfully = true;
                break;
            } catch (Throwable t) {
                LOG.warn("Caught throwable while processing record " + record, t);
            }

			try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                LOG.debug("Interrupted sleep", e);
			}
		}

        if (!processedSuccessfully) {
            LOG.error("Couldn't process record " + record + ". Skipping the record.");
		}
  	}
  }

  private void processSingleRecord(Record record) {
        // TODO Add your own record processing logic here

        
        String data = null;
	    JSONObject jsonObject = null;
	    Object jsonObj = null;

        try {
            data = decoder.decode(record.getData()).toString();
        
            jsonObj = parser.parse(data);
            jsonObject = (JSONObject) jsonObj;

            String encryptedBase64Key = (String) jsonObject.get("key");
            String encryptedBase64Data = (String) jsonObject.get("data");
            ByteBuffer ciphertextBlob = ByteBuffer.wrap(Base64.getDecoder().decode(encryptedBase64Key));

            DecryptRequest req = new DecryptRequest().withCiphertextBlob(ciphertextBlob);
            byte[] plainText = kms.decrypt(req).getPlaintext().array();

            SecretKey mySymmetricKeyEncoded = new SecretKeySpec(plainText, "AES");


            cipher.init(Cipher.DECRYPT_MODE, mySymmetricKeyEncoded);
            byte[] dataBytes = cipher.doFinal(Base64.getDecoder().decode(encryptedBase64Data));

            LOG.info(record.getSequenceNumber() + ", " + record.getPartitionKey() + ", " + data + ", Decrypted Data: " + (new String(dataBytes)));

        } catch (CharacterCodingException e) {
	        LOG.error("Malformed data: " + data, e);
	    } catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        } catch (BadPaddingException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
 }

/**
* {@inheritDoc}
*/
    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
    }
    
 

    /** Checkpoint with retries.
* @param checkpointer
*/
    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId);
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                LOG.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (NUM_RETRIES - 1)) {
                    LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + NUM_RETRIES, e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                LOG.debug("Interrupted sleep", e);
            }
        }
    }

}
