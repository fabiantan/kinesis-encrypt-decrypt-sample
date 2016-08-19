package com.amazonaws.services.kinesis.kms;

import java.nio.ByteBuffer;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.*;
import com.amazonaws.regions.RegionUtils;

public class DataKey {

    private ByteBuffer plaintextKey;
    private ByteBuffer encryptedKey;

    public ByteBuffer getPlaintextKey() {
        return plaintextKey;
    }

    public ByteBuffer getEncryptedKey() {
        return encryptedKey;
    }

    public DataKey(String alias, String regionName)  {


        AWSKMSClient kms = new AWSKMSClient(new DefaultAWSCredentialsProviderChain());
        kms.setRegion(RegionUtils.getRegion(regionName));
        String keyId = String.format("alias/%s", alias);
        GenerateDataKeyRequest dataKeyRequest = new GenerateDataKeyRequest();
        dataKeyRequest.setKeyId(keyId);
        dataKeyRequest.setKeySpec("AES_128");

        GenerateDataKeyResult dataKeyResult = kms.generateDataKey(dataKeyRequest);

        this.plaintextKey = dataKeyResult.getPlaintext();
        this.encryptedKey = dataKeyResult.getCiphertextBlob();
    }

    public static DataKey getRandomDataKey(String alias, String regionName) {
        return new DataKey(alias, regionName);

    }




}