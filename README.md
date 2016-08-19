# kinesis-encrypt-decrypt-sample
Use
```
git clone https://github.com/fabiantan/kinesis-encrypt-decrypt-sample
```

### Producer with KMS Encryption
```
cd kinesis-encrypt-decrypt-sample
mvn assembly:assembly
java -cp target/KinesisEncryptDecrypt-1.0-SNAPSHOT-complete.jar -Dstream-name=kinesis_start_3 -Dregion=ap-southeast-2 -Dkms-alias=fabtanKey1 com.amazonaws.services.kinesis.producer.Generator
```

### Consumer with KMS Decryption
```
cd kinesis-encrypt-decrypt-sample

Configure "application.properties" file

java -cp target/KinesisEncryptDecrypt-1.0-SNAPSHOT-complete.jar com.amazonaws.services.kinesis.consumer.KMSKinesisApplication
```
