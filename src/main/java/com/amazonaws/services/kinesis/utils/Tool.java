package com.amazonaws.services.kinesis.utils;

import java.nio.ByteBuffer;
import java.util.Base64;
/**
 * Created by fabtan on 8/16/16.
 */
public class Tool {
    public static String toBase64(ByteBuffer data) {
        return Base64.getEncoder().encodeToString(data.array());

    }

    public static String toBase64(byte[] data) {
        return Base64.getEncoder().encodeToString(data);

    }
}
