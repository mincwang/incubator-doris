package org.apache.doris.flink.table;

import org.apache.flink.calcite.shaded.com.google.common.base.Strings;

import java.io.StringWriter;

public class DorisDelimiterParser {
    private static final String HEX_STRING = "0123456789ABCDEF";

    public DorisDelimiterParser() {}

    public static String parse(String sp, String dSp) throws RuntimeException {
        if (Strings.isNullOrEmpty(sp)) {
            return dSp;
        } else if (!sp.toUpperCase().startsWith("\\X")) {
            return sp;
        } else {
            String hexStr = sp.substring(2);
            if (hexStr.isEmpty()) {
                throw new RuntimeException("Failed to parse delimiter: `Hex str is empty`");
            } else if (hexStr.length() % 2 != 0) {
                throw new RuntimeException("Failed to parse delimiter: `Hex str length error`");
            } else {
                char[] var3 = hexStr.toUpperCase().toCharArray();
                int var4 = var3.length;

                int var5;
                for (var5 = 0; var5 < var4; ++var5) {
                    char hexChar = var3[var5];
                    if (HEX_STRING.indexOf(hexChar) == -1) {
                        throw new RuntimeException(
                                "Failed to parse delimiter: `Hex str format error`");
                    }
                }

                StringWriter writer = new StringWriter();
                byte[] var9 = hexStrToBytes(hexStr);
                var5 = var9.length;

                for (int var10 = 0; var10 < var5; ++var10) {
                    byte b = var9[var10];
                    writer.append((char) b);
                }

                return writer.toString();
            }
        }
    }

    private static byte[] hexStrToBytes(String hexStr) {
        String upperHexStr = hexStr.toUpperCase();
        int length = upperHexStr.length() / 2;
        char[] hexChars = upperHexStr.toCharArray();
        byte[] bytes = new byte[length];

        for (int i = 0; i < length; ++i) {
            int pos = i * 2;
            bytes[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }

        return bytes;
    }

    private static byte charToByte(char c) {
        return (byte) HEX_STRING.indexOf(c);
    }
}
