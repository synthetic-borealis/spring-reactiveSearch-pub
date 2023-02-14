package com.elhanan.reactiveSearch.util;

import java.util.Random;

public class UUID {
    private final static int ID_LENGTH = 8;
    private final static Random random = new Random();

    public static String generateRandomID() {
        final String charPool = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        StringBuilder idBuilder = new StringBuilder();

        for (int i = 0; i < ID_LENGTH; ++i) {
            idBuilder.append(charPool.charAt(random.nextInt(charPool.length())));
        }

        return idBuilder.toString();
    }
}
