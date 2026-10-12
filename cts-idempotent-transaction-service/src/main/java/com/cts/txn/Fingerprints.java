package com.cts.txn;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Canonical payload fingerprint used to tell a replay apart from a transaction-ID collision. */
final class Fingerprints {
    private Fingerprints() { }

    /** Covers business fields only; requestId and the simulation flag are deliberately excluded. */
    static String of(Transaction tx) {
        StringBuilder canonical = new StringBuilder();
        // Length-prefixed fields: ("a|b","c") can never collide with ("a","b|c").
        field(canonical, tx.transactionId());
        field(canonical, tx.accountId());
        field(canonical, Integer.toString(tx.sequenceNumber()));
        field(canonical, tx.transactionType().name());
        field(canonical, tx.amount().stripTrailingZeros().toPlainString());
        field(canonical, tx.currency());
        field(canonical, tx.eventTime().toString());
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(canonical.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static void field(StringBuilder sb, String value) {
        sb.append(value.length()).append(':').append(value).append(';');
    }
}
