package com.cts.txn;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.regex.Pattern;

/** Stateless structural validation. Returns a rejection reason, or {@code null} when the input is acceptable. */
final class TransactionValidator {
    private static final Pattern ID = Pattern.compile("[\\w.:\\-]{1,128}");
    private static final int MAX_INTEGER_DIGITS = 15;

    private TransactionValidator() { }

    static String validate(Transaction tx) {
        if (tx == null) return "Transaction is null";
        if (!isValidId(tx.transactionId())) return "transactionId is required (1-128 chars of letters, digits, _ . : -)";
        if (!isValidId(tx.requestId())) return "requestId is required (1-128 chars of letters, digits, _ . : -)";
        if (!isValidId(tx.accountId())) return "accountId is required (1-128 chars of letters, digits, _ . : -)";
        if (tx.sequenceNumber() < 1) return "sequenceNumber must be >= 1";
        if (tx.transactionType() == null) return "transactionType is required";
        if (tx.eventTime() == null) return "eventTime is required";

        Currency currency = currency(tx.currency());
        if (currency == null) return "currency must be a valid ISO 4217 code";

        BigDecimal amount = tx.amount();
        if (amount == null || amount.signum() <= 0) return "amount must be greater than zero";
        // Bound the magnitude before any toPlainString()/arithmetic so hostile exponents cannot exhaust memory.
        if (amount.precision() - amount.scale() > MAX_INTEGER_DIGITS) return "amount is too large";
        int allowed = Math.max(currency.getDefaultFractionDigits(), 0);
        if (amount.stripTrailingZeros().scale() > allowed)
            return "amount has more than " + allowed + " decimal places for " + currency.getCurrencyCode();
        return null;
    }

    private static boolean isValidId(String value) {
        return value != null && ID.matcher(value).matches();
    }

    private static Currency currency(String code) {
        if (code == null || !code.matches("[A-Z]{3}")) return null;
        try {
            return Currency.getInstance(code);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
