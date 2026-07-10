package Price;

import Exceptions.InvalidPriceOperation;

import java.math.BigDecimal;
import java.math.RoundingMode;

public abstract class PriceFactory {

    public static Price makePrice (int value) throws InvalidPriceOperation {
        return makePrice((long) value);
    }

    public static Price makePrice(long value) throws InvalidPriceOperation {
        return new Price(value);
    }

    public static Price makePrice(String stringValueIn) throws InvalidPriceOperation {
        if (stringValueIn == null) {
            throw new InvalidPriceOperation("Null string not allowed");
        }

        stringValueIn = stringValueIn.replaceAll("[$,]", "");
        try {
            BigDecimal dollars = new BigDecimal(stringValueIn);
            long cents = dollars.movePointRight(2)
                    .setScale(0, RoundingMode.UNNECESSARY)
                    .longValueExact();
            return makePrice(cents);
        } catch (ArithmeticException | NumberFormatException e) {
            throw new InvalidPriceOperation("Invalid price value: " + stringValueIn);
        }
    }
}
