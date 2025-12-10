package Price;

import Exceptions.InvalidPriceOperation;

import java.util.HashMap;

public abstract class PriceFactory {


    private static final HashMap<Integer, Price> uniquePrice = new HashMap<>();

    public static Price makePrice (int value) throws InvalidPriceOperation {
        return getInstance(value);
    }

    public static Price makePrice(String stringValueIn) throws InvalidPriceOperation {
        if (stringValueIn == null) {
            throw new InvalidPriceOperation("Null string not allowed");
        }

        int cents = 0;

        stringValueIn = stringValueIn.replaceAll("[$,]", "");

        if (stringValueIn.contains(".")){

            double value = Double.parseDouble(stringValueIn) * 100;
            cents = (int) Math.floor(value); // Removes unnecessary decimal numbers

        } else {

            cents = Integer.parseInt(stringValueIn) * 100;

        }
        return getInstance(cents);
    }

    private static Price getInstance(int price) {
        if(uniquePrice.containsKey(price)) {
            return uniquePrice.get(price);
        } else {
            Price newVal = new Price(price);
            uniquePrice.put(price, newVal);
            return uniquePrice.get(price);
        }
    }

}