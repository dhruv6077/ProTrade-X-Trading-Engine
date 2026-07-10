package Price;

import Exceptions.InvalidPriceOperation;

import java.util.Objects;

public class Price implements Comparable<Price>{
    private final long cents;

    public Price(long cents){
        this.cents = cents;

    }

    private void nullChecker(Price p) throws InvalidPriceOperation {
        if(p == null){
            throw new InvalidPriceOperation("Null price value not allowed");
        }
    }

    public boolean isNegative(){
        return cents <= 0;
    }

    public long getCents() {
        return cents;
    }

    public Price add(Price p) throws InvalidPriceOperation {
        nullChecker(p);
        return new Price(this.cents + p.cents);
    }

    public Price subtract(Price p) throws InvalidPriceOperation {
        nullChecker(p);
        return new Price(this.cents - p.cents);
    }

    public Price multiply(Price p) throws InvalidPriceOperation {
        nullChecker(p);

        return new Price((this.cents * p.cents) / 100);
    }

    public boolean greaterOrEqual(Price p) throws InvalidPriceOperation {
        nullChecker(p);
        return this.cents >= p.cents;
    }

    public boolean lessOrEqual(Price p) throws InvalidPriceOperation {
        nullChecker(p);
        return this.cents <= p.cents;
    }

    public boolean greaterThan(Price p) throws InvalidPriceOperation {
        nullChecker(p);
        return this.cents > p.cents;
    }

    public boolean lessThan(Price p) throws InvalidPriceOperation{
        nullChecker(p);
        return this.cents < p.cents;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Price price = (Price) o;
        return cents == price.cents;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(cents);
    }

    @Override
    public int compareTo(Price p) {
        return Long.compare(this.cents, p.cents);
    }

    @Override
    public String toString() {
        long absCents = Math.abs(cents);
        String dollars = String.format("%,d", absCents / 100);
        String sign = cents < 0 ? "-" : "";
        return String.format("%s$%s.%02d", sign, dollars, absCents % 100);
    }
}
