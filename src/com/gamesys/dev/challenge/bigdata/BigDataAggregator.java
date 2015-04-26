package com.gamesys.dev.challenge.bigdata;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingDouble;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by harshitha.suresh on 19/04/2015.
 */
public class BigDataAggregator {
    public static Map<String, Double> exchangeRateMap(String exchangeRateFileName, final String currencyCode) throws IOException {

        try (Stream<String> lines = Files.lines(Paths.get(exchangeRateFileName))) {
            return lines.parallel().map(s -> s.split(","))
                    .filter(array -> array[1].equalsIgnoreCase(currencyCode))
                    .collect(Collectors.toMap(array -> array[0], array -> Double.valueOf(array[2])));
        }
    }

    private static Map<String, Double> findAggregatedResult(String partnerTransactionFileName, String exchangeRateFileName, String currencyCode, Predicate<String> partnerFilter) throws IOException {
        Map<String, Double> rateMap = exchangeRateMap(exchangeRateFileName, currencyCode);
        rateMap.put(currencyCode, 1.0);

        try (Stream<String> lines = Files.lines(Paths.get(partnerTransactionFileName))) {

            return lines.parallel().map(s -> s.split(","))
                    .filter(array -> partnerFilter.test(array[0]))
                    .collect(groupingBy(array -> array[0],
                                        summingDouble(array -> Double.valueOf(array[2]) * rateMap.get(array[1]))));
        }
    }

    public static Double aggregatedAmountForPartnerAndCurrency(String partnerTransactionFileName,
                                                               String exchangeRateFileName, String partner, String currencyCode) throws IOException {
        return findAggregatedResult( partnerTransactionFileName,exchangeRateFileName, currencyCode,
                partnerName -> partnerName.equalsIgnoreCase(partner)).get(partner);
    }

    public static Map<String, Double> findAggregatedResult(String partnerFileName, String exchangeRateFileName, String currencyCode) throws IOException {
        return findAggregatedResult(partnerFileName, exchangeRateFileName, currencyCode, partnerName -> true);
    }

    public static void main(String args[]){

    }
}