package test.com.gamesys.dev.challenge.bigdata;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;

import org.junit.Assert;
import org.junit.Test;

import com.gamesys.dev.challenge.bigdata.BigDataAggregator;

/**
 * Created by harshitha.suresh on 20/04/2015.
 */
public class BigDataAggregatorTest {
    private String exchangeRateFileName = "/Users/harshitha.suresh/Documents/gitcode/BigDataAggregator/src/resources/exchangerates.csv";
    private String partnerFileName = "/Users/harshitha.suresh/Documents/gitcode/BigDataAggregator/src/resources/partnertransactions.csv";
    @Test
    public void testExchangeRateMap() throws IOException {


        Map<String, Double> exchangeRateMap = BigDataAggregator.exchangeRateMap
                (exchangeRateFileName, "GBP");
        Assert.assertEquals(9, exchangeRateMap.size());
        exchangeRateMap = BigDataAggregator.exchangeRateMap
                (exchangeRateFileName, "EUR");
        Assert.assertEquals(9, exchangeRateMap.size());

    }

    @Test
    public void testFindAggregatedResultByPartnerAndCurrency() throws IOException {

        Map<String, Double> aggregatedResultMap = BigDataAggregator.findAggregatedResult(partnerFileName, exchangeRateFileName, "GBP");
        Assert.assertEquals(aggregatedResultMap.size(), 3);
        /*
        Unlimited ltd.,311.25
        Local plumber ltd.,136.56
        Defence ltd,234.75
         */
        Assert.assertEquals(new Double(310.7060032207), aggregatedResultMap.get("Unlimited ltd."));
        Assert.assertEquals(new Double(134.150076), aggregatedResultMap.get("Local plumber ltd."));
        Assert.assertEquals(new Double(234.11941214805003), aggregatedResultMap.get("Defence ltd."));
    }

    @Test
    public void testFindAggregatedAmountByPartnerByCurrency() throws IOException {

        String currencyCode = "GBP";
        String partner="Unlimited ltd.";
        Double aggregatedAmountForPartnerAndCurrency = BigDataAggregator.aggregatedAmountForPartnerAndCurrency(partnerFileName, exchangeRateFileName, partner, currencyCode);
        Assert.assertEquals(new Double(310.7060032207) ,aggregatedAmountForPartnerAndCurrency);

    }
}
