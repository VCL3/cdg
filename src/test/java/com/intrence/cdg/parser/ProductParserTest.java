package com.intrence.cdg.parser;

import com.intrence.models.model.Price;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Locale;

/**
 * Created by wliu on 11/26/17.
 */
public class ProductParserTest {

    public static String formattedAmount = "$1,2345.67890";

    @Test
    public void testCalculateSaleDiscount() {
        Price originalPrice = new Price.Builder().amount(19999).currencyCode("USD").build();
        Price currentPrice = new Price.Builder().amount(9999).currencyCode("USD").build();
        Integer saleDiscount = ProductParser.calculateSaleDiscount(originalPrice, currentPrice);
        Assert.assertTrue(saleDiscount == 50);
    }

    @Test
    public void testExtractAmountFromFormattedAmount() throws Exception {
        BigDecimal extractedAmount = ProductParser.extractAmountFromFormattedAmount(formattedAmount, Locale.US);
        System.out.println(extractedAmount.doubleValue());
    }

    @Test
    public void testExtractCurrencySymbol() {
        String currencySymbol = ProductParser.extractCurrencyFromFormattedAmount(formattedAmount);
        Assert.assertEquals("$", currencySymbol);
    }

    @Test
    public void testNormalizeExtractedAmount() {
        BigDecimal extractedAmount = new BigDecimal("123.4567");
        Integer amount = ProductParser.normalizeAmountFromExtractedAmount(extractedAmount);
        Assert.assertTrue(12346 == amount);
    }

    @Test
    public void testExtractPriceFromFormattedAmount() {
    }

}
