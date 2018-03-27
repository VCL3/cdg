/**
 * Created by wliu on 11/8/17.
 */
package com.intrence.cdg.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.intrence.cdg.net.FetchRequest;
import com.intrence.cdg.extractor.Extractor;
import com.intrence.cdg.util.Constants;
import com.intrence.models.model.DataPoint;
import com.intrence.models.model.Price;
import com.intrence.models.model.Product;
import com.intrence.models.model.ProductKeys;
import com.intrence.models.model.Size;
import com.intrence.models.util.CurrencyHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class ProductParser<N> extends BaseParser<N> {

    private final static Logger LOGGER = Logger.getLogger(ProductParser.class);

    public ProductParser(String source, JsonNode rules, FetchRequest req, Extractor<N> extractor) {
        super(source, rules, req, extractor);
    }

    /** BaseParser Abstract Methods */
    @Override
    protected String getUrl(String string) {
        throw new UnsupportedOperationException("this needs to be implemented in extented class");
    }

    @Override
    protected DataPoint extractEntity(N contentTree) {

        DataPoint dataPoint;
        String name = null;

        try {
            Product.Builder productBuilder = new Product.Builder();

            name = extractName(contentTree);

            productBuilder.name(name)
                    .description(extractDescription(contentTree))
                    .designer(extractDesigner(contentTree))
                    .availableSizes(extractAvailableSizes(contentTree))
                    .source(this.source)
                    .externalLink(this.originUrl)
                    .imageLink(extractImageLink(contentTree));

            Price originalPrice = extractOriginalPrice(contentTree);
            Price currentPrice = extractCurrentPrice(contentTree);

            // Product is on sale
            if (originalPrice != null && currentPrice != null) {
                productBuilder.isOnSale(currentPrice.equals(originalPrice)).saleDiscount(calculateSaleDiscount(originalPrice, currentPrice));
            } else {
                productBuilder.isOnSale(false).saleDiscount(0);
            }

            dataPoint = new DataPoint.Builder()
                    .product(productBuilder.build())
                    .build();

        } catch (IllegalArgumentException e) {
            if (StringUtils.isNotBlank(name)) {
                LOGGER.warn(String.format("Exception=PlaceParserError could not find external id for source=%s and url=%s",
                        source, originUrl));
            }
            dataPoint = null;
        } catch (Exception e) {
            LOGGER.error(String.format("Exception=PlaceParserError could not extract Place data for source=%s and  " +
                    "url=%s", source, originUrl),e);
            dataPoint = null;
        }
        return dataPoint;
    }

    public String extractName(N node) {
        return extractor.extractField(getRule(ProductKeys.NAME_KEY.toString()), node);
    }

    public String extractDescription(N node) {
        return extractor.extractField(getRule(ProductKeys.DESCRIPTION_KEY.toString()), node);
    }

    public String extractDesigner(N node) {
        return extractor.extractField(getRule(ProductKeys.DESIGNER_KEY.toString()), node);
    }

    public Set<Size> extractAvailableSizes(N node) {
        String sizeString = extractor.extractField(getRule(ProductKeys.AVAILABLE_SIZES_KEY.toString()), node);
        return new HashSet<>();
    }

    public String extractImageLink(N node) {
        return extractor.extractField(getRule(ProductKeys.IMAGE_LINK_KEY.toString()), node);
    }

    public Price extractOriginalPrice(N node) throws Exception {
        String originalPriceString = extractor.extractField(getRule(ProductKeys.ORIGINAL_PRICE_KEY.toString()), node);
        return extractPriceFromFormattedAmount(originalPriceString);
    }

    public Price extractCurrentPrice(N node) throws Exception {
        String currentPriceString = extractor.extractField(getRule(ProductKeys.CURRENT_PRICE_KEY.toString()), node);
        return extractPriceFromFormattedAmount(currentPriceString);
    }

    // Price related helpers
    private Price extractPriceFromFormattedAmount(String formattedAmount) throws Exception {
        if (formattedAmount != null && !formattedAmount.isEmpty()) {
            String currencySymbol = extractCurrencyFromFormattedAmount(formattedAmount);
            Locale locale;
            if (currencySymbol != null) {
                locale = CurrencyHandler.getInstance().getLocaleFromCurrencySymbol(currencySymbol);
            } else {
                // Default to US
                currencySymbol = Constants.USD_SYMBOL;
                locale = Locale.US;
            }
            BigDecimal extractedAmount = extractAmountFromFormattedAmount(formattedAmount, locale);
            Integer amount = normalizeAmountFromExtractedAmount(extractedAmount);
            String currencyCode = CurrencyHandler.getInstance().getCurrencyCodeFromCurrencySymbol(currencySymbol);
            return new Price.Builder()
                    .amount(amount)
                    .currencyCode(currencyCode)
                    .formattedAmount(Price.formattedAmountWithCurrency(amount, currencyCode))
                    .build();
        }
        return null;
    }

    public static String extractCurrencyFromFormattedAmount(String formattedAmount) {
        // Naive solution to just get the first character
        String firstCharacter = formattedAmount.substring(0, 1);
        if (Constants.availableCurrencySymbols.contains(firstCharacter)) {
            return firstCharacter;
        }
        return null;
    }

    public static BigDecimal extractAmountFromFormattedAmount(final String formattedAmount, final Locale locale) throws ParseException {
        final NumberFormat format = NumberFormat.getNumberInstance(locale);
        if (format instanceof DecimalFormat) {
            ((DecimalFormat) format).setParseBigDecimal(true);
        }
        // Replace anything that's not digit or dot
        return (BigDecimal) format.parse(formattedAmount.replaceAll("[^\\d.,]", ""));
    }

    public static Integer normalizeAmountFromExtractedAmount(BigDecimal extractedAmount) {
        // Round up to the 2nd digit after decimal point
        BigDecimal scaledAmount = extractedAmount.setScale(2, BigDecimal.ROUND_HALF_UP);
        return scaledAmount.multiply(new BigDecimal(100)).intValueExact();
    }

    public static Integer calculateSaleDiscount(Price originalPrice, Price currentPrice) {
        // saleDiscount is an percentage so multiply by a factor of 100
        Double saleDiscount = (originalPrice.getAmount() - currentPrice.getAmount()) / ((double) originalPrice.getAmount()) * 100;
        return saleDiscount.intValue();
    }

}
