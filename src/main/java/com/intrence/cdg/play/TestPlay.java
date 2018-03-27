package com.intrence.cdg.play;


import org.joda.money.Money;

import java.util.Currency;
import java.util.Locale;

/**
 * Created by wliu on 11/14/17.
 */
public class TestPlay {

    public static void main( String[] args ) throws Exception {
        System.out.println("Starting CDG Play");


        for (Locale locale : Locale.getAvailableLocales()) {
            System.out.println(locale);
            try {
                Currency currency = Currency.getInstance(locale);
                System.out.println(currency + " " + currency.getSymbol(locale));
            } catch (Exception e){
            }
        }



        System.out.println("Ending CDG Play");
    }

}
