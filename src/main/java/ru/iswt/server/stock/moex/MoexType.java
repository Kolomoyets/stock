package ru.iswt.server.stock.moex;

/**
 * Created by admin on 28.12.2016.
 */
public enum MoexType {
    ACTUAL, HISTORY;


    String getUrlQuery() {
        if (this == ACTUAL) {
            return "";
        } else {
            return "history/";
        }

    }
}
