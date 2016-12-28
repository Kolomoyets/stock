package ru.iswt.server.stock.moex;

/**
 * Created by admin on 30.11.2016.
 */
public enum MoexParams {

    ThreadCount("moex.thread");

    public String code;

    MoexParams(String code) {
        this.code = code;
    }
}
