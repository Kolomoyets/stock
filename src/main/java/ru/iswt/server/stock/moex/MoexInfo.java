package ru.iswt.server.stock.moex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by admin on 02.11.2015.
 * Информация загруженная с Moex структура и данные
 */

public class MoexInfo {

    private String id; // Первичный ключ
    private List<HashMap<String, String>> meta;
    private List<HashMap<String, String>> rows;

    /**
     * Конструктор
     */
    public MoexInfo() {

    }

    public void addRows(List<HashMap<String, String>> rows) {
        if (this.rows == null) {
            this.rows = new ArrayList<>();
        }
        this.rows.addAll(rows);
    }


    public List<String> getValue(String filedname) {
        List<String> list = new ArrayList<>();
        for (HashMap<String, String> row : rows) {
            list.add(row.get(filedname));
        }
        return list;
    }


    public List<String> getValueGroup(String... filedname) {
        List<String> list = new ArrayList<>();
        for (HashMap<String, String> row : rows) {
            String s = "";
            for (String code : filedname) {
                s = s + code + "=" + row.get(code) + ",";
            }
            s = s.substring(0, s.length() - 1);
            list.add(s);
        }
        return list;
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }


    public List<HashMap<String, String>> getMeta() {
        return meta;
    }

    public void setMeta(List<HashMap<String, String>> meta) {
        this.meta = meta;
    }

    public List<HashMap<String, String>> getRows() {
        return rows;
    }

    public void setRows(List<HashMap<String, String>> rows) {
        this.rows = rows;
    }

    public String getFullTablename() {
        return null;
    }

    public void setPrefix(String prefix) {

    }
}
