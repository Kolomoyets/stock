package ru.iswt.server.stock.moex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.iswt.server.utils.ServerHelper;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by admin on 03.11.2015.
 */
public class MoexHelp {
    static Logger LOG = LogManager.getLogger(MoexHelp.class);
    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");


    /**
     * Loading of global reference manuals ( Загрузка глобальных справочников)
     * /iss
     */
    public static Moex loadGlobalReference() {
        Moex moex = new Moex();
        String url = "http://www.micex.ru/iss.xml";
        try {
            List<MoexInfo> list = moex.parseXml(url);
            moex.setMoexInfoList(list);
            moex.relocate();


        } catch (Exception e) {
            LOG.error(e);
        }
        return moex;
    }


    /**
     * Download information for all securities
     * /iss/securities
     */
    static Moex loadSecurities(int start, int finish, int limit) {
        Moex moex = new Moex();
        try {
            String template = "http://www.micex.ru/iss/securities.xml?iss.json=extended&start=[start]&limit=[limit]";
            moex.parseTillEnds(template, start, finish, limit);
        } catch (Exception e) {
            LOG.error(e);
            e.printStackTrace();
        }
        moex.relocate();
        return moex;
    }


    /*
     *  /iss/securities/[security]
     * @param codes
     * @param threadCount
     */
    static void loadSecuritiesCodes(List<String> codes, Integer threadCount) {
        String url = "http://www.micex.ru/iss/securities/[security].xml";
        Map<String, List<MoexInfo>> map = load(url, "[security]", codes, threadCount);
        map.size();
    }


    /**
     * /iss/securities/[security]/indices
     *
     * @param codes
     * @param threadCount
     */
    static void loadSecuritiesIndices(List<String> codes, Integer threadCount) {
        String url = "http://www.micex.ru/iss/securities/[security]/indices.xml";
        Map<String, List<MoexInfo>> map = load(url, "[security]", codes, threadCount);
        map.size();
    }


    /**
     * /iss/securities/[security]/aggregates
     *
     * @param codes
     * @param threadCount
     */
    static void loadSecuritiesAggregates(List<String> codes, Integer threadCount) {
        String url = "http://www.micex.ru/iss/securities/[security]/aggregates.xml";
        Map<String, List<MoexInfo>> map = load(url, "[security]", codes, threadCount);
        map.size();
    }

    static Map<String, List<MoexInfo>> load(String pattern, String replace, List<String> codes, Integer threadCount) {
        HashMap<String, List<String>> keys = new HashMap<String, List<String>>();
        keys.put(replace, codes);
        return load(pattern, keys, threadCount);
    }

    static HashMap<String, List<String>> get(HashMap<String, List<String>> map, String ignoreCode) {
        HashMap<String, List<String>> res = new HashMap<String, List<String>>();
        for (String code : map.keySet()) {
            if (!code.equals(ignoreCode)) {
                res.put(code, map.get(code));
            }
        }
        return res;
    }


    static Map<String, HashMap<String, String>> generateUrl(HashMap<String, List<String>> keys) {
        List<String> res = new ArrayList<>();
        generateUrl(keys, res, null);

        Map<String, HashMap<String, String>> map = new HashMap<>();
        for (String group : res) {
            HashMap<String, String> hashMap = new HashMap<>();
            map.put(group, hashMap);
            String[] strings = group.split(",");
            for (String codeValue : strings) {
                String[] strings1 = codeValue.split("=");
                hashMap.put(strings1[0], strings1[1]);
            }
        }
        return map;
    }

    static void generateUrl(HashMap<String, List<String>> keys, List<String> start, String parent) {
        for (String part : keys.keySet()) {
            List<String> list = keys.get(part);
            HashMap<String, List<String>> map = get(keys, part);
            for (String code : list) {


                String next = IfNull(parent, "") + IfNull(parent, "", ",") + part + "=" + code;
                if (map.size() == 0) {
                    start.add(next);
                } else {
                    generateUrl(map, start, next);
                }
            }
            break;
        }
    }


    private static String IfNull(String value, String ifnull) {
        if (value == null) {
            return ifnull;
        }

        return value;
    }

    private static String IfNull(String value, String ifnull, String ifNotNull) {

        if (value == null) {
            return ifnull;
        } else {
            return ifNotNull;
        }
    }

    static Map<String, List<MoexInfo>> load(String pattern, HashMap<String, List<String>> keys, Integer threadCount) {
        Map<String, HashMap<String, String>> map2 = generateUrl(keys);
        HashMap<String, String> mapUrl = new HashMap<String, String>();
        for (String groups : map2.keySet()) {
            HashMap<String, String> map = map2.get(groups);
            String url = pattern;
            for (String code : map.keySet()) {
                String value = map.get(code);
                url = url.replace(code, value);
                mapUrl.put(groups, url);
            }
        }

        List<Thread> threads = new ArrayList<>();
        Map<String, List<MoexInfo>> mapMoexInfo = new ConcurrentHashMap<>();

        for (String code : mapUrl.keySet()) {
            String url = mapUrl.get(code);
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    long a1 = new Date().getTime();
                    Moex moex = new Moex();
                    List<MoexInfo> infos = moex.parseXml(url);
                    deleteMoexInfo(infos);
                    mapMoexInfo.put(code, infos);
                    long a2 = new Date().getTime();
                }
            });
            thread.setName("t_" + ServerHelper.getSession());
            threads.add(thread);
        }
        ServerHelper.executeThread(threads, threadCount);
        return mapMoexInfo;
    }


    private static MoexInfo getMoexInfo(List<MoexInfo> list, String id) {
        for (MoexInfo info : list) {
            if (id.equals(info.getId())) {
                return info;
            }
        }
        return null;
    }


    private static void deleteMoexInfo(List<MoexInfo> list) {
        Iterator<MoexInfo> iterator = list.iterator();
        while (iterator.hasNext()) {
            MoexInfo info = iterator.next();
            if (info.getRows().size() == 0) {
                iterator.remove();
            }
        }
    }

    /**
     * Download information for all securities
     *
     * @return
     */
    public static Moex loadSecurities() {
        return loadSecurities(0, 0, 0);
    }

    public static String date2String(Date date) {
        return dateFormat.format(date);
    }


    /**
     * To receive summary turns on the markets (Получить сводные обороты по рынкам)
     * /iss/turnovers
     *
     * @param isTonightSession - To show turns for an evening session (Показывать обороты за вечернюю сессию)
     * @param startDate        - Start date
     * @param endDate          - Finish date
     * @param threadCount      - Quantity of threads for processing
     * @return
     */
    static Map<String, List<MoexInfo>> loadTurnovers(boolean isTonightSession, Date startDate, Date endDate, Integer threadCount) {
        String url = "http://www.micex.ru/iss/turnovers.xml?lang=ru";
        if (isTonightSession) {
            url += "&is_tonight_session=1";
        }
        List<String> codes = new ArrayList<>();
        Map<String, List<MoexInfo>> map = null;
        if (startDate != null && endDate != null) {
            url = url + "&date=" + "[date]";

            Date curr = startDate;
            while (curr.before(endDate)) {
                curr = ServerHelper.addDays(curr, 1);
                String code = date2String(curr);
                codes.add(code);
            }
            map = load(url, "[date]", codes, threadCount);

        }
        return map;
    }


    /**
     * To receive the current value of turns of a trading session on the markets of trade system (Получить текущее значение оборотов торговой сессии по рынкам торговой системы)
     * /iss/engines/[engine]/turnovers
     *
     * @param codes
     * @param isTonightSession
     * @param date
     * @param threadCount
     * @return
     */
    static Map<String, List<MoexInfo>> loadEnginesTurnovers(List<String> codes, boolean isTonightSession, Date date, Integer threadCount) {
        String url = "http://www.micex.ru/iss/engines/[engine]/turnovers.xml?lang=ru";
        if (isTonightSession) {
            url += "&is_tonight_session=1";
        }
        if (date != null) {
            url += "&date==" + date2String(date);
        }

        Map<String, List<MoexInfo>> map = load(url, "[engine]", codes, threadCount);
        return map;
    }

    /**
     * To receive the current value of a turn on the market (Получить текущее значение оборота по рынку)
     * /iss/engines/[engine]/markets/[market]/turnovers
     *
     * @param engines
     * @param markets
     * @param threadCount
     * @return
     */
    static Map<String, List<MoexInfo>> loadEnginesMarketsTurnovers(List<String> engines, List<String> markets, Integer threadCount) {
        String url = "http://www.micex.ru/iss/engines/[engine]/markets/[market]/turnovers.xml?lang=ru";

        HashMap<String, List<String>> keys = new HashMap<>();
        keys.put("[engine]", engines);
        keys.put("[market]", markets);
        Map<String, List<MoexInfo>> map = load(url, keys, threadCount);
        return map;
    }


    /*
      To receive the description of fields for requests of a torguyemost of papers (listing)
      (Получить описание полей для запросов торгуемости бумаг (листинга))
       /iss/history/engines/[engine]/markets/[market]/.*?listing/columns
     */


    static Map<String, List<MoexInfo>> loadHistoryEnginesMarketsListingInfo() {
        return null;
    }


    /**
     * A list of non-tradable instruments with an indication of intervals of the trading modes
     * (Список неторгуемых инструментов с указанием интервалов торгуемости по режимам)
     * /iss/history/engines/[engine]/markets/[market]/listing
     *
     * @param codeMap
     * @param start
     * @param limit
     * @param threadCount
     * @return
     */
    public static Map<String, List<MoexInfo>> loadHistoryEnginesMarketsListing(Map<String, Map<String, String>> codeMap, int start, int limit, int threadCount) {
        //            http://iss.moex.com/iss/history/engines/stock/markets/shares/listing.xml?start=200&limit=50
        String pattern = "http://www.micex.ru/iss/history/engines/[engine]/markets/[market]/listing.xml?lang=ru&start=[start]";
        HashMap<String, List<String>> keys = new HashMap<>();


        HashMap<String, String> mapUrl = new HashMap<String, String>();
        for (String groups : codeMap.keySet()) {
            Map<String, String> map = codeMap.get(groups);
            String url = pattern;
            for (String code : map.keySet()) {
                String value = map.get(code);
                url = url.replace(code, value);
                mapUrl.put(groups, url);
            }
        }

        Map<String, List<MoexInfo>> map = new HashMap<>();
        List<Thread> threads = new ArrayList<>();
        for (String code : codeMap.keySet()) {
            String url = mapUrl.get(code);
            Thread thread = new Thread(() -> {
                Moex moex = new Moex();
                moex.parseTillEnds(url, start, limit, 0);
                map.put(code, moex.getMoexInfoList());
            });
            thread.setName("t_" + ServerHelper.getSession());
            threads.add(thread);
        }
        ServerHelper.executeThread(threads, threadCount);
        return map;
    }


    /**
     * To obtain the data for the listing of the securities in the historical context in the specified mode
     * (Получить данные по листингу бумаг в историческом разрезе по указанному режиму)
     * /iss/history/engines/[engine]/markets/[market]/boards/[board]/listing
     *
     * @param codeMap
     * @param start
     * @param finish
     * @param threadCount
     * @return
     */
    static Map<String, MoexInfo> loadHistoryEnginesMarketsBoardsListing(Map<String, Map<String, String>> codeMap, int start, int finish, int threadCount) {
        Map<String, MoexInfo> res = new HashMap<>();
        List<Thread> threads = new ArrayList<>();
        for (String code : codeMap.keySet()) {
            Map<String, String> map = codeMap.get(code);
            String engine = map.get("engine");
            String market = map.get("market");
            String board = map.get("board");

            Thread thread = new Thread(() -> {
                Moex moex = loadHistoryEnginesMarketsBoardsListing(engine, market, board, start, finish);
                moex.relocate();
                MoexInfo info = moex.get("securities");
                res.put(code, info);
            });
            thread.setName("t_" + ServerHelper.getSession());
            threads.add(thread);
        }
        ServerHelper.executeThread(threads, threadCount);
        return res;
    }

    /**
     * To obtain the data for the listing of the securities in the historical context in the specified mode
     * (Получить данные по листингу бумаг в историческом разрезе по указанному режиму)
     * /iss/history/engines/[engine]/markets/[market]/boards/[board]/listing
     *
     * @param engine
     * @param market
     * @param board
     * @param start
     * @param finish
     * @return
     */
    private static Moex loadHistoryEnginesMarketsBoardsListing(String engine, String market, String board, int start, int finish) {
        String pattern = "http://www.micex.ru/iss/history/engines/[engine]/markets/[market]/boards/[board]/listing.xml?lang=ru&start=[start]";
        String url = pattern.replace("[engine]", engine).replace("[market]", market).replace("[board]", board);
        Moex moex = new Moex();
        moex.parseTillEnds(url, start, finish, 0);
        return moex;
    }


    /**
     * To obtain the data for the listing of the securities in the historical context of this group of modes
     * (Получить данные по листингу бумаг в историческом разрезе по указанной группе режимов)
     * /iss/history/engines/[engine]/markets/[market]/boardgroups/[boardgroup]/listing
     *
     * @param codeMap
     * @param start
     * @param finish
     * @param threadCount
     * @return
     */
    static Map<String, MoexInfo> loadHistoryEnginesMarketsBoardgroupsListing(Map<String, Map<String, String>> codeMap, int start, int finish, int threadCount) {
        Map<String, MoexInfo> res = new HashMap<>();
        List<Thread> threads = new ArrayList<>();
        for (String code : codeMap.keySet()) {
            Map<String, String> map = codeMap.get(code);
            String engine = map.get("engine");
            String market = map.get("market");
            String board = map.get("boardgroup");

            Thread thread = new Thread(() -> {
                Moex moex = loadHistoryEnginesMarketsBoardgroupsListing(engine, market, board, start, finish);
                moex.relocate();
                MoexInfo info = moex.get("securities");
                res.put(code, info);
            });
            thread.setName("t_" + ServerHelper.getSession());
            threads.add(thread);
        }
        ServerHelper.executeThread(threads, threadCount);
        return res;
    }


    /**
     * To obtain the data for the listing of the securities in the historical context of this group of modes
     * (Получить данные по листингу бумаг в историческом разрезе по указанной группе режимов)
     * /iss/history/engines/[engine]/markets/[market]/boardgroups/[boardgroup]/listing
     *
     * @param engine
     * @param market
     * @param boardgroup
     * @param start
     * @param finish
     * @return
     */
    private static Moex loadHistoryEnginesMarketsBoardgroupsListing(String engine, String market, String boardgroup, int start, int finish) {
        String pattern = "http://www.micex.ru/iss/history/engines/[engine]/markets/[market]/boardgroups/[boardgroup]/listing.xml?lang=ru&start=[start]";
        String url = pattern.replace("[engine]", engine).replace("[market]", market).replace("[boardgroup]", boardgroup);
        Moex moex = new Moex();
        moex.parseTillEnds(url, start, finish, 0);
        return moex;
    }

    /**
     * To obtain data on curve non-coupon profitability (Получить данные по кривой безкупонной доходности)
     * /iss/engines/(state)/markets/zcyc
     */
    public static void loadZcyc() {
        //todo We must investigated and implemented later

    }

    /**
     * To description and mode of operation of the trading system
     * (Получить описание и режим работы торговой системы)
     * /iss/engines/[engine]
     *
     * @param engines
     * @return
     */
    public static Map<String, List<MoexInfo>> loadEnginesInfo(List<String> engines) {
        String pattern = "http://www.micex.ru/iss/engines/[engine].xml";
        Map<String, MoexInfo> res = new HashMap<>();
        Map<String, List<MoexInfo>> map = load(pattern, "[engine]", engines, 2);

        return map;
    }

    // To obtain a description of the fields for requests for quotes for market
    // (Получить описание полей для запросов стакана котировок для рынка)
    // /iss/engines/[engine]/markets/[market]/.*?orderbook/columns
    public static void loadEnginesMarketsOrderbook() {

    }

   /*
    * A description of fields for queries published by the securities market
    * (Получить описание полей для запросов публикуемых бумаг для рынка)
    * /iss/engines/[engine]/markets/[market]/.*?securities/columns
    *
    * */

    static Map<String, Moex> loadEnginesMarketsSecurities(MoexType type, MoexObjectType moexObjectType, Map<String, Map<String, String>> codeMap, int threadCount) {
        Map<String, Moex> res = new HashMap<>();
        List<Thread> threads = new ArrayList<>();
        for (String code : codeMap.keySet()) {
            Map<String, String> map = codeMap.get(code);
            String engine = map.get("engine");
            String market = map.get("market");

            Thread thread = new Thread(() -> {
                Moex moex = loadEnginesMarketsSecurities(type, moexObjectType, engine, market);
                res.put(code, moex);
            });
            thread.setName("t_" + ServerHelper.getSession());
            threads.add(thread);
        }
        ServerHelper.executeThread(threads, threadCount);
        return res;
    }


    /*
    * A description of fields for queries published by the securities market
    * (Получить описание полей для запросов публикуемых бумаг для рынка)
    * /iss/engines/[engine]/markets/[market]/.*?securities/columns

    * Получить описание полей для запросов исторических данных по бумагам для рынка.
    * (A description of fields for queries of historical data on securities for the market.)
    * /iss/history/engines/[engine]/markets/[market]/.*?securities/columns
    */

    public static Moex loadEnginesMarketsSecurities(MoexType type, MoexObjectType moexObjectType, String engine, String market) {
        String pattern = "http://www.micex.ru/iss/" + type.getUrlQuery() + "engines/[engine]/markets/[market]/" + moexObjectType.getUrlQuery() + "/columns.xml";
        String url = pattern.replace("[engine]", engine).replace("[market]", market);
        Moex moex = new Moex();
        List<MoexInfo> list = moex.parseXml(url);
        moex.setMoexInfoList(list);
        moex.relocate();
        return moex;
    }


    public static Moex loadEnginesMarkets(MoexType type, String engine, String market) {
        String pattern = "http://www.micex.ru/iss/engines/[engine]/markets/[market]";
        if (type == MoexType.HISTORY) {
            pattern = "http://www.micex.ru/iss/history/engines/[engine]/markets/[market]/securities/columns.xml";
        }
        String url = pattern.replace("[engine]", engine).replace("[market]", market);
        Moex moex = new Moex();
        List<MoexInfo> list = moex.parseXml(url);
        moex.setMoexInfoList(list);
        moex.relocate();
        return moex;
    }


    //   /iss/engines/[engine]/markets/[market]/.*?trades/columns


}
