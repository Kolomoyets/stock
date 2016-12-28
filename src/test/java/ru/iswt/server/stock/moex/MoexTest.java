/*
*
* */
package ru.iswt.server.stock.moex;

import org.apache.commons.lang3.text.StrSubstitutor;
import org.junit.Before;
import org.junit.Test;
import ru.iswt.server.utils.ServerHelper;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MoexTest {
    static Moex moex;
    static Integer threadCount;


    @Before
    public void initLoad() {
        ServerHelper.loadProperties();
        threadCount = ServerHelper.configuration.getInt(MoexParams.ThreadCount.code);
        moex = MoexHelp.loadGlobalReference();
    }

    @Test
    public void generateTest() {
        HashMap<String, List<String>> hashMap = new HashMap<String, List<String>>();
        {
            List<String> list = new ArrayList<String>();
            list.add("01");
            list.add("02");
            list.add("03");
            hashMap.put("market", list);
            list = new ArrayList<String>();
            list.add("11");
            list.add("12");
            list.add("13");
            hashMap.put("enegy", list);
            list = new ArrayList<String>();
            list.add("21");
            list.add("22");
            list.add("23");
            hashMap.put("rule", list);
        }
        Map map = MoexHelp.generateUrl(hashMap);
        assertEquals("generateUrl-27", map.size(), 27);
    }


    @Test
    public void loadGlobalReferenceTest() {
        assertEquals("engines-Meta-size", moex.get("engines").getMeta().size(), 3);
        assertEquals("engines-Rows-size", moex.get("engines").getRows().size(), 6);
        assertEquals("markets-Meta-size", moex.get("markets").getMeta().size(), 8);
        assertEquals("markets-Rows-size", moex.get("markets").getRows().size(), 23);
        assertEquals("boards-Meta-size", moex.get("boards").getMeta().size(), 9);
        assertEquals("boards-Rows-size", moex.get("boards").getRows().size(), 155);
        assertEquals("boardgroups-Meta-size", moex.get("boardgroups").getMeta().size(), 11);
        assertEquals("boardgroups-Rows-size", moex.get("boardgroups").getRows().size(), 67);
        assertEquals("durations-Meta-size", moex.get("durations").getMeta().size(), 5);
        assertEquals("durations-Rows-size", moex.get("durations").getRows().size(), 7);
        assertEquals("securitytypes-Meta-size", moex.get("securitytypes").getMeta().size(), 6);
        assertEquals("securitytypes-Rows-size", moex.get("securitytypes").getRows().size(), 30);
        assertEquals("securitygroups-Meta-size", moex.get("securitygroups").getMeta().size(), 4);
        assertEquals("securitygroups-Rows-size", moex.get("securitygroups").getRows().size(), 15);
        assertEquals("securitycollections-Meta-size", moex.get("securitycollections").getMeta().size(), 4);
        assertEquals("securitycollections-Rows-size", moex.get("securitycollections").getRows().size(), 81);
    }


    @Test
    public void globalReferenceEnginesTest() {
        MoexInfo info = moex.get("engines");
        assertNotNull(info);
        assertEquals(info.getId(), "engines");
    }

    @Test
    public void loadMarketsTest() {
        MoexInfo info = moex.get("markets");
        assertNotNull(info);
        assertEquals(info.getId(), "markets");
    }


    @Test
    public void loadTurnoversTest() {
        Map<String, List<MoexInfo>> map = MoexHelp.loadTurnovers(false, ServerHelper.String2Date("01.01.2016"), ServerHelper.String2Date("31.01.2016"), threadCount);
        Map<String, List<MoexInfo>> map2 = MoexHelp.loadTurnovers(true, ServerHelper.String2Date("01.01.2016"), ServerHelper.String2Date("31.01.2016"), threadCount);
        assertTrue(map.size() == 30);
        assertTrue(map2.size() == 30);
    }

    @Test
    public void loadEnginesTurnoversTest() {
        MoexInfo info = moex.get("engines");
        List<String> list = info.getValue("name");
        Map<String, List<MoexInfo>> map = MoexHelp.loadEnginesTurnovers(list, true, null, threadCount);
    }


    @Test
    public void loadEnginesMarketsTurnoversTest() {
        MoexInfo infoEngines = moex.get("engines");
        MoexInfo infoMarkets = moex.get("markets");
        Map<String, List<MoexInfo>> map = MoexHelp.loadEnginesMarketsTurnovers(infoEngines.getValue("name"), infoMarkets.getValue("market_name"), 2);
        assertTrue(map.size() > 0);
    }


    @Test
    public void loadHistoryEnginesMarketsListingTest() {
        MoexInfo infoMarkets = moex.get("markets");
        List<String> strings = infoMarkets.getValueGroup("trade_engine_name", "market_name");

        Map<String, Map<String, String>> codeMap = new HashMap<String, Map<String, String>>();
        for (String s : strings) {
            s = s.replace("trade_engine_name", "[engine]");
            s = s.replace("market_name", "[market]");
            Map<String, String> map = ServerHelper.String2Map(s);
            codeMap.put(s, map);
        }


        Map<String, List<MoexInfo>> map = MoexHelp.loadHistoryEnginesMarketsListing(codeMap, 0, 500, 5);
        assertTrue(map.size() > 0);
    }

    @Test
    public void loadHistoryEnginesMarketsBoardsListingTest() {
        MoexInfo infoEngines = moex.get("engines");
        MoexInfo infoMarkets = moex.get("markets");
        MoexInfo infoBoards = moex.get("boards");

        List<String> strings = infoBoards.getValueGroup("engine_id", "market_id", "boardid");
        // List<String> strings = infoMarkets.getValueGroup("trade_engine_name", "market_name");

        List<String> marketList = infoMarkets.getValueGroup("id", "market_name");
        List<String> engineList = infoEngines.getValueGroup("id", "name");

        Map<String, String> markets = ServerHelper.ListString2Map(marketList, ",", "=", "id", "market_name");
        Map<String, String> engines = ServerHelper.ListString2Map(engineList, ",", "=", "id", "name");

        Map<String, Map<String, String>> codeMap = new HashMap<String, Map<String, String>>();
        for (String s : strings) {
            s = s.replace("trade_engine_name", "[engine]");
            s = s.replace("market_name", "[market]");
            Map<String, String> map = ServerHelper.String2Map(s);

            Map<String, String> res = new HashMap<>();
            res.put("engine", engines.get(map.get("engine_id")));
            res.put("market", markets.get(map.get("market_id")));
            res.put("board", map.get("boardid"));

            String code = ServerHelper.Map2String(res, ",", "engine", "market", "board");

            codeMap.put(code, res);
        }

        Map<String, MoexInfo> map = MoexHelp.loadHistoryEnginesMarketsBoardsListing(codeMap, 0, 500, 5);
        assertTrue(map.size() > 0);
    }


    @Test
    public void loadHistoryEnginesMarketsBoardgroupsListingTest() {
        MoexInfo infoBoards = moex.get("boardgroups");
        List<String> strings = infoBoards.getValueGroup("trade_engine_name", "market_name", "board_group_id");

        Map<String, Map<String, String>> codeMap = new HashMap<String, Map<String, String>>();
        for (String s : strings) {

            String s2 = s.replace("trade_engine_name=", "engine=");
            s2 = s2.replace("market_name=", "market=");
            s2 = s2.replace("board_group_id=", "boardgroup=");

            Map<String, String> res = ServerHelper.String2Map(s2);
            String code = ServerHelper.Map2String(res, ",", "engine", "market", "boardgroup");
            codeMap.put(s2, res);
        }

        Map<String, MoexInfo> map = MoexHelp.loadHistoryEnginesMarketsBoardgroupsListing(codeMap, 0, 500, 5);
        assertTrue(map.size() > 0);
    }


    @Test
    public void loadEnginesInfoTest() {
        MoexInfo info = moex.get("engines");
        List<String> strings = info.getValue("name");
        Map<String, List<MoexInfo>> map = MoexHelp.loadEnginesInfo(strings);
        assertTrue(map.size() > 0);
    }


    @Test
    public void loadEnginesMarketsSecuritiesTest() {
        MoexInfo infoBoards = moex.get("markets");
        List<String> strings = infoBoards.getValueGroup("trade_engine_name", "market_name");
        Map<String, Map<String, String>> codeMap = new HashMap<String, Map<String, String>>();
        for (String s : strings) {
            String s2 = s.replace("trade_engine_name=", "engine=");
            s2 = s2.replace("market_name=", "market=");
            Map<String, String> res = ServerHelper.String2Map(s2);
            codeMap.put(s2, res);
        }
        Map<String, Moex> map = MoexHelp.loadEnginesMarketsSecurities(MoexType.ACTUAL,codeMap, 5);
        assertTrue(map.size() > 0);
    }



    @Test
    public void loadHistoryEnginesMarketsSecuritiesTest() {
        MoexInfo infoBoards = moex.get("markets");
        List<String> strings = infoBoards.getValueGroup("trade_engine_name", "market_name");
        Map<String, Map<String, String>> codeMap = new HashMap<String, Map<String, String>>();
        for (String s : strings) {
            String s2 = s.replace("trade_engine_name=", "engine=");
            s2 = s2.replace("market_name=", "market=");
            Map<String, String> res = ServerHelper.String2Map(s2);
            codeMap.put(s2, res);
        }
        Map<String, Moex> map = MoexHelp.loadEnginesMarketsSecurities(MoexType.HISTORY,codeMap, 5);
        assertTrue(map.size() > 0);
    }


    @Test
    public void load() {

        MoexHelp.loadZcyc();
        // /iss/engines/(state)/markets/zcyc
        assertTrue(true);
    }
}
