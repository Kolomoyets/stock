package ru.iswt.server.stock.moex;

import org.junit.Before;
import org.junit.Test;
import ru.iswt.server.utils.ServerHelper;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class MoexSecuritiesTest {
    static String code = "securities";
    static Moex moex;
    static MoexInfo info;
    static Integer threadCount;

    @Before
    public void initLoad() {
        ServerHelper.loadProperties();
        threadCount = ServerHelper.configuration.getInt(MoexParams.ThreadCount.code);
        moex = MoexHelp.loadSecurities(0, 100, 20);
        info = moex.get(code);
    }


    @Test
    public void loadSecuritiesCodeTest() {
        List<String> list = info.getValue("secid");
        MoexHelp.loadSecuritiesCodes(list, threadCount);
        assertEquals(info.getRows().size(), list.size());
    }


    @Test
    public void loadSecuritiesIndicesTest() {
        List<String> list = info.getValue("secid");
        MoexHelp.loadSecuritiesIndices(list, threadCount);
        assertEquals(info.getRows().size(), list.size());
    }


    @Test
    public void loadSecuritiesAggregatesTest() {
        List<String> list = info.getValue("secid");
        MoexHelp.loadSecuritiesAggregates(list, threadCount);
        assertEquals(info.getRows().size(), list.size());
    }




    @Test
    public void loadEnginesTest() {
        assertNotNull("No object ", info);
        assertEquals("Information about the ID of the object is not loaded", info.getId(), code);
    }
}
