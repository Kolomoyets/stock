package ru.iswt.server.stock.moex;

import com.sun.istack.internal.NotNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import ru.iswt.server.utils.HttpHelper;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;


public class Moex {
    static Logger LOG = LogManager.getLogger(Moex.class);
    private List<MoexInfo> moexInfoList = new ArrayList<MoexInfo>(); // Список элементов для
    private HashMap<String, MoexInfo> mapMoexInfo = new HashMap<>();

    private Connection connection;

    /**
     * Конструктор по умолчанию
     */
    public Moex() {

    }

    /**
     * @param url url
     */
    public List<MoexInfo> parseXml(String url) {
        List<MoexInfo> list = new ArrayList<>();

        Element root = HttpHelper.parse(url);
        List<Node> nodes = findNodes(root, "data");

        for (Node node : nodes) {
            MoexInfo moexInfo = new MoexInfo();
            moexInfo.setId(getAttributesValue(node, "id"));
            moexInfo.setMeta(parseInfo(node, "columns"));
            moexInfo.setRows(parseInfo(node, "rows"));
            list.add(moexInfo);
        }
        return list;
    }


    /**
     * Парсинг XML документа
     *
     * @return Элемент
     */

    private List<Node> findNodes(Node root, String string) {
        List<Node> nodes = new ArrayList<>();
        findNodes(root, string, nodes);
        return nodes;

    }


    public static String getAttributesValue(Node node, String name) {
        NamedNodeMap attributes = node.getAttributes();
        if (attributes.getLength() > 0) {
            Node nodeId = attributes.getNamedItem(name);
            if (nodeId != null) {
                return nodeId.getNodeValue();
            }
        }
        return null;
    }


    /**
     * Search item by name in the child list
     *
     * @param root   root-node
     * @param string the search string
     * @return The list of found elements
     */
    private void findNodes(Node root, String string, List<Node> nodes) {
        NodeList nodeList = root.getChildNodes();
        if (nodeList != null) {
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (node.getNodeName() != null && node.getNodeName().equals(string)) {
                    nodes.add(node);
                } else {
                    findNodes(node, string, nodes);
                }
            }
        }
    }

    /**
     * Рекурсивный поиск элемента в XML
     *
     * @param root         корневой элемент
     * @param findnodename исковая строка
     * @return элемент
     */

    private Node findNode(Node root, String findnodename) {
        int j = root.getChildNodes().getLength() - 1;
        Node node;
        for (int i = 1; i < j; i += 2) {
            node = root.getChildNodes().item(i);
            String nodename = node.getNodeName();
            if (findnodename.equals(nodename)) {
                return node;
            } else {
                Node node_res = findNode(node, findnodename);
                if (node_res != null) {
                    return node_res;
                }
            }
        }
        return null;
    }

    /**
     * Загрузка данных
     *
     * @param root Корневой элемент
     */

    private MoexInfo parse(Node root) {
        MoexInfo moexInfo = new MoexInfo();
        NamedNodeMap map = root.getAttributes();
        assert map.getLength() > 0 : new RuntimeException("");
        Node node = map.item(0);
        assert node != null : new RuntimeException("");


        List<HashMap<String, String>> metadata = parseInfo(root, "columns");
        moexInfo.setMeta(metadata);

        List<HashMap<String, String>> rowchain = parseInfo(root, "rows");
        moexInfo.setRows(rowchain);

        return moexInfo;
    }


    private List<HashMap<String, String>> parseInfo(Node root, String nodename) {
        List<HashMap<String, String>> data = new ArrayList<HashMap<String, String>>();
        Node node = findNode(root, nodename);
        if (node == null) {
            return data;
        }

        NodeList nodes = node.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            Node ChNode1 = nodes.item(i);
            NamedNodeMap map = ChNode1.getAttributes();
            if (map != null) {
                HashMap<String, String> map1 = new HashMap<String, String>();
                for (int r = 0; r < map.getLength(); r++) {
                    Node Attrib1 = ChNode1.getAttributes().item(r);
                    map1.put(Attrib1.getNodeName(), Attrib1.getNodeValue());
                }
                data.add(map1);
            }
        }
        return data;
    }

   /* private List<HashMap<String, String>> parseInfo2(Node root, String nodename) {
        Node nodeRows = findNode(root, nodename);
        List<HashMap<String, String>> rowchain = new ArrayList<HashMap<String, String>>(); // Данные
        for (int j = 1; j < (nodeRows.getChildNodes().getLength() - 1); j += 2) {
            Node ChNode2 = nodeRows.getChildNodes().item(j);
            HashMap<String, String> map1 = new HashMap<String, String>();
            for (int r = 0; r < ChNode2.getAttributes().getLength(); r++) {
                Node Attrib2 = ChNode2.getAttributes().item(r);
                map1.put(Attrib2.getNodeName(), Attrib2.getNodeValue());
            }
            rowchain.add(map1);
        }
        return rowchain;
    }
*/


    /**
     * Сохранение данных в БД (создание таблицы если + вставка данных)
     *
     * @param moexInfo -- Информация
     * @param PKName   -- Поле которое является первичных ключом
     */
    public void save(MoexInfo moexInfo, String PKName) {
      /*  moexInfo.setPkName(PKName);
        String sql = createTable(moexInfo.getMeta(), moexInfo.getFullTablename(), moexInfo.getPkName(), null, null);
        sqlQuery(sql, connection);
        insertData(moexInfo, null, null);
        */
    }

    /**
     * Вставка данных в БД (Формирование sql)
     *
     * @param moexInfo   --
     * @param fieldname
     * @param fieldvalue
     */
    private void insertData(MoexInfo moexInfo, String fieldname, String fieldvalue) {
        for (HashMap<String, String> map : moexInfo.getRows()) {
            String data1 = "insert into " + moexInfo.getFullTablename() + " (";
            String data2 = ") values (";

            for (HashMap<String, String> mapField : moexInfo.getMeta()) {
                String snameF = mapField.get("name");
                String svalues = map.get(snameF);

                String stypeF = mapField.get("type");
                data1 += snameF + "_,";
                if (svalues.equals(""))
                    data2 += "null,";
                else
                    data2 += value2insertDb(stypeF, svalues.replace("'", "''")) + ",";
            }
            data1 = data1.substring(0, data1.length() - 1);
            data2 = data2.substring(0, data2.length() - 1);
            if (fieldname != null) {
                data1 += fieldname + ",";
                data2 += fieldvalue + ",";
            }
            String dataIn = data1 + data2 + ")";
            sqlQuery(dataIn, connection);
        }

    }


    /**
     * Создание таблицы в БД
     *
     * @param maps
     * @param tablename    - таблица
     * @param pkName       - Первичный ключ
     * @param addField     - дополнительное поле
     * @param addFieldType - тип дополнительного поля
     * @return SQL строка создания таблицы
     */
    private String createTable(List<HashMap<String, String>> maps, String tablename, String pkName, String addField, String addFieldType) {
        String sql = " create table " + tablename + "(";
        if (addField != null) {
            sql = sql + addField + " " + addFieldType + ",";
        }

        for (HashMap<String, String> map : maps) {
            String sname = map.get("name");
            String stype = convertType2DbType(map.get("type"));
            sql += " " + sname + "_ " + stype + ",";
        }
        sql += " PRIMARY KEY (" + pkName + "_ ) ) ";

        return sql;
    }


    /**
     * Ковертировать тип указанных в XML в БД тип
     *
     * @param sType - Тип
     * @return Тип БД
     */
    private String convertType2DbType(String sType) {
        if (sType.equals("int32")) return "number";
        else if (sType.equals("string")) return "varchar2(1000)";
        else if (sType.equals("int64")) return "number";
        else if (sType.equals("double")) return "number";
        else if (sType.equals("datetime")) return "date2String";
        else
            return sType;
    }


    /**
     * @param sql          - SQL текст для выполнения
     * @param dbConnection - Подключение к БД
     */
    private static void sqlQuery(String sql, Connection dbConnection) {
        Statement stmt = null;
        try {
            stmt = dbConnection.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            int code = e.getErrorCode();
            if (code == 955) { //   ORA-00955: имя уже задействовано для существующего объекта

            } else if (code == 1) {// ORA-00001: нарушено ограничение уникальности

            } else {
                LOG.error(sql, e);
            }
        } finally {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * Приведение типов для вставки в БД
     *
     * @param sType  - Тип
     * @param sValue - Значение
     * @return
     */
    private static String value2insertDb(String sType, String sValue) {
        if (sType.equals("int32")) return sValue;
        else if (sType.equals("string")) return "'" + sValue + "'";
        else if (sType.equals("int64")) return sValue;
        else if (sType.equals("double")) return sValue;
        else if (sType.equals("datetime")) return "date2String('" + sValue + "','YYYY-MM-DD HH24:MI:SS' )";
        else if (sType.equals("date2String")) return "date2String('" + sValue + "','YYYY-MM-DD'";
        else
            return sType;
    }


    /**
     * Разобрать до конца
     *
     * @param pattern - URL
     * @param start   - Начало
     * @param limit   - ограницение
     */
    public void parseTillEnds(@NotNull String pattern, @NotNull int start, int finish, int limit) {
        int row;
        int pos = start;
        String url = null;
        boolean b = true;
        if (finish == 0) finish = Integer.MAX_VALUE;
        if (limit == 0) limit = 100;

        MoexInfo moexInfo = new MoexInfo();
        try {
            do {
                long a0 = new Date().getTime();
                url = pattern.replace("[start]", Integer.toString(pos));
                url = url.replace("[limit]", Integer.toString(limit));
                Element root = HttpHelper.parse(url);
                Node node = findNode(root, "rows");

                if (node == null) {
                    break;
                }

                row = (node.getChildNodes().getLength());
                if (b) {
                    moexInfo.setId(getAttributesValue(findNode(root, "data"), "id"));
                    moexInfo.setMeta(parseInfo(root, "columns"));
                    b = false;
                }

                List<HashMap<String, String>> hashMaps = parseInfo(root, "rows");
                moexInfo.addRows(hashMaps);


                pos += limit;
                if (finish <= pos) {
                    break;
                }
                long a3 = new Date().getTime();
                LOG.debug("time all=" + (a3 - a0) + ", pos=" + pos + ",limit=" + limit);
            } while (row > 1);

            if (moexInfo.getRows()!=null) {
                moexInfoList.add(moexInfo);
            }
        } catch (Exception e) {
            LOG.error(e);
            e.printStackTrace();
        }
    }


    /**
     * Разбор XML и заполнение moexInfoList
     *
     * @param node Корневой элемент
     */
    private void addRows(Node node) {
        NodeList nodes = node.getChildNodes();
        for (int i = 1; i < (nodes.getLength() - 1); i++) {
            Node child = nodes.item(i);
            HashMap<String, String> map = new HashMap<String, String>();

            NamedNodeMap attributes = child.getAttributes();
            for (int r = 0; r < attributes.getLength(); r++) {
                Node item = attributes.item(r);
                map.put(item.getNodeName(), item.getNodeValue());
            }


            moexInfoList.get(0).getRows().add(map);
        }
    }

    public List<MoexInfo> getMoexInfoList() {
        return moexInfoList;
    }

    public void setMoexInfoList(List<MoexInfo> moexInfoList) {
        this.moexInfoList = moexInfoList;
    }

    public void setPrefix(String prefix) {
        for (MoexInfo info : getMoexInfoList()) {
            info.setPrefix(prefix);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public void relocate() {
        for (MoexInfo info : moexInfoList) {
            mapMoexInfo.put(info.getId(), info);
        }
    }

    public MoexInfo get(String id) {
        return mapMoexInfo.get(id);
    }

    public void parseStruct(String templateUrl, int start, int limit) {
        // Загрузка структуры
        {
            String url = templateUrl.replace("@@@", Integer.toString(start));
            url = url.replace("$$$", Integer.toString(limit));


            Element rootel = HttpHelper.parse(url);
            MoexInfo moexInfo = parse(findNode(rootel, "data"));
            moexInfoList.add(moexInfo);
            start += limit;
        }

        for (MoexInfo info : moexInfoList) {
            info.getRows().clear();
        }
    }
}
