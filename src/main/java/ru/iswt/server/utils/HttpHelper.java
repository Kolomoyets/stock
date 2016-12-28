package ru.iswt.server.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;
import ru.iswt.server.stock.moex.Moex;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.Date;

public class HttpHelper {
    static Logger LOG = LogManager.getLogger(HttpHelper.class);

    public static Element parse(String url) {
        DocumentBuilder xml = null;
        try {
            xml = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document doc = xml.parse(url);
            Element element = doc.getDocumentElement();
            LOG.debug("Load url="+ url);
            return element;
        } catch (ParserConfigurationException | SAXException | IOException e) {
            LOG.error(e);
        }
        return null;
    }
}
