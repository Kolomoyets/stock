package ru.iswt.server.utils;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;


public class ServerHelper {
    static Logger LOG = LogManager.getLogger(ServerHelper.class);

    private static volatile int session;
    private static Parameters builderParams = new Parameters();
    private static FileBasedConfigurationBuilder<FileBasedConfiguration> builder;
    public static Configuration configuration;
    private static String cfgFile;

    public static void loadProperties() {
        try {
            cfgFile = "config.xml";
            File file = new File(cfgFile);
            builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>(XMLConfiguration.class)
                    .configure(builderParams.xml().setFile(file));
            configuration = builder.getConfiguration();
            if (configuration != null) {
                String server_name = configuration.getString("server.name");
                configuration.size();
            }

        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

    public static synchronized int getSession() {
        session++;
        return session;
    }


    public static void saveProperties() {


    }

    public static void executeThread(List<Thread> threads, int threadCount) {
        executeThreadList(threads, 20, 1000);

        /*

        ExecutorService service = Executors.newFixedThreadPool(2);

        for (Thread thread : threads) {
            //thread.start();
            //service.execute(thread);
            Future future = service.submit(thread);
            try {
                future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        */


    }

    public static Thread getAnyThread(List<Thread> list, Thread.State state) {
        for (Thread t : list) {
            if (t.getState() == state) {
                return t;
            }
        }
        return null;
    }

    public static void executeThreadList(List<Thread> list, int count_thread, int timeSleep) {
        while (true) {
            int count = 0;
            ArrayList<Thread> arrayList = new ArrayList<Thread>();
            for (Thread t : list) {
                if (t.getState() == Thread.State.TERMINATED) {
                    arrayList.add(t);
                }
            }

            list.removeAll(arrayList);
            for (Thread t : list) {
                if (t.getState() != Thread.State.TERMINATED &&
                        t.getState() != Thread.State.NEW) {
                    count++;
                }
            }
            for (int i = count; i < count_thread; i++) {
                Thread thread = getAnyThread(list, Thread.State.NEW);
                if (thread != null) {
                    thread.start();
                }
            }

            boolean b = true;
            for (Thread t : list) {
                if (t.getState() != Thread.State.TERMINATED) {
                    b = false;
                    break;
                }
            }

            if (b) {
                break;
            }

            try {
                LOG.info(getThreadStatistics(list));
                Thread.sleep(timeSleep);

            } catch (InterruptedException e) {
                LOG.error(e);
            }
        }
    }

    public static String getThreadStatistics(List<Thread> list) {
        int cNEW = 0;
        int cRUNNABLE = 0;
        int cBLOCKED = 0;
        int cWAITING = 0;
        int cTIMED_WAITING = 0;
        int cTERMINATED = 0;

        for (Thread t : list) {
            if (t.getState() == Thread.State.NEW) {
                cNEW++;
            }
            if (t.getState() == Thread.State.RUNNABLE) {
                cRUNNABLE++;
            }
            if (t.getState() == Thread.State.BLOCKED) {
                cBLOCKED++;
            }
            if (t.getState() == Thread.State.WAITING) {
                cWAITING++;
            }
            if (t.getState() == Thread.State.TIMED_WAITING) {
                cTIMED_WAITING++;
            }
            if (t.getState() == Thread.State.TERMINATED) {
                cTERMINATED++;
            }
        }
        return "cNEW=" + cNEW + ",cRUNNABLE=" + cRUNNABLE + ",cBLOCKED=" + cBLOCKED + ",cWAITING=" + cWAITING + ",cTIMED_WAITING=" + cTIMED_WAITING + ",cTERMINATED=" + cTERMINATED; //NON-NLS
    }

    public static Date addDays(Date curr, int countDay) {
        return new Date(curr.getTime() + 60 * 60 * 24 * 1000 * countDay);
    }

    public static Date String2Date(String value) {
        return getDateByFormat(value, "dd.MM.yyyy");
    }

    public static Date getDateByFormat(String value, String format) {
        Date date = null;
        try {
            return new SimpleDateFormat(format).parse(value);
        } catch (Exception e) {
            LOG.error("value=" + value + ",format" + format, e);
        }
        return date;
    }


    public static Map<String, String> String2Map(String group) {
        return String2Map(group, ",", "=");
    }

    public static Map<String, String> String2Map(String group, String separator, String separatorValues) {
        Map<String, String> hashMap = new HashMap<>();
        String[] strings = group.split(separator);
        for (String codeValue : strings) {
            String[] strings1 = codeValue.split(separatorValues);
            hashMap.put(strings1[0], strings1[1]);
        }
        return hashMap;
    }

    public static Map<String, String> ListString2Map(List<String> marketList, String separatorGroup, String separatorValue, String filedCode, String filedValue) {
        Map<String, String> map = new HashMap<>();
        for (String s : marketList) {
            Map<String, String> values = String2Map(s, separatorGroup, separatorValue);
            map.put(values.get(filedCode), values.get(filedValue));
            values.size();
        }
        return map;
    }

    public static String Map2String(Map<String, String> res, String separator, String... fileds) {
        String s = "";
        for (String code : fileds) {
            s = s + code + "=" + res.get(code) + separator;
        }
        s = s.substring(0, s.length() - 1);
        return s;
    }
}
