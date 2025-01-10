package dbtaobao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class ceshi {
    private static Connection con = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;

    public static ArrayList<String[]> text_1() throws Exception {
        ArrayList<String[]> list = new ArrayList<>();
        String URL = "jdbc:hive2://192.168.211.129:10000";
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection(URL, "hadoop", "qwe123456");
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select * from hour_behavior");
        while (rs.next()) {
            String[] temp = {rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5)};
            list.add(temp);
        }
        return list;
    }

    public static ArrayList<String[]> text_2() throws Exception {
        ArrayList<String[]> list = new ArrayList<>();
        String URL = "jdbc:hive2://192.168.211.129:10000/md";
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection(URL, "hadoop", "qwe123456");
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select * from zhuanhualv");
        while (rs.next()) {
            String[] temp = {rs.getString(6), rs.getString(7), rs.getString(8)};
            list.add(temp);
            System.out.println(list.get(0)[0]);
            System.out.println(list.get(0)[1]);
            System.out.println(list.get(0)[2]);
        }
        return list;
    }

    public static ArrayList<String[]> text_3() throws Exception {
        ArrayList<String[]> list = new ArrayList<>();
        String URL = "jdbc:hive2://192.168.211.129:10000/md";
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection(URL, "hadoop", "qwe123456");
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select * from rijun");
        while (rs.next()) {
            String[] temp = {rs.getString(2), rs.getString(3)};
            list.add(temp);
        }
        return list;
    }

    public static ArrayList<String[]> text_4() throws Exception {
        ArrayList<String[]> list = new ArrayList<>();
        String URL = "jdbc:hive2://192.168.211.129:10000/md";
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection(URL, "hadoop", "qwe123456");
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select * from week");
        while (rs.next()) {
            String[] temp = {rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5)};
            list.add(temp);
        }
        return list;
    }
}