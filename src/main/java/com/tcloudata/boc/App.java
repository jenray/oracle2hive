package com.tcloudata.boc;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by jenray on 2017/1/3.
 */
public class App {
    private HashSet<String> dbType = new HashSet<String>();
    private String baseDir = "/Users/jenray/Downloads/boc/1/";
    //private String baseDir = "/Users/jenray/Downloads/boc/sql/sql/21.148.4.56/";
    //private String baseDir = "/Users/jenray/Downloads/boc/sql/sql/21.148.8.175/";
    private String errorFile = baseDir + "error.txt";
    private HashSet<String> tableSet = new HashSet<String>();

    private static final boolean isChinese(char c) {
        Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
        if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
                || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
                || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
                || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION
                || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
                || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
            return true;
        }
        return false;
    }

    public static final boolean isChinese(String strName) {
        char[] ch = strName.toCharArray();
        for (int i = 0; i < ch.length; i++) {
            char c = ch[i];
            if (isChinese(c)) {
                return true;
            }
        }
        return false;
    }

    private String hiveType(String type) {
        type = type.toUpperCase();
        if (type.startsWith("CHAR")) {
            if (type.matches(".*[0-9]+.*")) {
                String numStr = type.replace("CHAR(", "");
                numStr = numStr.replace(")", "");
                int i = Integer.parseInt(numStr);
                if (i > 32) {
                    return "string";
                }
            }
            return type.toLowerCase();
        }
        if (type.startsWith("NUMBER")) {
            if (type.startsWith("NUMBER(1)") ||
                    type.startsWith("NUMBER(2)") ||
                    type.startsWith("NUMBER(3)") ||
                    type.startsWith("NUMBER(4)") ||
                    type.startsWith("NUMBER(5)") ||
                    type.startsWith("NUMBER(6)") ||
                    type.startsWith("NUMBER(7)") ||
                    type.startsWith("NUMBER(8)"))
                return "INT";
            if (type.matches(".*[0-9]+,[0-9]+.*")) {
                String numStr = type.replace("NUMBER(", "");
                numStr = numStr.replace(")", "");
                String[] numSa = numStr.split("[^0-9]+");
                int i1 = Integer.parseInt(numSa[0]);
                int i2 = Integer.parseInt(numSa[1]);
                if (i1 <= i2) {
                    i1 += i2;
                }
                return String.format("DECIMAL(%d,%d)", i1, i2);
            }
            return type.replaceAll("NUMBER", "DECIMAL").toLowerCase();
        }
        if (type.startsWith("DATE")) {
            return type.toLowerCase();
        }
        if (type.startsWith("TIMESTAMP")) {
            return "timestamp";
        }
        if (type.startsWith("FLOAT")) {
            return type.toLowerCase();
        }
        if (type.startsWith("DOUBLE")) {
            return type.toLowerCase();
        }
        if (type.startsWith("INTEGER")) {
            return type.replaceAll("INTEGER", "INT").toLowerCase();
        }
        if (type.startsWith("LONG")) {
            return type.replaceAll("LONG", "BIGINT").toLowerCase();
        }
        return "string";
    }

    private LinkedHashMap<String, String> processFields(List<String> lines) {
        LinkedHashMap<String, String> fieldMap = new LinkedHashMap<String, String>();
        System.out.println("++++++++++ processFields ++++++++++");
        for (String line : lines) {
            //System.out.println("++ " + line);
            line = line.replaceAll("[ ]+", " ");
            line = line.trim();

            String[] strArray = line.split(" ");
            String field = strArray[0].trim();
            if (isChinese(field) || field.contains("(") || field.contains(".")) return null;
            String type = strArray[1].trim();
            //type = type.replaceAll("\\(.*?\\)", "").replaceAll("[^a-zA-Z]+", "");
            if (type.endsWith(",")) {
                type = type.substring(0, type.lastIndexOf(","));
            }
            fieldMap.put(field, type);
            dbType.add(type);
        }
        for (Map.Entry<String, String> entry : fieldMap.entrySet()) {
            System.out.println("++++ " + entry.getKey() + "\t" + entry.getValue() + "\t" + hiveType(entry.getValue()));
        }
        return fieldMap;
    }

    private LinkedHashMap<String, String> processComment(List<String> lines) {
        LinkedHashMap<String, String> fieldMap = new LinkedHashMap<String, String>();
        StringBuilder sb = new StringBuilder();
        System.out.println("__________ Comment Start __________");
        for (String line : lines) {
            System.out.println(line);
            sb.append(line);
        }
        System.out.println("__________ Comment End __________");
        String commentLine = sb.toString().trim();
        if (commentLine == null || commentLine.length() == 0) return null;
        commentLine = commentLine.replaceAll("comment on table", " ");
        commentLine = commentLine.replaceAll("comment on column", " ");
        commentLine = commentLine.replaceAll(" is ", " ");
        commentLine = commentLine.replaceAll("[ \\\\\"]+", " ");

        //System.out.println(comment);
        for (String fieldComment : commentLine.split("';")) {
            fieldComment = fieldComment.trim();
            if (fieldComment.contains("'")) {
                String[] items = fieldComment.split(" '");
                if (items.length < 2) {
                    System.out.println("--------- processComment[" + fieldComment + "]----------");
                } else {
                    String field = items[0];
                    field = field.trim().toLowerCase();
                    String comment = items[1];
                    comment = comment.replaceAll(";", "；");
                    comment = comment.replaceAll("[ \t\r\n]+", " ").trim();
                    fieldMap.put(field, comment);
                }
            } else {
                System.out.println("--------- processComment[" + fieldComment + "]---------");
            }
        }
        System.out.println("------------------ processComment ------------------");
        for (Map.Entry<String, String> entry : fieldMap.entrySet()) {
            System.out.println(entry.getKey() + "\t" + entry.getValue());
        }
        return fieldMap;
    }

    private String generateHql(String db, String table, LinkedHashMap<String, String> fieldMap, LinkedHashMap<String, String> commentMap) {
        StringBuilder sb = new StringBuilder("create table if not exists ");
        sb.append(String.format("%s_%s (\r\n", db, table.toLowerCase()));
        for (Map.Entry<String, String> fieldEntry : fieldMap.entrySet()) {
            String fieldStr = String.format("`%s` %s", fieldEntry.getKey(), hiveType(fieldEntry.getValue()));
            String commentKey = String.format("%s.%s", table, fieldEntry.getKey()).toLowerCase();

            if (commentMap != null && commentMap.containsKey(commentKey)) {
                fieldStr += String.format(" comment '%s',\r\n", commentMap.get(commentKey));

            } else {
                fieldStr += ",\r\n";
            }
            sb.append(fieldStr);
        }
        sb.deleteCharAt(sb.length() - 3);
        sb.append(")");
        if (commentMap != null && commentMap.containsKey(table.toLowerCase())) {
            sb.append(String.format("comment '%s'", commentMap.get(table.toLowerCase())));
        }
        sb.append(";\r\n");
        return sb.toString();
    }

    private String processTable(List<String> lines, String db) {
        if (lines == null || lines.isEmpty() || !lines.get(0).startsWith("create ")) return "";
        List<String> fieldList = new ArrayList<String>();
        List<String> commentList = new ArrayList<String>();
        LinkedHashMap<String, String> fieldMap = null, commentMap = null;
        boolean isField = false;
        boolean isComment = false;
        String table = null;
        System.out.println("========== processTable ==========");
        for (String line : lines) {

            //System.out.println("== " + line);
            if (line.startsWith("create ") && line.contains(" table ")) {
                table = line.replaceAll("create.*?table", "").trim();
                System.out.println("@@@@ " + table + " @@@@");
                isField = true;
                continue;
            }
            if (line.equals("(")) {
                continue;
            } else if (line.equals(")")) {
                isField = false;
                fieldMap = processFields(fieldList);
                fieldList = new ArrayList<String>();
                continue;
            } else if (line.equals(");")) {
                isField = false;
                fieldMap = processFields(fieldList);
                fieldList = new ArrayList<String>();
                isComment = true;
                continue;
            }
            if (line.endsWith(";")) {
                isComment = true;
                continue;
            }
            if (isField) {
                fieldList.add(line);
            }
            if (isComment) {
                System.out.println("+++ " + line);
                commentList.add(line);
            }
        }
        if (fieldMap == null) {
            System.out.println("==================== fieldMap is Null[" + table + "]====================");
            try {
                FileUtils.write(new File(errorFile), String.format("!!!!!!!!%s.%s!!!!!!!!\r\n", db, table), "UTF-8", true);
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
            return null;
        } else {
            commentMap = processComment(commentList);
            tableSet.add(db + "_" + table.toLowerCase());
            return generateHql(db, table, fieldMap, commentMap);
        }
    }

    public void ProcessFile(File file, String db) {
        try {

            List<String> lines = FileUtils.readLines(file, "GBK");
            List<String> tableList = new ArrayList<String>();
            String hql = null;
            boolean skipLine = false;
            boolean margeLine = false;
            StringBuilder margeSb = new StringBuilder();
            for (String line : lines) {
                //System.out.println(line);
                if (line != null && line != "") {
                    line = line.trim();
                } else {
                    continue;
                }
                if (line.startsWith("prompt ") ||
                        line.startsWith("--") ||
                        line.startsWith("spool ") ||
                        line.startsWith("alter table ") ||
                        line.contains("add constraint") ||
                        line.contains("partition ") ||
                        line.contains("tablespace ") ||
                        line.contains("add primary key ") ||
                        line.contains("add foreign key ") ||
                        line.contains("disable;") ||
                        line.contains("references ") ||
                        line.contains("grant ") ||
                        line.contains("compress;") || skipLine) {
                    System.out.println("**i " + line);
                    continue;
                }
                if ((line.startsWith("create ") && line.contains(" index "))) {
                    System.out.println("**i " + line);
                    skipLine = !line.endsWith(";");
                    continue;
                }
                if (skipLine && line.endsWith(";")) {
                    System.out.println("**i " + line);
                    skipLine = false;
                    continue;
                }
                System.out.println("** " + line);
                if (margeLine) {
                    margeSb.append(" " + line);
                    System.out.println("**mi " + line);
                    continue;
                }
                if (line.startsWith("comment on ")) {
                    margeLine = !line.endsWith(";");
                    margeSb = new StringBuilder(line);
                    System.out.println("**mi " + line);
                    continue;
                }
                if (margeLine && line.endsWith(";")) {
                    line = margeSb.toString();
                    System.out.println("**m " + line);
                    margeLine = false;
                }
                if (line.startsWith("create ") && line.contains("table ")) {
                    if (tableList == null) {
                        tableList = new ArrayList<String>();
                    } else if (!tableList.isEmpty()) {
                        hql = processTable(tableList, db);
                        if (hql != null && hql.length() > 0) {
                            System.out.println("******************** ProcessTable Done ********************");
                            System.out.println(hql);
                            outputHql(db, hql);
                        }
                        tableList = new ArrayList<String>();

                    }
                }
                tableList.add(line);
            }
            if (tableList != null && !tableList.isEmpty()) {
                hql = processTable(tableList, db);
                tableList = new ArrayList<String>();
                if (hql != null && hql.length() > 0) {
                    System.out.println("******************** ProcessTable Done ********************");
                    System.out.println(hql);
                    outputHql(db, hql);
                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void outputHql(String db, String hql) {
        try {
            FileUtils.write(new File(baseDir + db + ".hql"), hql, "UTF-8", true);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public static void main(String[] args) {

        //dapdw.sql dapsource.sql daphis.sql
        try {

            App app = new App();
            FileUtils.write(new File(app.errorFile), "####################\r\n", "UTF-8", false);
            for (File file : FileUtils.listFiles(new File(app.baseDir), new String[]{"sql"}, false)) {
                String db = file.getName().replaceAll(".sql", "");
                System.out.println("############################################################");
                System.out.println("#################### ProcessDB " + db + " ####################");
                System.out.println("############################################################");
                FileUtils.write(new File(app.baseDir + db + ".hql"), "", "UTF-8", false);
                app.ProcessFile(file, db);
            }

            FileUtils.write(new File(app.baseDir + "drop.hql"), "", "UTF-8", false);
            FileUtils.write(new File(app.baseDir + "count.hql"), "", "UTF-8", false);
            FileUtils.write(new File(app.baseDir + "sqoop.txt"), "", "UTF-8", false);
            for (String table : app.tableSet) {
                String line = table.replaceFirst("_.*", "").toUpperCase() + "|" + table.replaceFirst("_", ".").toUpperCase() + "|" + table + "\r\n";
                String dropSql = String.format("DROP TABLE IF EXISTS %s;\r\n", table);
                String countSql = String.format("select '%s' as t,count(*) as c from %s;\r\n", table, table);
                //System.out.print(line);
                FileUtils.write(new File(app.baseDir + "sqoop.txt"), line, "UTF-8", true);
                FileUtils.write(new File(app.baseDir + "drop.hql"), dropSql, "UTF-8", true);
                FileUtils.write(new File(app.baseDir + "count.hql"), countSql, "UTF-8", true);
            }
            /*
            for (String type : app.dbType) {
                System.out.println(type);
            }
            */
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
