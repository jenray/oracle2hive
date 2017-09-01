package com.tcloudata.boc;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCommentStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.odps.ast.OdpsCreateTableStatement;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleCreateTableStatement;
import com.alibaba.druid.util.JdbcConstants;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class AppDruid {
    private HashSet<String> dbType = new HashSet<String>();
    private String baseDir = "/home/jenray/Downloads/boc/";
    //private String baseDir = "/Users/jenray/Downloads/boc/sql/sql/21.148.4.56/";
    //private String baseDir = "/Users/jenray/Downloads/boc/sql/sql/21.148.8.175/";
    private String errorFile = baseDir + "error.txt";
    private HashSet<String> tableSet = new HashSet<String>();
    public int Counter = 0;

    public static void main1(String[] args) {
        String sql = "create table DAPSF.TB_SFQS_RCPS_TRANLIST\n" +
                "(\n" +
                "  二级行    VARCHAR2(100),\n" +
                "  交易机构号  VARCHAR2(7),\n" +
                "  交易机构名称 VARCHAR2(800),\n" +
                "  报文种类   VARCHAR2(28),\n" +
                "  记录状态   VARCHAR2(13),\n" +
                "  日期     VARCHAR2(8),\n" +
                "  货币     VARCHAR2(3),\n" +
                "  金额     NUMBER(16,2),\n" +
                "  发起行行号  VARCHAR2(14),\n" +
                "  汇款人账号  VARCHAR2(32),\n" +
                "  汇款人名称  VARCHAR2(120),\n" +
                "  接收行行号  VARCHAR2(14),\n" +
                "  收款人账号  VARCHAR2(32),\n" +
                "  收款人名称  VARCHAR2(120),\n" +
                "  入账账号   VARCHAR2(32),\n" +
                "  入账名称   VARCHAR2(120),\n" +
                "  支付交易序号 VARCHAR2(35),\n" +
                "  客户号    VARCHAR2(30),\n" +
                "  来往账标示  VARCHAR2(6)\n" +
                ")\n" +
                "tablespace TBS_DAPSF\n" +
                "  pctfree 10\n" +
                "  initrans 1\n" +
                "  maxtrans 255\n" +
                "  storage\n" +
                "  (\n" +
                "    initial 64K\n" +
                "    next 1M\n" +
                "    minextents 1\n" +
                "    maxextents unlimited\n" +
                "  );";

        AppDruid app = new AppDruid();
        String hql = app.processSQL("test", sql);
        System.out.println(hql);
    }

    public static void main(String[] args) {
        AppDruid app = new AppDruid();
        try {
            FileUtils.write(new File(app.errorFile), "####################\r\n", "UTF-8", false);
            for (File file : FileUtils.listFiles(new File(app.baseDir), new String[]{"sql"}, false)) {
                String db = file.getName().replaceAll(".sql", "");
                System.out.println("############################################################");
                System.out.println("#################### ProcessDB " + db + " ####################");
                System.out.println("############################################################");
                FileUtils.write(new File(app.baseDir + db + ".hql"), "", "UTF-8", false);
                FileUtils.write(new File(app.baseDir + "alter_"+db + ".hql"), "", "UTF-8", false);
                app.ProcessFile(file, db);
            }
            FileUtils.write(new File(app.baseDir + "drop.hql"), "", "UTF-8", false);
            FileUtils.write(new File(app.baseDir + "count.hql"), "", "UTF-8", false);
            FileUtils.write(new File(app.baseDir + "sqoop.txt"), "", "UTF-8", false);
            for (String table : app.tableSet) {
                String line = table.replaceFirst("_.*", "").toUpperCase() + "|" + table.replaceFirst(".*?_", "").toUpperCase() + "|" + table + "\r\n";
                String dropSql = String.format("DROP TABLE IF EXISTS %s;\r\n", table);
                String countSql = String.format("select '%s' as t,count(*) as c from %s;\r\n", table, table);
                FileUtils.write(new File(app.baseDir + "sqoop.txt"), line, "UTF-8", true);
                FileUtils.write(new File(app.baseDir + "drop.hql"), dropSql, "UTF-8", true);
                FileUtils.write(new File(app.baseDir + "count.hql"), countSql, "UTF-8", true);
            }
            System.out.println("[Total Create Table Sql]:" + app.Counter);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void ProcessFile(File file, String db) {
        try {
            List<String> lines = FileUtils.readLines(file, "GBK");
            String hql = null;
            StringBuilder sqlSb = new StringBuilder();
            for (String line : lines) {
                //System.out.println(line);
                if (line != null && line != "") {
                    line = line.trim();
                } else {
                    continue;
                }
                if (line.startsWith("prompt ") ||
                        line.startsWith("--") ||
                        line.startsWith("set ") ||
                        //line.contains("alter table ") ||
                        //line.contains("add constraint ") ||
                        //line.contains("primary key") ||
                        line.startsWith("spool ")) {
                    //System.out.println("** " + line);
                    continue;
                }

                if (line.startsWith("create ") && line.contains("table ")) {
                    this.Counter++;
                    if (sqlSb.length() > 0) {
                        hql = processSQL(db, sqlSb.toString());
                        if (hql != null && hql.length() > 0) {
                            System.out.println("******************** ProcessTable ********************");
                            System.out.println(hql);
                            outputHql(db, hql);
                        }
                        sqlSb = new StringBuilder();

                    }
                }
                sqlSb.append(line + " ");
            }
            if (sqlSb.length() > 0) {
                hql = processSQL(db, sqlSb.toString());
                sqlSb = new StringBuilder();
                if (hql != null && hql.length() > 0) {
                    System.out.println("******************** ProcessTable ********************");
                    System.out.println(hql);
                    outputHql(db, hql);
                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


    private String processSQL(String db, String sql) {
        sql = sql.replaceAll("alter table(.*?);", "");
        sql = sql.replaceAll("create index(.*?);", "");
        System.out.println(sql);
        //格式化输出
        //String result = SQLUtils.format(sql, JdbcConstants.ORACLE);
        //System.out.println("========= Format SQL =========");
        //System.out.println(result);
        ArrayList<String> fieldList = new ArrayList<String>();
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcConstants.ORACLE);

        //解析出的独立语句的个数
        //System.out.println("========= SQLStatement Size is:" + stmtList.size() + " =========");
        SQLExpr tableComment = null;
        String tableName = "";
        HashMap<String, SQLExpr> mapComment = new HashMap<String, SQLExpr>();
        HashMap<String, SQLColumnDefinition> mapField = new HashMap<String, SQLColumnDefinition>();
        for (int i = 0; i < stmtList.size(); i++) {
            SQLStatement stmt = stmtList.get(i);
            if (OracleCreateTableStatement.class.isInstance(stmt)) {
                //System.out.println(stmt);
                OracleCreateTableStatement stmtCreate = (OracleCreateTableStatement) stmt;
                tableName = stmtCreate.getName().getSimpleName().toLowerCase();
                for (SQLTableElement tabEle : stmtCreate.getTableElementList()) {
                    if (SQLColumnDefinition.class.isInstance(tabEle)) {
                        SQLColumnDefinition colDefObj = (SQLColumnDefinition) tabEle;
                        String fieldName = colDefObj.getName().getSimpleName();
                        if (isChinese(fieldName)) {
                            try {
                                FileUtils.write(new File(errorFile), String.format("!!!!!!!!%s.%s!!!!!!!!\r\n", db, tableName), "UTF-8", true);
                            } catch (IOException ioe) {
                                ioe.printStackTrace();
                            }
                            return null;
                        }
                        fieldList.add(fieldName);
                        mapField.put(fieldName, colDefObj);
                    }
                }
            }
            if (SQLCommentStatement.class.isInstance(stmt)) {
                //System.out.println(stmt);
                SQLCommentStatement stmtComment = (SQLCommentStatement) stmt;
                if (stmtComment.getType().name().equalsIgnoreCase("TABLE")) {
                    tableComment = stmtComment.getComment();
                } else {
                    String simpleName = stmtComment.getOn().toString();
                    if (simpleName.contains(".")) {
                        simpleName = simpleName.substring(simpleName.lastIndexOf(".") + 1);
                    }
                    mapComment.put(simpleName, stmtComment.getComment());
                }
            }

        }
        if (tableName != null && !tableName.equals("")) {
            OdpsCreateTableStatement odpsCreate = new OdpsCreateTableStatement();
            odpsCreate.setIfNotExiists(true);
            odpsCreate.setDbType(JdbcConstants.ODPS);
            String table = String.format("%s_%s", db, tableName);
            odpsCreate.setName(table);
            tableSet.add(table);
            if (tableComment != null) {
                //System.out.println(tableComment);
                odpsCreate.setComment(tableComment);
            }
            for(String fieldName:fieldList){
                 SQLColumnDefinition columnDefinition=mapField.get(fieldName);
                SQLColumnDefinition colDefNew = new SQLColumnDefinition();
                colDefNew.setName(columnDefinition.getName());
                colDefNew.setDataType(toHiveType(columnDefinition.getDataType()));
                if (mapComment.containsKey(fieldName)) {
                    colDefNew.setComment(mapComment.get(fieldName));
                }
                odpsCreate.getTableElementList().add(colDefNew);
            }
            /*
            for (Map.Entry<String, SQLColumnDefinition> entryField : mapField.entrySet()) {
                SQLColumnDefinition colDefNew = new SQLColumnDefinition();
                colDefNew.setName(entryField.getValue().getName());
                colDefNew.setDataType(toHiveType(entryField.getValue().getDataType()));
                if (mapComment.containsKey(entryField.getKey())) {
                    colDefNew.setComment(mapComment.get(entryField.getKey()));
                }
                odpsCreate.getTableElementList().add(colDefNew);
            }*/
            String showTbSql = "select 'create table " + table + " ...' as table_name";
            String crtSql = odpsCreate.toString().toLowerCase();
            crtSql = crtSql.replaceAll(";", " ");
            return String.format("%s;\r\n%s;\r\n", showTbSql, crtSql);
        }
        return null;
    }

    private void outputHql(String db, String hql) {
        try {
            FileUtils.write(new File(baseDir + db + ".hql"), hql, "UTF-8", true);
            String alterHql=hql.replace("create table if not exists ","alter table ");
            alterHql=alterHql.replace("create table ","alter table ");
            alterHql=alterHql.replaceFirst(" \\("," replace columns (");
            System.out.println(alterHql);
            FileUtils.write(new File(baseDir + "alter_"+db + ".hql"), alterHql, "UTF-8", true);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

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

    private SQLDataType toHiveType(SQLDataType dataType) {
        SQLDataType hiveType = new SQLDataTypeImpl();
        hiveType.setName("string");
        if (dataType.getName().equalsIgnoreCase("NUMBER")) {
            hiveType.setName("decimal");
        } else if (dataType.getName().equalsIgnoreCase("CHAR")) {
            hiveType.setName("string");
        } else if (dataType.getName().equalsIgnoreCase("INTEGER")) {
            hiveType.setName("int");
        } else if (dataType.getName().equalsIgnoreCase("LONG")) {
            hiveType.setName("bigint");
        } else if (dataType.getName().equalsIgnoreCase("DATE")) {
            hiveType.setName("date");
        } else if (dataType.getName().equalsIgnoreCase("TIMESTAMP")) {
            hiveType.setName("timestamp");
        } else if (dataType.getName().equalsIgnoreCase("FLOAT")) {
            hiveType.setName("float");
        } else if (dataType.getName().equalsIgnoreCase("DOUBLE")) {
            hiveType.setName("double");
        }
        if (dataType.getArguments() != null &&
                (!dataType.getArguments().isEmpty()) &&
                (!hiveType.getName().equalsIgnoreCase("string"))) {
            hiveType.getArguments().addAll(dataType.getArguments());
        }
        return hiveType;
    }
}
