package com.clickhouse.parser;

import com.clickhouse.data.FieldLineageInfo;
import com.clickhouse.parser.ast.DistributedTableInfoDetector;
import com.clickhouse.parser.ast.INode;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.List;

@Slf4j
public class TestAstParser {

    @Test
    public void testAstParser() {
        // parse SQL and generate its AST
        AstParser astParser = new AstParser();
        String sql1 = "ALTER TABLE my_db.my_tbl ADD COLUMN IF NOT EXISTS id Int64";
        Object parsedResult1 = astParser.parse(sql1);
        log.info(parsedResult1.toString());
        String sql2 = "ALTER TABLE my_db.my_tbl DROP PARTITION '2020-11-21'";
        Object parsedResult2 = astParser.parse(sql2);
        log.info(parsedResult2.toString());
    }

    @Test
    public void testReferredTablesDetector() {
//        String sql = "SELECT t1.a FROM t1 RIGHT JOIN t2 ON t1.id = t2.id LIMIT 1000";
//        String sql = "INSERT INTO insert_select_testtable (a, b, c) select d ,e, f from others;";

        String sql = "INSERT INTO TABLE db_test.table_result (id, name)\n"
                + "SELECT\n"
                + "    t1.id,\n"
                + "    t2.name\n"
                + "FROM\n"
                + "(\n"
                + "    SELECT\n"
                + "        id1 + id2 AS id\n"
                + "    FROM\n"
                + "        db_test.table1\n"
                + ") t1\n"
                + "LEFT JOIN\n"
                + "(\n"
                + "    SELECT\n"
                + "        id,\n"
                + "        name\n"
                + "    FROM\n"
                + "    (\n"
                + "        SELECT\n"
                + "            id,\n"
                + "            sourcename AS name\n"
                + "        FROM\n"
                + "            db_test.table2\n"
                + "    )\n"
                + ") t2\n"
                + "ON t1.id=t2.id";

//        String sql = "INSERT INTO TABLE db_test.table_result (id, name)\n"
//                + "SELECT\n"
//                + "    t1.id,\n"
//                + "    t2.name\n"
//                + "FROM\n"
//                + "(\n"
//                + "    SELECT\n"
//                + "        id1 + id2 AS id\n"
//                + "    FROM\n"
//                + "        db_test.table1\n"
//                + ") t1\n"
//                + "LEFT JOIN db_test.table2 as t2\n"
//                + "ON t1.id=t2.id;";

//        String sql = "INSERT INTO TABLE db_test.table_result\n"
//                + "SELECT\n"
//                + "    t1.id,\n"
//                + "    t1.name\n"
//                + "FROM\n"
//                + " task as t1;";

        AstParser astParser = new AstParser();
        Object ast = astParser.parse(sql);
        DataLineageDetector dataLineageDetector = new DataLineageDetector();
//        ReferredTablesDetector referredTablesDetector = new ReferredTablesDetector();
//        List<String> tables = referredTablesDetector.searchTables((INode) ast);
        dataLineageDetector.visit((INode)ast);
        List<FieldLineageInfo> fieldLineageInfoList = dataLineageDetector.getFieldLineage();
        System.out.println(fieldLineageInfoList.size());
//        tables.parallelStream().forEach(table -> System.out.println(table));
    }

    @Test
    public void testDistributedTableInfoDetector() {
        String sql = "CREATE TABLE my_db.my_tbl (date Date, name String) Engine = Distributed('my_cluster', 'my_db', 'my_tbl_local', rand())";
        DistributedTableInfoDetector distributedTableInfoDetector = new DistributedTableInfoDetector();
        String clusterName = distributedTableInfoDetector.searchCluster(sql);
        log.info(clusterName);
        long start = System.currentTimeMillis();
        String tableFullName = distributedTableInfoDetector.searchLocalTableFullName(sql);
        long end = System.currentTimeMillis();
        log.info(tableFullName);
        log.info("It takes " + (end - start) + " ms");
    }

    @Test
    public void testDistributedTableInfoDetector2() {
        String sql = "CREATE TABLE mydb.mytb (uuid UUID DEFAULT generateUUIDv4(), cktime DateTime DEFAULT now() COMMENT 'c', openid String, username String, appid String, from_channel String, source_channel String, source String, regtime DateTime, brandid String, devicecode String, actiontime DateTime, ismingamelogin String, version String, platform String, project String, plat String, source_openid String COMMENT 'a', event Int16 COMMENT 'b') ENGINE = ReplicatedMergeTree('/clickhouse/mydb/mytb/{shard}', '{replica}') PARTITION BY toYYYYMM(cktime) ORDER BY (regtime, appid, openid) SETTINGS index_granularity = 8192";
        DistributedTableInfoDetector distributedTableInfoDetector = new DistributedTableInfoDetector();
        String clusterName = distributedTableInfoDetector.searchCluster(sql);
        log.info(clusterName);
        long start = System.currentTimeMillis();
        String tableFullName = distributedTableInfoDetector.searchLocalTableFullName(sql);
        long end = System.currentTimeMillis();
        log.info(tableFullName);
        log.info("It takes " + (end - start) + " ms");
    }

    @Test
    public void testDistributedTableInfoDetector3() {
        String sql = "CREATE TABLE my_db.my_tbl on cluster my_cluster Engine = Distributed('my_cluster', 'my_db', 'my_tbl_local', rand()) as my_db.my_tbl_local";
        DistributedTableInfoDetector distributedTableInfoDetector = new DistributedTableInfoDetector();
        String clusterName = distributedTableInfoDetector.searchCluster(sql);
        log.info(clusterName);
        long start = System.currentTimeMillis();
        String tableFullName = distributedTableInfoDetector.searchLocalTableFullName(sql);
        long end = System.currentTimeMillis();
        log.info(tableFullName);
        log.info("It takes " + (end - start) + " ms");
    }


}
