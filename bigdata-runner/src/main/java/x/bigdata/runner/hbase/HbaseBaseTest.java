package x.bigdata.runner.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

public class HbaseBaseTest {

    public static void main(String[] args) throws Exception {
        HbaseBaseTest test = new HbaseBaseTest();

        test.batchInsert();
    }

    private void batchInsert() throws IOException {
        System.out.println("batchInsert begin: " + new Date());

        Connection connection = this.getConnection();
        Random random = new Random();
        ArrayList list = new ArrayList();

        try {
            Table table = connection.getTable(TableName.valueOf("base_test_metric_1y"));

            for (int i = 0; i < 100000000; i++) {
                int metric = random.nextInt(100);
                String key = String.format("%02d", metric) + "|" + String.format("%09d", i) + "|metric";

                Put put = new Put(key.getBytes());
                this.put(put, "c", "metric", String.valueOf(metric));
                list.add(put);

                if (i % 1000000 == 0) {
                    System.out.println("insert 100w begin: " + new Date());

                    //table.put(list);
                    list.clear();

                    System.out.println("insert 100w end: " + new Date());
                }
            }
        } finally {
            connection.close();
        }

        System.out.println("batchInsert end: " + new Date());

        //耗时：25分04秒
        //TPS：66666条/s
        //造100w耗时：4秒
        //插100w耗时：11秒
        //容量：4.8G
    }

    private void batchInsert(String tableName, int begin, int end, int putNum) throws IOException {
        System.out.println("batchInsert begin: " + new Date());

        Connection connection = this.getConnection();
        Random random = new Random();
        ArrayList list = new ArrayList();

        try {
            Table table = connection.getTable(TableName.valueOf(tableName));

            for (int i = begin; i <= end; i++) {
                int metric = random.nextInt(100);
                String key = String.format("%02d", i) + "|" + String.format("%09d", i) + "|metric";

                Put put = new Put(key.getBytes());
                this.put(put, "c", "metric", String.valueOf(metric));
                list.add(put);

                if (i % putNum == 0) {
                    System.out.println("insert 100w begin: " + new Date());

                    table.put(list);
                    list.clear();

                    System.out.println("insert 100w end: " + new Date());
                }
            }
        } finally {
            connection.close();
        }

        System.out.println("batchInsert end: " + new Date());
    }


    private void select() throws IOException {
        System.out.println("select begin: " + new Date());

        Connection connection = this.getConnection();

        try {
            Table table = connection.getTable(TableName.valueOf("monitor_constant_bus_z"));
            Get get = new Get("0520-test100w-999999".getBytes());
            Result result = table.get(get);

            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

                System.out.println(colName + "=" + value);
            }
        } finally {
            connection.close();
        }

        System.out.println("select end: " + new Date());

        //将HBase重启后，测试查询
        //不压缩，3s
        //压缩，3s
        //压缩看来不影响查询
    }

    private void put(Put put, String family, String column, String value) {
        put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
    }

    private Connection getConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        //集群配置
        //configuration.set("hbase.zookeeper.quorum", "101.236.39.141,101.236.46.114,101.236.46.113");
        configuration.set("hbase.master", "127.0.0.1:60000");
        return ConnectionFactory.createConnection(configuration);
    }
}
