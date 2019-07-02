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

        //test.batchInsert();

        test.insert01("base_test_metric", 1, 10000);
    }

    // create 'base_test_metric_1y', 'c', SPLITS=>['10|','20|','30|','40|','50|','60|','70|','80|','90|']


    private void insert01(String tableName, int begin, int end) throws IOException {
        System.out.println("insert begin: " + new Date());

        Connection connection = this.getPoolConnection();
        Random random = new Random();
        Table table = connection.getTable(TableName.valueOf(tableName));

        for (int i = begin; i <= end; i++) {
            String key = String.format("%02d", i) + "|" + String.format("%09d", i) + "|metric";
            Put put = new Put(key.getBytes());

            int metric = random.nextInt(100);
            this.put(put, "c", "metric", String.valueOf(metric));

            System.out.println("insert " + i + " begin: " + new Date());

            table.put(put);

            System.out.println("insert " + i + " end: " + new Date());
        }

        System.out.println("insert begin: " + new Date());
    }


    private void batchInsert01(String tableName, int begin, int end, int putNum) throws IOException {
        System.out.println("batchInsert begin: " + new Date());

        Connection connection = this.getPoolConnection();
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

                    table.put(list);
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

    private Connection getPoolConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        //集群配置
        //configuration.set("hbase.zookeeper.quorum", "101.236.39.141,101.236.46.114,101.236.46.113");
        configuration.set("hbase.master", "127.0.0.1:60000");
        return ConnectionFactory.createConnection(configuration);
    }

}
