package x.bigdata.runner.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import redis.clients.jedis.ShardedJedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

public class HbaseBaseTest2 {
    // 相关文档：HBase基准测试设计.xmind、HBase集锦3.docx

    public static void main(String[] args) throws Exception {
        HbaseBaseTest2 test = new HbaseBaseTest2();

        test.insert01();

    }

    // create 'base_test_metric', 'c', SPLITS=>['10|','20|','30|','40|','50|','60|','70|','80|','90|']


    private void insert01() throws IOException {
        System.out.println("insert begin: " + new Date());

        Connection connection = this.getConnection();
        Random random = new Random();
        Table table = connection.getTable(TableName.valueOf("base_test_metric"));

        for (int i = 1; i <= 100; i++) {
            String no = String.format("%09d", i);
            String key = no.substring(7) + "|" + no + "|metric";
            Put put = new Put(key.getBytes());

            int metric = random.nextInt(100);
            this.put(put, "c", "metric", String.valueOf(metric));

            table.put(put);
        }

        System.out.println("insert begin: " + new Date());
    }


    private void put(Put put, String family, String column, String value) {
        put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
    }

    private Connection getConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        //configuration.set("hbase.zookeeper.quorum", "172.22.6.9,172.22.6.10,172.22.6.11");
        configuration.set("hbase.zookeeper.quorum", "tdh60dev01:2181,tdh60dev02:2181,tdh60dev03:2181");

        //tdh60dev01:2181,tdh60dev02:2181,tdh60dev03:2181
        return ConnectionFactory.createConnection(configuration);
    }



}
