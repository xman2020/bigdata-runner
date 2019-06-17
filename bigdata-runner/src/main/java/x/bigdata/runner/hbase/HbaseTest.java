package x.bigdata.runner.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Date;

public class HbaseTest {

    public static void main(String[] args) throws Exception {
        HbaseTest test = new HbaseTest();

        //test.insert();

        //test.insert100w();

        test.select();


    }

    private void insert() throws IOException {
        Connection connection = this.getConnection();
        Put put = new Put("0520-test".getBytes());
        this.put(put, "c", "id", "4cdbc040657a4847b2667e31d9e2c3e9");
        this.put(put, "c", "data_time", "2019-05-20 09:09:10");
        this.put(put, "c", "equip_no", "trade_003");
        this.put(put, "c", "bus_type", "105");
        this.put(put, "c", "monit_item", "cpu_zy");
        this.put(put, "c", "value", "0.81");
        this.put(put, "c", "data_type", "number");
        this.put(put, "c", "collect_time", "2019-05-20 09:09:10");
        this.put(put, "c", "create_time", "2019-05-20 09:09:10");

        try {
            Table table = connection.getTable(TableName.valueOf("monitor_constant_bus"));
            table.put(put);
        } finally {
            connection.close();
        }
    }

    private void insert100w() throws IOException {
        System.out.println("insert100w begin: " + new Date());

        Connection connection = this.getConnection();

        try {
            Table table = connection.getTable(TableName.valueOf("monitor_constant_bus_z"));

            for (int i = 0; i < 1000000; i++) {
                Put put = new Put(("0520-test100w-" + i).getBytes());
                this.put(put, "c", "id", "4cdbc040657a4847b2667e31d9e2c3e9");
                this.put(put, "c", "data_time", "2019-05-20 09:09:10");
                this.put(put, "c", "equip_no", "trade_003");
                this.put(put, "c", "bus_type", "105");
                this.put(put, "c", "monit_item", "cpu_zy");
                this.put(put, "c", "value", "0.81");
                this.put(put, "c", "data_type", "number");
                this.put(put, "c", "collect_time", "2019-05-20 09:09:10");
                this.put(put, "c", "create_time", "2019-05-20 09:09:10");

                table.put(put);
            }

        } finally {
            connection.close();
        }

        System.out.println("insert100w end: " + new Date());

        //insert100w begin: Mon May 20 16:32:54 CST 2019
        //insert100w end: Mon May 20 16:44:09 CST 2019
        //100万条，耗时11分15秒，每秒插入1481条

        //启用gzip压缩，create 'monitor_constant_bus_z', {NAME => 'c', COMPRESSION => 'gz'}
        //insert100w begin: Mon May 20 22:59:07 CST 2019
        //insert100w end: Mon May 20 23:11:55 CST 2019
        //100万条，耗时12分48秒，每秒插入1302条

    }

    private void put(Put put, String family, String column, String value) {
        put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
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

    private Connection getConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        //configuration.set("hbase.zookeeper.quorum", "172.22.6.15");
        //集群配置
        //configuration.set("hbase.zookeeper.quorum", "101.236.39.141,101.236.46.114,101.236.46.113");
        configuration.set("hbase.master", "127.0.0.1:60000");
        //configuration.set("hbase.master", "172.22.6.15:60000");
        return ConnectionFactory.createConnection(configuration);
    }

}
