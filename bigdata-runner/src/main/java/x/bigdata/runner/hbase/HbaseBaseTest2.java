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

public class HbaseBaseTest2 {
    // 相关文档：Hbase基准测试.docx

    private String table = "base_test_metric_16";

    private static Connection CONN;
    private static String connections = "1";
    private static String threads = "10";

    private static int oneThreadSelect = 200000000;
    private static int selectThreads = 100;

    // java -cp bigdata-runner-1.0-SNAPSHOT-jar-with-dependencies.jar x.bigdata.runner.hbase.HbaseBaseTest2

    public static void main(String[] args) throws Exception {
        HbaseBaseTest2 test = new HbaseBaseTest2();

        //test.insert01();
        //test.insert03();
        //test.insert05();

        //test.select01(10);
        test.select02();
        //test.select04();
    }

    static {
        try {
            CONN = getConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // create 'base_test_metric_16', 'c', SPLITS=>['10|','20|','30|','40|','50|','60|','70|','80|','90|']

    private void insert01() throws IOException {
        System.out.println("insert begin: " + new Date());

        Connection connection = this.getConnection();
        Random random = new Random();
        Table table = connection.getTable(TableName.valueOf(this.table));

        for (int i = 1; i <= 100000; i++) {
            String no = String.format("%09d", i);
            String key = no.substring(7) + "|" + no + "|metric";
            Put put = new Put(key.getBytes());

            int metric = random.nextInt(100);
            this.put(put, "c", "metric", String.valueOf(metric));

            table.put(put);
        }

        System.out.println("insert end: " + new Date());
    }

    private void insert02(int begin, int end) throws IOException {
        System.out.println("insert begin: " + new Date());

        Connection connection = CONN;
        Random random = new Random();
        Table table = connection.getTable(TableName.valueOf(this.table));

        for (int i = begin; i <= end; i++) {
            String no = String.format("%09d", i);
            String key = no.substring(7) + "|" + no + "|metric";
            Put put = new Put(key.getBytes());

            int metric = random.nextInt(100);
            this.put(put, "c", "metric", String.valueOf(metric));

            table.put(put);
        }

        System.out.println("insert end: " + new Date());
    }

    public void insert03() {
        new InsertThread(1, 100000).start();
        new InsertThread(100001, 200000).start();
        new InsertThread(200001, 300000).start();
        new InsertThread(300001, 400000).start();
        new InsertThread(400001, 500000).start();
        new InsertThread(500001, 600000).start();
        new InsertThread(600001, 700000).start();
        new InsertThread(700001, 800000).start();
        new InsertThread(800001, 900000).start();
        new InsertThread(900001, 1000000).start();
    }

    static class InsertThread extends Thread {
        private int begin;
        private int end;

        public InsertThread(int begin, int end) {
            this.begin = begin;
            this.end = end;
        }

        @Override
        public void run() {
            HbaseBaseTest2 test = new HbaseBaseTest2();
            try {
                test.insert02(begin, end);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void insert04(int batch, int count) throws IOException {
        System.out.println("insert begin: " + new Date());

        Connection connection = CONN;
        Random random = new Random();
        Table table = connection.getTable(TableName.valueOf(this.table));
        String batchStr = String.format("%03d", batch);

        for (int i = 1; i <= count; i++) {
            String no = String.format("%06d", i);
            String key = no.substring(4) + "|" + batchStr + no + "|metric";
            Put put = new Put(key.getBytes());

            int metric = random.nextInt(100);
            this.put(put, "c", "metric", String.valueOf(metric));

            table.put(put);
        }

        System.out.println("insert end: " + new Date());
    }

    public void insert05() {
        for (int i = 1; i <= 400; i++) {
            new InsertThread2(i, 20000).start();
        }
    }

    static class InsertThread2 extends Thread {
        private int batch;
        private int count;

        public InsertThread2(int batch, int count) {
            this.batch = batch;
            this.count = count;
        }

        @Override
        public void run() {
            HbaseBaseTest2 test = new HbaseBaseTest2();
            try {
                test.insert04(batch, count);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void put(Put put, String family, String column, String value) {
        put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
    }

    private void select01(int count) throws IOException {
        System.out.println("select begin: " + new Date());

        Connection connection = CONN;
        Table table = connection.getTable(TableName.valueOf(this.table));

        for (int i = 1; i <= count; i++) {
            String no = String.format("%09d", i);
            String key = no.substring(7) + "|" + no + "|metric";
            Get get = new Get(key.getBytes());

            Result result = table.get(get);

//            for (Cell cell : result.rawCells()) {
//                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
//                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
//
//                System.out.println(colName + "=" + value);
//            }
        }

        System.out.println("select end: " + new Date());
    }

    public void select02() {
        for (int i = 1; i <= selectThreads; i++) {
            new SelectThread(oneThreadSelect).start();
        }
    }

    static class SelectThread extends Thread {
        private int count;

        public SelectThread(int count) {
            this.count = count;
        }

        @Override
        public void run() {
            HbaseBaseTest2 test = new HbaseBaseTest2();
            try {
                test.select01(count);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void select03(int count, int batchNum) throws IOException {
        System.out.println("select begin: " + new Date());

        Connection connection = CONN;
        Table table = connection.getTable(TableName.valueOf(this.table));
        ArrayList list = new ArrayList();

        for (int i = 1; i <= count; i++) {
            String no = String.format("%09d", i);
            String key = no.substring(7) + "|" + no + "|metric";
            Get get = new Get(key.getBytes());
            list.add(get);

            if (i % batchNum == 0) {
                Result[] result = table.get(list);
                list.clear();
            }
        }

        System.out.println("select end: " + new Date());
    }

    public void select04() {
        for (int i = 1; i <= selectThreads; i++) {
            new SelectThread2(oneThreadSelect, 50).start();
        }
    }

    static class SelectThread2 extends Thread {
        private int count;
        private int batchNum;

        public SelectThread2(int count, int batchNum) {
            this.count = count;
            this.batchNum = batchNum;
        }

        @Override
        public void run() {
            HbaseBaseTest2 test = new HbaseBaseTest2();
            try {
                test.select03(count, batchNum);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static Connection getConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "172.22.6.9,172.22.6.10,172.22.6.11");
        configuration.set("zookeeper.znode.parent", "/hyperbase1");
        configuration.set("hbase.hconnection.threads.core", threads);
        configuration.set("hbase.hconnection.threads.max", threads);
        configuration.set("hbase.client.ipc.pool.size", connections);

        // 问题：put时，程序卡住
        // 需要配置configuration.set("zookeeper.znode.parent", "/hyperbase1");

        return ConnectionFactory.createConnection(configuration);
    }


}
