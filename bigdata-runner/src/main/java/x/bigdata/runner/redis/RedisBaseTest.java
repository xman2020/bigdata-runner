package x.bigdata.runner.redis;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class RedisBaseTest {
    // 相关文档：Redis基准测试1.docx

    private ShardedJedisPool pool;

    // java -cp bigdata-runner-1.0-SNAPSHOT-jar-with-dependencies.jar x.bigdata.runner.redis.RedisBaseTest

    public static void main(String[] args) {
        RedisBaseTest test = new RedisBaseTest();

        //test.insert01();
        //test.insert03();

        //test.select01();
        //test.select03();
        test.select05();
    }

    public RedisBaseTest() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxActive(50);
        config.setMaxIdle(10);
        //config.setTestOnBorrow(true);

        JedisShardInfo node1 = new JedisShardInfo("172.22.6.1", 6379);
        //JedisShardInfo node1 = new JedisShardInfo("127.0.0.1", 6379);
        List shardList = new ArrayList();
        shardList.add(node1);

        this.pool = new ShardedJedisPool(config, shardList);
    }

    public void insert01() {
        System.out.println("insert begin: " + new Date());

        Random random = new Random();
        ShardedJedis jedis = null;

        try {
            jedis = this.pool.getResource();

            for (int i = 1; i <= 5000000; i++) {
                int metric = random.nextInt(100);
                String key = String.format("%02d", i) + "|" + String.format("%09d", i) + "|metric";
                jedis.set(key, String.valueOf(metric));
            }
        } finally {
            this.pool.returnResource(jedis);
        }

        System.out.println("insert end: " + new Date());
    }

    public void insert02(int begin, int end) {
        System.out.println(begin + " insert begin: " + new Date());

        Random random = new Random();
        ShardedJedis jedis = null;

        try {
            jedis = this.pool.getResource();

            for (int i = begin; i <= end; i++) {
                int metric = random.nextInt(100);
                String key = String.format("%02d", i) + "|" + String.format("%09d", i) + "|metric";
                jedis.set(key, String.valueOf(metric));
            }
        } finally {
            this.pool.returnResource(jedis);
        }

        System.out.println(begin + " insert end: " + new Date());
    }

    public void insert03() {
//        new InsertThread(1, 1000000).start();
//        new InsertThread(1000001, 2000000).start();
//        new InsertThread(2000001, 3000000).start();
//        new InsertThread(3000001, 4000000).start();
//        new InsertThread(4000001, 5000000).start();

        new InsertThread(1, 500000).start();
        new InsertThread(500001, 1000000).start();
        new InsertThread(1000001, 1500000).start();
        new InsertThread(1500001, 2000000).start();
        new InsertThread(2000001, 2500000).start();
        new InsertThread(2500001, 3000000).start();
        new InsertThread(3000001, 3500000).start();
        new InsertThread(3500001, 4000000).start();
        new InsertThread(4000001, 4500000).start();
        new InsertThread(4500001, 5000000).start();
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
            RedisBaseTest test = new RedisBaseTest();
            test.insert02(begin, end);
        }
    }

    public void select01() {
        System.out.println("select begin: " + new Date());

        Random random = new Random();
        ShardedJedis jedis = null;

        try {
            jedis = this.pool.getResource();

            for (int i = 1; i <= 5000000; i++) {
                //int metric = random.nextInt(100);
                String key = String.format("%02d", i) + "|" + String.format("%09d", i) + "|metric";
                jedis.get(key);
                //System.out.println(jedis.get(key));
            }
        } finally {
            this.pool.returnResource(jedis);
        }

        System.out.println("select end: " + new Date());
    }

    public void select02(int begin, int end) {
        System.out.println(begin + " select begin: " + new Date());

        Random random = new Random();
        ShardedJedis jedis = null;

        try {
            jedis = this.pool.getResource();

            for (int i = begin; i <= end; i++) {
                //int metric = random.nextInt(100);
                String key = String.format("%02d", i) + "|" + String.format("%09d", i) + "|metric";
                jedis.get(key);
                //System.out.println(jedis.get(key));
            }
        } finally {
            this.pool.returnResource(jedis);
        }

        System.out.println(begin + " select end: " + new Date());
    }

    public void select03() {
//        new SelectThread(1, 1000000).start();
//        new SelectThread(1000001, 2000000).start();
//        new SelectThread(2000001, 3000000).start();
//        new SelectThread(3000001, 4000000).start();
//        new SelectThread(4000001, 5000000).start();

        for (int i = 0; i < 3; i++) {
            new SelectThread(1, 500000).start();
            new SelectThread(500001, 1000000).start();
            new SelectThread(1000001, 1500000).start();
            new SelectThread(1500001, 2000000).start();
            new SelectThread(2000001, 2500000).start();
            new SelectThread(2500001, 3000000).start();
            new SelectThread(3000001, 3500000).start();
            new SelectThread(3500001, 4000000).start();
            new SelectThread(4000001, 4500000).start();
            new SelectThread(4500001, 5000000).start();
        }
    }

    static class SelectThread extends Thread {
        private int begin;
        private int end;

        public SelectThread(int begin, int end) {
            this.begin = begin;
            this.end = end;
        }

        @Override
        public void run() {
            RedisBaseTest test = new RedisBaseTest();
            test.select02(begin, end);
        }
    }

    public void select04() {
        String threadName = Thread.currentThread().getName();
        System.out.println(threadName +" select begin: " + new Date());

        Random random = new Random();
        ShardedJedis jedis = null;
        int count = 0;
        int batch = 0;

        try {
            jedis = this.pool.getResource();

            while (true) {
                int i = random.nextInt(5000000);
                String key = String.format("%02d", i) + "|" + String.format("%09d", i) + "|metric";
                jedis.get(key);

                count++;

                if (count / 1000000 == 1) {
                    batch++;
                    System.out.println(threadName +" select batch " + batch + ": " + new Date());
                    count = 0;
                }
            }
        } finally {
            this.pool.returnResource(jedis);
        }
    }

    public void select05() {
        for (int i = 0; i < 10; i++) {
            new SelectThread2().start();
        }
    }

    static class SelectThread2 extends Thread {
        @Override
        public void run() {
            RedisBaseTest test = new RedisBaseTest();
            test.select04();
        }
    }


}
