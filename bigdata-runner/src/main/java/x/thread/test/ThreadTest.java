package x.thread.test;

public class ThreadTest {
    // 相关文档：Java多核多线程测试.docx

    public static void main(String[] args) throws Exception {
        int threads = Integer.parseInt(args[0]);
        System.out.print(threads + " threads ");

        for (int i = 0; i < threads; i++) {
            new ThreadTest.SimpleThread().start();
        }
    }

    static class SimpleThread extends Thread {
        @Override
        public void run() {
            while (true) {
                int i = 1 + 1;

                try {
                    sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
