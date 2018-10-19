package org.apache.kafka.test;

/**
 * @author: wangjc
 * 2018/10/18
 */
public class ReteenLockTest {

    public static void main(String[] args) {
        MyThread t = new MyThread();
        t.start();
    }
}

class Father{
    public int i = 10;

    public synchronized void operateFather(){
        i--;
        System.out.println("Father i " + i);
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class Son extends  Father{
    public synchronized void operateSon(){
        try {
            while(true){
                i--;
                System.out.println("Son i " + i);
                Thread.sleep(100);
                operateFather();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class MyThread extends Thread{
    @Override
    public void run() {
        Son son = new Son();
        son.operateSon();
    }
}
