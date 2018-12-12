package org.apache.kafka.test;

import org.junit.Test;

import java.util.ConcurrentModificationException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * KafkaConsumer中用到的一个判断是否在当前线程的小工具
 * @author: wangjc
 * 2018/11/28
 */
public class MustCurrentThreadTest {
    //初始线程ID
    private static final long NO_CURRENT_THREAD = -1L;
    //当前线程ID
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    //当前线程中调用了refcount次acquire()，然后再调用release() 释放refcount次后重新让currentThread回到初始值
    private final AtomicInteger refcount = new AtomicInteger(0);


    @Test
    public void testAtomicLong(){
        Thread newTrhead = Executors.defaultThreadFactory().newThread(new Runnable() {
            @Override
            public void run() {}
        });

        assertTrue(currentThread.compareAndSet(NO_CURRENT_THREAD,Thread.currentThread().getId()));
        assertFalse(currentThread.compareAndSet(NO_CURRENT_THREAD,Thread.currentThread().getId()));
        assertTrue(currentThread.compareAndSet(Thread.currentThread().getId(),newTrhead.getId()));
        assertTrue(Long.valueOf(newTrhead.getId()) == currentThread.get());
    }

    /**
     * 启动一个新的线程，然后进入到doWork()方法中，这时当前线程正在访问，所以会抛出ConcurrentModificationException异常
     */
    @Test(expected = ConcurrentModificationException.class)
    public void testInCurentThread() {
        Executors.defaultThreadFactory().newThread(new Runnable() {
            @Override
            public void run() {
                doWork();
            }
        }).start();

        sleep(500);

        doWork();

    }

    private void doWork(){
        try {
            acquire();
            sleep(2000);
        } finally {
            release();
        }
    }

    private void acquire() {
        long threadId = Thread.currentThread().getId();
        System.out.println(threadId);
        if (threadId != currentThread.get() && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId))
            throw new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access");
        refcount.incrementAndGet();
    }

    private void release() {
        if (refcount.decrementAndGet() == 0)
            currentThread.set(NO_CURRENT_THREAD);
    }

    private void sleep(long ms){
        try {Thread.sleep(ms); } catch (InterruptedException e) {}
    }
}
