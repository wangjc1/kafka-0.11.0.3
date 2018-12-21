/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

/**
 * A helper class for managing the heartbeat to the coordinator
 */
public final class Heartbeat {
    private final long sessionTimeout;
    //每次发送心跳间隔时间，由heartbeat.interval.ms参数配置
    private final long heartbeatInterval;
    //由max.poll.interval.ms参数配置，最大拉取时长
    private final long maxPollInterval;
    //重试时间，由retry.backoff.ms参数设置
    private final long retryBackoffMs;

    //上一次发送心跳的时间点
    private volatile long lastHeartbeatSend; // volatile since it is read by metrics
    //上一次心跳成功响应时间点
    private long lastHeartbeatReceive;
    //查找主coordinator后响应GroupCoordinatorResponseHandler回调中调用resetTimeouts()方法更新的一个时间点
    //也就是Consumer客户端和GroupCoordinator成功建立连接后的一个时间点
    private long lastSessionReset;
    //上一次调用pollHeartbeat(now)方法的时间点
    private long lastPoll;
    private boolean heartbeatFailed;

    public Heartbeat(long sessionTimeout,
                     long heartbeatInterval,
                     long maxPollInterval,
                     long retryBackoffMs) {
        if (heartbeatInterval >= sessionTimeout)
            throw new IllegalArgumentException("Heartbeat must be set lower than the session timeout");

        this.sessionTimeout = sessionTimeout;
        this.heartbeatInterval = heartbeatInterval;
        this.maxPollInterval = maxPollInterval;
        this.retryBackoffMs = retryBackoffMs;
    }

    public void poll(long now) {
        this.lastPoll = now;
    }

    public void sentHeartbeat(long now) {
        this.lastHeartbeatSend = now;
        this.heartbeatFailed = false;
    }

    public void failHeartbeat() {
        this.heartbeatFailed = true;
    }

    public void receiveHeartbeat(long now) {
        this.lastHeartbeatReceive = now;
    }

    public boolean shouldHeartbeat(long now) {
        return timeToNextHeartbeat(now) == 0;
    }
    
    public long lastHeartbeatSend() {
        return this.lastHeartbeatSend;
    }

    public long timeToNextHeartbeat(long now) {
        //截止目前上一次心跳发送过去多长时间了，如果一次心跳还没发送过，则以发现coordinator的时间点为上一次发送心跳的时间
        long timeSinceLastHeartbeat = now - Math.max(lastHeartbeatSend, lastSessionReset);
        final long delayToNextHeartbeat;
        if (heartbeatFailed)
            delayToNextHeartbeat = retryBackoffMs;
        else
            delayToNextHeartbeat = heartbeatInterval;

        if (timeSinceLastHeartbeat > delayToNextHeartbeat)
            return 0;
        else
            return delayToNextHeartbeat - timeSinceLastHeartbeat;
    }

    public boolean sessionTimeoutExpired(long now) {
        return now - Math.max(lastSessionReset, lastHeartbeatReceive) > sessionTimeout;
    }

    public long interval() {
        return heartbeatInterval;
    }

    public void resetTimeouts(long now) {
        this.lastSessionReset = now;
        this.lastPoll = now;
        this.heartbeatFailed = false;
    }

    public boolean pollTimeoutExpired(long now) {
        return now - lastPoll > maxPollInterval;
    }

}