package com.jnks.iot.common.util;

import org.springframework.util.StopWatch;

/**
 * Utility method that extends Spring Framework StopWatch
 * It is a MONOTONIC time stopwatch.
 * It is a replacement for any measurements with a wall-clock like System.currentTimeMillis()
 * It is not affected by leap second, day-light saving and wall-clock adjustments by manual or network time synchronization
 * The main features is a single call for common use cases:
 *  - create and start: JnksIotStopWatch sw = JnksIotStopWatch.startNew()
 *  - stop and get: sw.stopAndGetTotalTimeMillis() or sw.stopAndGetLastTaskTimeMillis()
 * */
public class JnksIotStopWatch extends StopWatch {

    public static JnksIotStopWatch create(){
        JnksIotStopWatch stopWatch = new JnksIotStopWatch();
        stopWatch.start();
        return stopWatch;
    }

    public static JnksIotStopWatch create(String taskName){
        JnksIotStopWatch stopWatch = new JnksIotStopWatch();
        stopWatch.start(taskName);
        return stopWatch;
    }

    public void startNew(String taskName){
        stop();
        start(taskName);
    }

    public long stopAndGetTotalTimeMillis(){
        stop();
        return getTotalTimeMillis();
    }

    public long stopAndGetTotalTimeNanos(){
        stop();
        return getLastTaskTimeNanos();
    }

    public long stopAndGetLastTaskTimeMillis(){
        stop();
        return getLastTaskTimeMillis();
    }

    public long stopAndGetLastTaskTimeNanos(){
        stop();
        return getLastTaskTimeNanos();
    }

}
