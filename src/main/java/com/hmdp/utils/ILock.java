package com.hmdp.utils;

public interface ILock {
    /**
     * 尝试获取锁
     * @param timeoutSec 锁的过期时间，单位秒
     * @return true代表获取锁成功
     */
    boolean tryLock(long timeoutSec);

    void unLock();
}
