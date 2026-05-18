-- 分布式锁解锁脚本
-- KEYS[1]: 锁的key
-- ARGV[1]: 当前线程的ID

-- 获取锁中存储的线程ID


-- 判断锁中存储的线程ID是否和当前线程ID一致
if redis.call('get', KEYS[1]) == ARGV[1] then
    redis.call('del', KEYS[1])
end
return 0
