-- -- 秒杀库存，一人一单
-- local voucherId = ARGV[1]
--
-- local userId = ARGV[2]
--
-- local orderId = ARGV[3]
--
-- -- 订单key
-- local orderKey = 'seckill:order:' .. voucherId
-- -- 库存key
-- local stockKey = 'seckill:stock:' .. voucherId
-- -- 订单key
-- local orderKey = 'seckill:order:' .. voucherId
--
-- -- 判断库存是否充足
-- if (tonumber(redis.call('get', stockKey)) <= 0) then
--     -- 库存不足
--     return 1
-- end
-- -- 判断用户是否重复下单
-- if (redis.call('sismember', orderKey, userId) == 1) then
--     -- 重复下单
--     return 2
-- end
-- -- 扣减库存
-- redis.call('incrby', stockKey, -1)
-- -- 添加到下单集合
-- redis.call('sadd', orderKey, userId)
-- -- 消息队列下单
-- redis.call('xadd','stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id',orderId)
--
-- 秒杀库存，一人一单
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]

-- 订单key
local orderKey = 'seckill:order:' .. voucherId
-- 库存key
local stockKey = 'seckill:stock:' .. voucherId

-- 判断库存是否充足
local stock = redis.call('get', stockKey)
if (stock == false or tonumber(stock) <= 0) then
    -- 库存不足
    return 1
end
-- 判断用户是否重复下单
if (redis.call('sismember', orderKey, userId) == 1) then
    -- 重复下单
    return 2
end
-- 扣减库存
redis.call('incrby', stockKey, -1)
-- 添加到下单集合
redis.call('sadd', orderKey, userId)
-- 消息队列下单
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)
return 0