--1.参数列表
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId=ARGV[3]

--2.数据key
local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId

-- 3.【修复nil报错】先获取库存，不存在则赋值为0
local stock = tonumber(redis.call('get', stockKey)) or 0

-- 4.判断库存是否充足
if (stock <= 0) then
    return 1
end

--5.判断用户是否已经下单
if (redis.call('sismember', orderKey, userId) == 1) then
    return 2
end

--6.扣库存
redis.call('incrby', stockKey, -1)

--7.记录用户
redis.call('sadd', orderKey, userId)
--发送消息到队列中
redis.call('xadd','stream.orders','*','userId',userId,'voucherId',voucherId,'id',orderId)
return 0