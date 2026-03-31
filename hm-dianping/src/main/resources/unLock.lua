
--- DateTime: 2026/3/28 19:57
----- 锁的key
local key=KEYS[1]
--当前线程标识
local threadId=ARGV[1]

-- 获取锁中的线程标识
local id=redis.call('get',key)
--比较线程标识与锁中的是否一致
if(id==threadId) then
 return redis.call('del',key)
end
return 0