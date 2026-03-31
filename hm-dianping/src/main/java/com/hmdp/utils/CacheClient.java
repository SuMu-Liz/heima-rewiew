package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R,ID> R queryWithPassThrough(String keyProfix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit){
        String key = keyProfix+id;
        String json = stringRedisTemplate.opsForValue().get(key);

        //判断是否存在
        if(StrUtil.isNotBlank(json)){
            //存在则返回
            return JSONUtil.toBean(json, type);
        }
        //判断命中是否是空值
        if(json!=null){
            //命中空值，返回错误信息
            return null;
        }

        //如果缓存中没有则从数据库中查询
        R r = dbFallback.apply(id);
        if(r==null){
            //空值写入redis避免穿透缓存
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回错误信息
            return null;
        }

        //存在 写入redis
        this.set(key,r,time,unit);
        return r;
    }


    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    public <R,ID> R queryWithLogicalExpire(String keyPrefix,ID id, Class<R> type,Function<ID,R> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix+id;
        String json = stringRedisTemplate.opsForValue().get(key);

        //判断是否存在
        if(StrUtil.isBlank(json)){
            return null;
        }

        //命中了，先把json反序列化对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r= JSONUtil.toBean((JSONObject) redisData.getData(),type);
        LocalDateTime expireTime = redisData.getExpireTime();

        //判断缓存是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //未过期，直接返回店铺信息
            return r;
        }
        //已过期，实现缓存重建
        String lockKey = LOCK_SHOP_KEY+id;
        //获取互斥锁
        boolean isLock = tryLock(lockKey);

        //互斥锁获取成功，开启独立线程实现缓存重建
        if(isLock){
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //查数据库
                    R r1     = dbFallback.apply(id);
                    //写入Redis
                    this.setWithLogicalExpire(key,r1,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unlock(lockKey);
                }


            });
        }
        //失败 返沪过期的商铺信息


        return r;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key,"",10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

}
