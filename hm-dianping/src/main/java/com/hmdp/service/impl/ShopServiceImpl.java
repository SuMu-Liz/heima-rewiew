package com.hmdp.service.impl;

import cn.hutool.cache.Cache;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
//        缓存穿透
//        Shop shop = cacheClient
//                .queryWithPassThrough(CACHE_SHOP_KEY,id,Shop.class,id2->getById(id2),CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);
        //逻辑过期解决缓存击穿,CACHE_SHOP_TTL是逻辑过期时间
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY,id,Shop.class,
                id2->getById(id2),
                CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, id2 -> getById(id2), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if(shop==null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
//    public Shop queryWithLogicalExpire(Long id){
//        String key = CACHE_SHOP_KEY+id;
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//
//        //判断是否存在
//        if(StrUtil.isBlank(shopJson)){
//            return null;
//        }
//
//        //命中了，先把json反序列化对象
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        Shop shop= JSONUtil.toBean((JSONObject) redisData.getData(),Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//
//        //判断缓存是否过期
//        if(expireTime.isAfter(LocalDateTime.now())){
//            //未过期，直接返回店铺信息
//            return shop;
//        }
//        //已过期，实现缓存重建
//        String lockKey = LOCK_SHOP_KEY+id;
//        //获取互斥锁
//        boolean isLock = tryLock(lockKey);
//
//        //互斥锁获取成功，开启独立线程实现缓存重建
//        if(isLock){
//            CACHE_REBUILD_EXECUTOR.submit(() -> {
//                try {
//                    this.saveShop2Redis(id,20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }finally {
//                    //释放锁
//                    unlock(lockKey);
//                }
//
//
//            });
//        }
//        //失败 返沪过期的商铺信息
//
//
//        return shop;
//    }

    public Shop queryWithMutex(Long id){
        //从redis中查询店铺缓存
            String key = CACHE_SHOP_KEY + id;
            String shopJson = stringRedisTemplate.opsForValue().get(key);

            //判断是否存在
            if (StrUtil.isNotBlank(shopJson)) {
                //存在则返回
                Shop shop = JSONUtil.toBean(shopJson, Shop.class);
                return shop;
            }
            //判断命中是否是空值
            if (shopJson != null) {
                //命中空值，返回错误信息
                return null;
            }
            //实现缓存重建
            //获取互斥锁
            String lockKey = RedisConstants.LOCK_SHOP_KEY+id;
            Shop shop=null;
        try {
            boolean isLock = tryLock(lockKey);
            //判断是否获取成功
            if (!isLock) {
                //失败，休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }


            //成功，根据id查询数据库
            //如果缓存中没有则从数据库中查询
            shop = getById(id);
            if (shop == null) {
                //空值写入redis避免穿透缓存
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                //返回错误信息
                return null;
            }

            //存在 写入redis
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            //释放锁
            unlock(lockKey);
        }


            return shop;

    }

    public Shop queryWithPassThrough(Long id){
        String key = CACHE_SHOP_KEY+id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            //存在则返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断命中是否是空值
        if(shopJson!=null){
            //命中空值，返回错误信息
            return null;
        }

        //如果缓存中没有则从数据库中查询
        Shop shop = getById(id);
        if(shop==null){
            //空值写入redis避免穿透缓存
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回错误信息
            return null;
        }

        //存在 写入redis
        // 基础TTL 30分钟，随机增加0-10分钟
        long ttl = CACHE_SHOP_TTL + ThreadLocalRandom.current().nextInt(0, 11);
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), ttl, TimeUnit.MINUTES);
        return shop;
    }


    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key,"",10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    public void saveShop2Redis(Long id,Long expireSeconds){
        //查询店铺数据
        Shop shop = getById(id);
        //封装逻辑过期时间
        RedisData redisData=new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id==null){
            return Result.fail("店铺id不能为空");
        }
//        1.更新数据库
        updateById(shop);
        //删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY+shop.getId());
        return Result.ok();
    }
}
