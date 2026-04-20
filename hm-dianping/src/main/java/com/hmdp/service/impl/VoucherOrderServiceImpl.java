package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.BeanUtils;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;//分布式锁，防止一个人重复下单
    @Resource
    private RabbitTemplate rabbitTemplate;
    //加载秒杀脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    //static是在项目启动时执行一次，加载lua脚本
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    //------------以下RabbitMQ实现----------
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        //订单id
        long orderId = redisIdWorker.nextId("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        //2. 判断结果是否为0
        int r = result.intValue();
        if(r !=0){
            //2.1 不为0，没有购买资格
            return Result.fail(r==1?"库存不足":"不能重复下单");
        }
        Map<String,Object> msg = new HashMap<>();
        msg.put("userId",userId);
        msg.put("voucherId",voucherId);
        msg.put("orderId",orderId);
        rabbitTemplate.convertAndSend("order.exchange","order.create",msg);
        //3.返回订单id
        return Result.ok(orderId);
    }


    //----------以下Stream实现------------
    //单线程池，用于从队列里拿到订单信息，写入数据库。这个就是那个子线程
//    private static final ExecutorService SECKILL_ORDER_EXECUTOR=Executors.newSingleThreadExecutor();



//    @PostConstruct//当前类初始化完毕后开始执行。项目一启动就开子线程
//    @EventListener(ApplicationReadyEvent.class)
//    protected void init(){
//        // 创建消费者组（如果 Stream 不存在则同时创建 Stream）
//        try {
//            stringRedisTemplate.execute((RedisCallback<Object>) connection -> {
//                // MKSTREAM 选项：如果 stream 不存在则自动创建
//                connection.streamCommands().xGroupCreate(
//                        "stream.orders".getBytes(),
//                        Arrays.toString("g1".getBytes()),
//                        ReadOffset.from("0"),
//                        true   // mkstream 参数，表示如果 stream 不存在则创建
//                );
//                return null;
//            });
//            log.info("Stream consumer group 'g1' created or already exists");
//        } catch (Exception e) {
//            log.warn("Consumer group creation failed (may already exist): {}", e.getMessage());
//        }
//        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
//    }
    //阻塞队列，用于存储待处理的订单信息

//    private class VoucherOrderHandler implements Runnable{
//        String queueName = "stream.orders";
//        @Override
//        public void run() {
//            int retry=0;
//            while (retry < 30) {
//                try {
//                    // 尝试执行一个简单的 Redis 命令，检查连接是否可用
//                    stringRedisTemplate.opsForValue().get("ping");
//                    log.info("Redis 连接已就绪，开始处理订单消息");
//                    break; // 连接成功，退出等待循环
//                } catch (Exception e) {
//                    retry++;
//                    log.warn("等待 Redis 连接池初始化... 第 {} 次", retry);
//                    try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
//                }
//            }
//            if (retry >= 30) {
//                log.error("Redis 连接池未能就绪，后台线程退出");
//                return;
//            }
//            while (true){
//                try {
//                    //获取消息队列中的订单信息（没有订单就阻塞等待）
//                    //g1是消费者组，c1是消费者id XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.order >
//                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
//                            Consumer.from("g1", "c1"),
//                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
//                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
//                    );
//                    //判断消息获取是否成功
//
//                    //不成功，没有消息。继续下一次循环
//                     if(list==null||list.isEmpty()){
//                                            continue;
//                                        }
//                    MapRecord<String, Object, Object> record = list.get(0);
//                    Map<Object, Object> values = record.getValue();
//                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
//                    //MQ消费者幂等性
//                    Long userId=voucherOrder.getUserId();
//                    Long voucherId=voucherOrder.getVoucherId();
//                    String Idepotency="order:"+userId+":"+voucherId;
//                    boolean success=stringRedisTemplate.opsForValue().setIfAbsent(Idepotency,"1", Duration.ofMinutes(5));
//                    if(Boolean.FALSE.equals(success)){
//                        stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
//                        continue;
//                    }
//                    //有消息，获取成功，可以下单
//                    handleVoucherOrder(voucherOrder);
//                    //ACK确认
//                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
//                    //创建订单（写入MySQL）
//                } catch (Exception e) {
//                    log.error("处理订单异常",e);
//                    handlePendingList();
//                }
//
//            }
//        }
//
//        private void handlePendingList() {
//            while (true){
//                try {
//                    //获取PendingList中的订单信息（没有订单就阻塞等待）
//                    //g1是消费者组，c1是消费者id XREADGROUP GROUP g1 c1 COUNT 1 STREAMS streams.order >
//                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
//                            Consumer.from("g1", "c1"),
//                            StreamReadOptions.empty().count(1),
//                            StreamOffset.create(queueName, ReadOffset.from("0"))
//                    );
//                    //判断PendingList是否成功
//
//                    //不成功，没有PendingList中的异常循环。继续下一次循环
//                    if(list==null||list.isEmpty()){
//                        break;
//                    }
//                    MapRecord<String, Object, Object> record = list.get(0);
//                    Map<Object, Object> values = record.getValue();
//                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
//                    handleVoucherOrder(voucherOrder);
//                    //有消息，获取成功，可以下单
//                    //ACK确认
//                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
//                    //创建订单（写入MySQL）
//                } catch (Exception e) {
//                    log.error("处理pendingList订单异常",e);
//                    try {
//                        Thread.sleep(20);
//                    } catch (InterruptedException ex) {
//                        throw new RuntimeException(ex);
//                    }
//                }
//
//            }
//        }
//    }


//    //阻塞队列，用于存储待处理的订单信息
//    private BlockingQueue<VoucherOrder> orderTasks=new ArrayBlockingQueue<>(1024*1024);
//    private class VoucherOrderHandler implements Runnable{
//        @Override
//        public void run() {
//            while (true){
//                try {
//                    //获取队列中的订单信息（没有订单就阻塞等待）
//                    VoucherOrder voucherOrder = orderTasks.take();
//                //创建订单（写入MySQL）
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单异常",e);
//                    throw new RuntimeException(e);
//                }
//
//            }
//        }
//    }

    public void handleVoucherOrder(VoucherOrder voucherOrder) {
        //获取用户
        Long userId = voucherOrder.getUserId();
        Long voucherId=voucherOrder.getVoucherId();
        //幂等性检查，防止同一条消息被重复消费
        String idempotentKey="order:"+userId+":"+voucherId;
        Boolean success=stringRedisTemplate.opsForValue()
                .setIfAbsent(idempotentKey,"1",Duration.ofMinutes(5));
        if(Boolean.FALSE.equals(success)){
            log.info("重复消息，已忽略");
            return;
        }

        //分布式锁，一人一单，解决同一用户同时有多个线程进入createVoucherOrder
        RLock lock = redissonClient.getLock("lock:order" + userId);
      boolean isLock = lock.tryLock();
      if (!isLock) {
          log.error("不允许重复下单");
          return;
      }
        try {
            // 调用事务方法创建订单（注意事务传播）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

//    private IVoucherOrderService proxy;
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        Long userId = UserHolder.getUser().getId();
//        //订单id
//        long orderId = redisIdWorker.nextId("order");
//        //1.执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString(), String.valueOf(orderId)
//        );
//        //2. 判断结果是否为0
//        int r = result.intValue();
//        if(r !=0){
//            //2.1 不为0，没有购买资格
//            return Result.fail(r==1?"库存不足":"不能重复下单");
//        }
//        //获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();//获取当前代理对象
//
//        //3.返回订单id
//        return Result.ok(orderId);
//    }


//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        Long userId = UserHolder.getUser().getId();
//       //1.执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//        //2. 判断结果是否为0
//        int r = result.intValue();
//        if(r !=0){
//            //2.1 不为0，没有购买资格
//            return Result.fail(r==1?"库存不足":"不能重复下单");
//        }

//        VoucherOrder voucherOrder = new VoucherOrder();
//        //订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        //用户id
//        voucherOrder.setUserId(userId);
//        //代金券id
//        voucherOrder.setVoucherId(voucherId);
//        //创建blockingQueue
//        orderTasks.add(voucherOrder);
//        //获取代理对象
//         proxy = (IVoucherOrderService) AopContext.currentProxy();//获取当前代理对象
//
//    //3.返回订单id
//        return Result.ok(orderId);
//    }
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//
//        //判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀尚未开始！");
//        }
//        //判断秒杀是否已经结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已结束！");
//        }
//        //判断库存是否充足
//        if (voucher.getStock() < 1) {
//            return Result.fail("库存不足！");
//        }
//        Long userId = UserHolder.getUser().getId();
//
//        //创建锁对象
/// /        SimpleRedisLock lock = new SimpleRedisLock("order" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("order" + userId);
//        //获取锁
//        boolean isLock = lock.tryLock();
//        if (!isLock) {
//            return Result.fail("不允许重复下单");
//        }
//
//        try {
//            //获取代理对象（事务）
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();//获取当前代理对象
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            lock.unlock();
//        }
//
//    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //one person one order
        Long userId = voucherOrder.getUserId();
            //search for order
            int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
            if (count > 0) {
               log.error("用户已经购买过一次");
               return;
            }
            //扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock=stock-1")//set stock=stock-1
                    .eq("voucher_id", voucherOrder.getVoucherId())
                    .gt("stock", 0)//where id=? and stock>0
                    .update();       //创建订单

            if (!success) {
                log.error("库存不足");
                return;
            }

            //返回订单id
            save(voucherOrder);
    }
}