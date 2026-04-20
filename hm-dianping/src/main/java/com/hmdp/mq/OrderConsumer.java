package com.hmdp.mq;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;

@Slf4j
@Component
public class OrderConsumer {
    @Resource
    private IVoucherOrderService voucherOrderService;

    @RabbitListener(queues="order.queue")
    public void handleOrder(Map<String,Object> msg){
        Long userId=Long.valueOf(msg.get("userId").toString());
        Long voucherId=Long.valueOf(msg.get("voucherId").toString());
        Long orderId=Long.valueOf(msg.get("orderId").toString());

        VoucherOrder voucherOrder=new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);

        voucherOrderService.handleVoucherOrder(voucherOrder);
    }

}
