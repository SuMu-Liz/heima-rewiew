package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class RefreshTokenInterceptor implements HandlerInterceptor {
    private final StringRedisTemplate stringRedisTemplate;

    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {

        //1. 获取请求头中的token
        String token = request.getHeader("authorization");
        System.out.println("拦截器获取到的token:"+token);
        if (StrUtil.isBlank(token)) {
            return true;
        }
        //2. 基于Token从Redis中获取user
        String key=RedisConstants.LOGIN_USER_KEY+token;
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(key);
        System.out.println("Redis查到的用户数据：" + userMap);
        //3.判断用户是否存在
        if(userMap.isEmpty()){
            //不存在，拦截
            return true;
        }
        //将查询到的Hash数据转换为UserDTO对象
        UserDTO userDTO=BeanUtil.fillBeanWithMap(userMap, new UserDTO(),false);
         //存在，保存用户信息到ThreadLocal
        UserHolder.saveUser(userDTO);
        //刷新token有效期
        stringRedisTemplate.expire(key,RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);
        //放行
        return true;
    }



    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        UserHolder.removeUser();
    }
}
