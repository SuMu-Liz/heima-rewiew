package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class RedissonConfig {
    @Bean
    @Primary
    public RedissonClient redissonClient() {
        //config
        Config config = new Config();
        config.useSingleServer().setAddress("redis://192.168.130.130:6379").setPassword("@Zhy12191219");
        return Redisson.create(config);
    }

}
