package com.example.marketanalysis.beans;

import lombok.Data;

/**
 * @author wangyutian
 * @version 1.0
 * @date 2021/8/27
 */
@Data
public class MarketingUserBehavior {
    private Long userId;
    private String behavior;
    private String channel;
    private Long timestamp;

    public MarketingUserBehavior() {
    }

    public MarketingUserBehavior(Long userId, String behavior, String channel, Long timestamp) {
        this.userId = userId;
        this.behavior = behavior;
        this.channel = channel;
        this.timestamp = timestamp;
    }
}
