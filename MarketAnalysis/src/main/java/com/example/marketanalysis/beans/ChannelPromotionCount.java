package com.example.marketanalysis.beans;

import lombok.Data;

/**
 * @author wangyutian
 * @version 1.0
 * @date 2021/8/27
 */
@Data
public class ChannelPromotionCount {
    private String channel;
    private String behavior;
    private String windowEnd;
    private Long count;

    public ChannelPromotionCount() {
    }

    public ChannelPromotionCount(String channel, String behavior, String windowEnd, Long count) {
        this.channel = channel;
        this.behavior = behavior;
        this.windowEnd = windowEnd;
        this.count = count;
    }

}
