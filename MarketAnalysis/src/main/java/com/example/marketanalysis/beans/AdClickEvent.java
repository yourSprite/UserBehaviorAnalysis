package com.example.marketanalysis.beans;

import lombok.Data;

/**
 * @author yutian
 * @version 1.0
 * @date 2021/8/27
 */
@Data
public class AdClickEvent {
    private Long userId;
    private Long adId;
    private String province;
    private String city;
    private Long timestamp;

    public AdClickEvent() {
    }

    public AdClickEvent(Long userId, Long adId, String province, String city, Long timestamp) {
        this.userId = userId;
        this.adId = adId;
        this.province = province;
        this.city = city;
        this.timestamp = timestamp;
    }
}
