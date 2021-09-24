package com.example.networkflowanalysis.beans;

import lombok.Data;

/**
 * @author wangyutian
 * @version 1.0
 * @date 2021/8/23
 */
@Data
public class ApacheLogEvent {
    private String ip;
    private String userId;
    private Long timestamp;
    private String method;
    private String url;

    public ApacheLogEvent() {
    }

    public ApacheLogEvent(String ip, String userId, Long timestamp, String method, String url) {
        this.ip = ip;
        this.userId = userId;
        this.timestamp = timestamp;
        this.method = method;
        this.url = url;
    }
}
