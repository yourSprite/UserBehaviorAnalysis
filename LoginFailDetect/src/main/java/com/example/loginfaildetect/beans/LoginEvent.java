package com.example.loginfaildetect.beans;

import lombok.Data;

/**
 * @author yutian
 * @version 1.0
 * @date 2021/8/29
 */
@Data
public class LoginEvent {
    private Long userId;
    private String ip;
    private String loginState;
    private Long timestamp;

    public LoginEvent() {
    }

    public LoginEvent(Long userId, String ip, String loginState, Long timestamp) {
        this.userId = userId;
        this.ip = ip;
        this.loginState = loginState;
        this.timestamp = timestamp;
    }
}
