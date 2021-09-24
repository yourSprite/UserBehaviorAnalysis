package com.example.loginfaildetect.beans;

import lombok.Data;

/**
 * @author yutian
 * @version 1.0
 * @date 2021/8/29
 */
@Data
public class LoginFailWarning {
    private Long userId;
    private Long firstFailTime;
    private Long lastFailTime;
    private String warningMsg;

    public LoginFailWarning() {
    }

    public LoginFailWarning(Long userId, Long firstFailTime, Long lastFailTime, String warningMsg) {
        this.userId = userId;
        this.firstFailTime = firstFailTime;
        this.lastFailTime = lastFailTime;
        this.warningMsg = warningMsg;
    }
}
