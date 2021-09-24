package com.example.marketanalysis.beans;

import lombok.Data;

/**
 * @author yutian
 * @version 1.0
 * @date 2021/8/28
 */
@Data
public class BlackListUserWarning {
    private Long userId;
    private Long adId;
    private String warningMsg;

    public BlackListUserWarning() {
    }

    public BlackListUserWarning(Long userId, Long adId, String warningMsg) {
        this.userId = userId;
        this.adId = adId;
        this.warningMsg = warningMsg;
    }
}
