package com.example.marketanalysis.beans;

import lombok.Data;

/**
 * @author yutian
 * @version 1.0
 * @date 2021/8/27
 */
@Data
public class AdCountViewByProvince {
    private String province;
    private String windowEnd;
    private Long count;

    public AdCountViewByProvince() {
    }

    public AdCountViewByProvince(String province, String windowEnd, Long count) {
        this.province = province;
        this.windowEnd = windowEnd;
        this.count = count;
    }
}
