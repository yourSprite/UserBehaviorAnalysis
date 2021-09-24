package com.example.networkflowanalysis.beans;

import lombok.Data;

/**
 * @author wangyutian
 * @version 1.0
 * @date 2021/8/23
 */
@Data
public class PageViewCount {
    private String url;
    private Long windowEnd;
    private Long count;

    public PageViewCount() {
    }

    public PageViewCount(String url, Long windowEnd, Long count) {
        this.url = url;
        this.windowEnd = windowEnd;
        this.count = count;
    }
}
