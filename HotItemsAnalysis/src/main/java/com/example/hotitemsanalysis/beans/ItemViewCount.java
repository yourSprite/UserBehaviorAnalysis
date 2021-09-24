package com.example.hotitemsanalysis.beans;

import lombok.Data;

/**
 * 实时热门商品统计结果实体类
 *
 * @author yutian
 * @version 1.0
 * @date 2021/8/22
 */
@Data
public class ItemViewCount {
    private Long itemId;
    private Long windowEnd;
    private Long count;

    public ItemViewCount() {
    }

    public ItemViewCount(Long itemId, Long windowEnd, Long count) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.count = count;
    }
}
