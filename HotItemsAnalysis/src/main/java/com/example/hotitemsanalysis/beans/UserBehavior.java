package com.example.hotitemsanalysis.beans;


import lombok.Data;

/**
 * 用户行为数据实体类
 *
 * @author yutian
 * @version 1.0
 * @date 2021/8/22 14
 */
@Data
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;

    public UserBehavior() {
    }

    public UserBehavior(Long userId, Long itemId, Integer categoryId, String behavior, Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }
}
