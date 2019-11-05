package com.alibaba.otter.canal.store.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;
import com.alibaba.otter.canal.protocol.position.PositionRange;

/**
 * 代表一组数据对象的集合
 *
 * 仅仅是通过一个List维护了一组数据，尽管这里定义的是泛型，但真实放入的数据实际上是Event类型。
 * 而PositionRange是protocol模块中的类，描述了这组Event的开始(start)和结束位置(end)，
 * 显然，start表示List集合中第一个Event的位置，end表示最后一个Event的位置。
 *
 * @author jianghang 2012-6-14 下午09:07:41
 * @version 1.0.0
 */
public class Events<EVENT> implements Serializable {

    private static final long serialVersionUID = -7337454954300706044L;

    private PositionRange     positionRange    = new PositionRange();
    private List<EVENT>       events           = new ArrayList<EVENT>();

    public List<EVENT> getEvents() {
        return events;
    }

    public void setEvents(List<EVENT> events) {
        this.events = events;
    }

    public PositionRange getPositionRange() {
        return positionRange;
    }

    public void setPositionRange(PositionRange positionRange) {
        this.positionRange = positionRange;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }
}
