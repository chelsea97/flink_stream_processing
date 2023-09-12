package com.atguigu.table.bean;

public class Shares {
    private String id;
    private Long total;
    private Double per;

    public Shares(String id, Long total, Double per) {
        this.id = id;
        this.total = total;
        this.per = per;
    }

    public String getId() {
        return id;
    }

    public Long getTotal() {
        return total;
    }

    public Double getPer() {
        return per;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public void setPer(Double per) {
        this.per = per;
    }
}
