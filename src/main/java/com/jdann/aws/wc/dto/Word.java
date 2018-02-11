package com.jdann.aws.wc.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Comparator;

public class Word {

    @JsonIgnore
    private String address;
    private String word;
    private Long total;

    public Word(String address, String word) {
        this.address = address;
        this.word = word;
        this.total = 0L;
    }

    public Word() {}

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public static Comparator<Word> byTotal = (Word w1, Word w2) -> w2.getTotal().compareTo(w1.getTotal());
}
