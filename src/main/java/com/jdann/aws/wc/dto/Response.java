package com.jdann.aws.wc.dto;

import java.util.List;

public class Response {

    private String status;

    private List<Word> words;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public List<Word> getWords() {
        return words;
    }

    public void setWords(List<Word> words) {
        this.words = words;
    }

}
