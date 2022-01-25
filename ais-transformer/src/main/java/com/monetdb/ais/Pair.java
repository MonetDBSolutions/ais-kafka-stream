package com.monetdb.ais;

public class Pair<A, T> {
    A left;
    T right;
    
    public Pair(A left, T right) {
        this.left = left;
        this.right = right;
    }

    public A getLeft() {
        return this.left;
    }

}

