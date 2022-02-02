package com.monetdb.ais;

import java.util.List;

public interface IOutputWriter {
    public void Write(List<List<String>> messages);
}
