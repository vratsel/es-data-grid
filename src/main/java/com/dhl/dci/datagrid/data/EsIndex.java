package com.dhl.dci.datagrid.data;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
public class EsIndex {
    private String name;

    private Map<String, String> properties;

    private String filterName;

    private String queryName;

    private String resultName;

    public EsIndex(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
        filterName = name + "_filter";
        resultName = name + "_list";
        queryName = "get_" + resultName;
    }
}
