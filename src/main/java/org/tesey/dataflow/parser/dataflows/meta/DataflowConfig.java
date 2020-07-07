package org.tesey.dataflow.parser.dataflows.meta;

public class DataflowConfig {

    private String name;

    private String source;

    private String select;

    private String filter;

    private String sink;

    private Boolean isFirst = false;

    private DataflowJoin join;

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSource() {
        return this.source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getSelect() {
        return this.select;
    }

    public void setSelect(String select) {
        this.select = select;
    }

    public String getFilter() {
        return this.filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public String getSink() {
        return this.sink;
    }

    public void setSink(String sink) {
        this.sink = sink;
    }

    public Boolean getIsFirst() {
        return this.isFirst;
    }

    public void setIsFirst(Boolean isFirst) {
        this.isFirst = isFirst;
    }

    public DataflowJoin getJoin() {
        return this.join;
    }

    public void setJoin(DataflowJoin join) {
        this.join = join;
    }

}
