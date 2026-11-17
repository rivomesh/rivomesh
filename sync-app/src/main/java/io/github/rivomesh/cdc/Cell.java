package io.github.rivomesh.cdc;

import java.util.Objects;

/** A missing field differs from SQL NULL. Values use lossless textual/binary-base64 wire encoding. */
public final class Cell {
    public boolean present;
    public String value;
    public Cell() {}
    private Cell(boolean present,String value){this.present=present;this.value=value;}
    public static Cell value(String value){return new Cell(true,value);}
    public static Cell missing(){return new Cell(false,null);}
    public void validate(){if(!present&&value!=null)throw new IllegalArgumentException("MISSING_FIELD_HAS_VALUE");}
    @Override public boolean equals(Object other){return other instanceof Cell&&present==((Cell)other).present&&Objects.equals(value,((Cell)other).value);}
    @Override public int hashCode(){return Objects.hash(present,value);}
}
