package org.fly.sync.mysql.model;

import org.fly.sync.contract.ModelInterface;

import java.beans.ConstructorProperties;

public class Variable implements ModelInterface {
    public String variableName;
    public String value;

    @ConstructorProperties({"Variable_name", "Value"})
    public Variable(String variableName, String value) {
        this.variableName = variableName;
        this.value = value;
    }
}
