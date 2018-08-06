package com.fly.sync.mysql;

import com.fly.sync.action.InsertAction;
import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.setting.River;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Relation {

    private DbFactory dbFactory;
    private Map<String, List<AbstractAction>> tableActions = new HashMap<>();

    public Relation(DbFactory dbFactory) {

        this.dbFactory = dbFactory;
    }

    private void putAction(String tableName, AbstractAction action)
    {
        List<AbstractAction> actionList;
        if (tableActions.containsKey(tableName))
        {
            actionList = tableActions.get(tableName);
        } else {
            actionList = new ArrayList<>();
            tableActions.put(tableName, actionList);
        }

        actionList.add(action);
    }

    private River.Table getRiverTable(String tableName)
    {
        return dbFactory.getRiverDatabase().getTable(tableName);
    }

    private String getLocalKeys(River.Table table, String withName)
    {
        return null;
    }

    public List<AbstractAction> load(List<AbstractAction> actionList)
    {
        tableActions.clear();

        for (AbstractAction action :actionList
        ) {
            if (action instanceof InsertAction)
            {
                InsertAction a = (InsertAction)action;
                if (getRiverTable(a.table).hasWith())
                    putAction(a.table, action);
            }
        }

        for (Map.Entry<String, List<AbstractAction>> entry: tableActions.entrySet()
        ) {
            River.Table table = getRiverTable(entry.getKey());
            List<String> withNames = table.getWithNames();

            for (String withName: withNames
                 ) {
                River.Associate associate = dbFactory.getRiverDatabase().findAssociate(withName);

            }

            //getLocalKeys(table)
        }

        return actionList;
    }

    public void fillRelation(List<AbstractAction> actionList)
    {
        /*Map<String, River.SyncType> withs = table.withs;
        if (withs.size() == 0)
            return;
        for (String relationKey:withs.keySet()
             ) {

            getRelationData(actionList, relationKey);
        }*/
    }

    private void getRelationData(List<AbstractAction> actionList, String relationKey)
    {

    }

}
