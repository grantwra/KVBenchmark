package com.example.kvbenchmark;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.StreamCorruptedException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.TreeMap;


public class CreateDB {

    Context context;
    Utils utils;

    public CreateDB(Context contextIn){
        context = contextIn;
    }

    public int create(int workload){

        utils = new Utils();
        String singleJsonString = utils.jsonToString(context, workload);
        JSONObject jsonObject = utils.jsonStringToObject(singleJsonString);
        int tester = populateKV(jsonObject);
        if(tester != 0){
            return 1;
        }

        /*

        int tester = populateSqlDb(jsonObject);
        if(tester != 0){
            return 1;
        }

        */

        return 0;
    }

    public int populateKV(JSONObject jsonObject){

        TreeMap<String,Object> map = new TreeMap<>();

        try {
            JSONArray initArray = jsonObject.getJSONArray("init");
            for (int i = 0; i < initArray.length(); i++){
                JSONObject obj2 = initArray.getJSONObject(i);
                JSONObject kvObject = obj2.getJSONObject("kv");
                Object opObj = kvObject.get("op");
                if(opObj.toString().contains("NOOP")){
                    continue;
                }
                Object keyObj = kvObject.get("key");
                Object valueObj = kvObject.get("value");
                if (opObj.toString().contains("PUT")){
                    map.put(keyObj.toString(),valueObj);
                }
            }

        } catch (JSONException e) {
            e.printStackTrace();
            return 1;
        }

        //utils.printMap(context,map,"/beforeSaveKV");

        utils.writeMapToDisk(context,map);
        //TreeMap<String,Object> myNewlyReadInMap = utils.readInMap(context);

        //utils.printMap(context,myNewlyReadInMap,"/afterSaveKV");


        return 0;
    }

    private int populateSqlDb(JSONObject jsonObject){

        SQLiteDatabase db = context.openOrCreateDatabase("SQLBenchmark",0,null);

        try {
            JSONArray initArray = jsonObject.getJSONArray("init");
            for (int i = 0; i < initArray.length(); i++){
                JSONObject obj2 = initArray.getJSONObject(i);
                Object sqlObject = obj2.get("sql");
                String sqlStatement = sqlObject.toString();
                db.execSQL(sqlStatement);

            }

        } catch (JSONException e) {
            e.printStackTrace();
            return 1;
        }
        return 0;
    }

}
