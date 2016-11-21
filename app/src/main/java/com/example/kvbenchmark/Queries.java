package com.example.kvbenchmark;

import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;

public class Queries {

    JSONObject workloadJsonObject;
    Context context;
    Utils utils;
    //Double SELECT;
    //Double UPDATE;
    //Double INSERT;
    //Double DELETE;

    public Queries(Context inContext){
        utils = new Utils();
        workloadJsonObject = Utils.workloadJsonObject;
        context = inContext;
    }

    public int startQueries(){

        //int a[] = utils.restrictHeapTo25();
        //int a[] = utils.restrictHeapTo50();
        //utils.restrictHeapTo25();

        utils.putMarker("{\"EVENT\":\"TESTBENCHMARK\"}", "trace_marker");

        utils.putMarker("START: App started\n", "trace_marker");


/*
        utils.putMarker("{\"EVENT\":\"SQL_START\"}", "trace_marker");

        int tester = sqlQueries();
        if(tester != 0){
            return 1;
        }

        utils.putMarker("{\"EVENT\":\"SQL_END\"}", "trace_marker");
*/

        utils.putMarker("{\"EVENT\":\"KV_START\"}", "trace_marker");

        int tester = kvQueries();
        if(tester != 0){
            return 1;
        }



        utils.putMarker("{\"EVENT\":\"SAVE_START\"}", "trace_marker");
        utils.writeMapToDisk(context,MainActivity.globalMap);
        utils.putMarker("{\"EVENT\":\"SAVE_END\"}", "trace_marker");
        utils.putMarker("{\"EVENT\":\"KV_END\"}", "trace_marker");
        utils.putMarker("END: app finished\n", "trace_marker");


        return 0;
    }

    private int kvQueries(){

        utils.putMarker("{\"EVENT\":\"LOAD_START\"}", "trace_marker");
        MainActivity.globalMap = utils.readInMap(context);
        utils.putMarker("{\"EVENT\":\"LOAD_END\"}", "trace_marker");

        utils.putMarker("{\"EVENT\":\"QUERIES_START\"}", "trace_marker");

        File file2 = new File(context.getFilesDir().getPath() + "/testQueries");
        FileOutputStream fos;

        try {
            JSONArray benchmarkArray = workloadJsonObject.getJSONArray("benchmark");

            for(int i = 0; i < benchmarkArray.length(); i ++){
                JSONObject operationJson = benchmarkArray.getJSONObject(i);
                Object operationObject = operationJson.get("op");
                String operation = operationObject.toString();

                switch (operation) {
                    case "query": {

                        JSONObject kvObject = operationJson.getJSONObject("kv");
                        Object opObj = kvObject.get("op");
                        String op = opObj.toString();

                        if(op.contains("GET")){
                            Object keyObj = kvObject.get("key");
                            String key = keyObj.toString();
                            Object returned = MainActivity.globalMap.get(key);

                            /*
                            try {
                                fos = context.openFileOutput(file2.getName(), Context.MODE_APPEND);
                                fos.write(("Op: " + op + "\n").getBytes());
                                fos.write(("Key: " + key + "\n").getBytes());
                                //fos.write(("Value: " + value + "\n").getBytes());
                                fos.close();

                            } catch (IOException e) {
                                e.printStackTrace();
                            }*/


                            break;
                        }
                        else if(op.contains("PUT")) {
                            Object keyObj = kvObject.get("key");
                            String key = keyObj.toString();
                            Object valueObj = kvObject.get("value");
                            String value = valueObj.toString();
                            MainActivity.globalMap.put(key,value);

                            /*
                            try {
                                fos = context.openFileOutput(file2.getName(), Context.MODE_APPEND);
                                fos.write(("Op: " + op + "\n").getBytes());
                                fos.write(("Key: " + key + "\n").getBytes());
                                fos.write(("Value: " + value + "\n").getBytes());
                                fos.close();

                            } catch (IOException e) {
                                e.printStackTrace();
                            }*/

                            break;
                        }

                        else if(op.contains("SCAN")){
                            Object keyObj = kvObject.get("key");
                            String key = keyObj.toString();
                            Object countObj = kvObject.get("count");
                            String count = countObj.toString();

                            if(MainActivity.globalMap.containsKey(key)) {
                                NavigableMap<String, Object> returnedMap = MainActivity.globalMap.headMap(key, true);
                                Set<String> keys = returnedMap.keySet();
                                int counting = 0;
                                String keyAfterCount = null;

                                for(String keySetKey : keys){
                                    if(counting == Integer.valueOf(count)){
                                        keyAfterCount = keySetKey;
                                    }
                                    counting++;
                                }
                                if(keyAfterCount != null) {
                                    NavigableMap<String, Object> finalMap = returnedMap.tailMap(keyAfterCount, true);
                                }
                            }
                            //int biggestKeyInt = Integer.getInteger(key) + Integer.getInteger(count);
                            //String biggestKey = Integer.toString(biggestKeyInt);
                            //SortedMap<String,Object> finalMap = MainActivity.globalMap.subMap(key,biggestKey);
                            //SortedMap<String,Object> finalMap = returnedMap.subMap(returnedMap.firstKey(),biggestKey);
                            /*
                            try {
                                fos = context.openFileOutput(file2.getName(), Context.MODE_APPEND);
                                fos.write(("Op: " + op + "\n").getBytes());
                                fos.write(("Key: " + key + "\n").getBytes());
                                fos.write(("Count: " + count + "\n").getBytes());
                                fos.close();

                            } catch (IOException e) {
                                e.printStackTrace();
                            }*/

                            break;
                        }
                        //ask about seq not having more then one op defined.....
                        else if(op.contains("SEQ")){
                            Object seqObj = kvObject.get("seq");
                            String seq = seqObj.toString();


                            try {
                                fos = context.openFileOutput(file2.getName(), Context.MODE_APPEND);
                                fos.write(("Op: " + op + "\n").getBytes());
                                fos.write(("Seq: " + seq + "\n").getBytes());
                                //fos.write(("Value: " + value + "\n").getBytes());
                                fos.close();

                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                            break;
                        }


                        break;
                    }
                    case "break": {


                        Object breakObject = operationJson.get("delta");
                        int breakTime = Integer.parseInt(breakObject.toString());

                        int tester = utils.sleepThread(breakTime);
                        if(tester != 0){
                            return 1;
                        }
                        /*
                        try {
                            fos = context.openFileOutput(file2.getName(), Context.MODE_APPEND);
                            fos.write(("Break: " + breakTime + "\n").getBytes());
                            fos.close();

                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        */

                        break;
                    }
                    default:
                        return 1;
                        //break;
                }

            }

        } catch (JSONException e) {
            e.printStackTrace();

            return 1;
        }
        utils.putMarker("{\"EVENT\":\"QUERIES_END\"}", "trace_marker");
        return 0;
    }

    private int sqlQueries(){

        SQLiteDatabase db = context.openOrCreateDatabase("SQLBenchmark",0,null);
        int sqlException = 0;

        try {
            JSONArray benchmarkArray = workloadJsonObject.getJSONArray("benchmark");
          //  SELECT = 0.0;
            //UPDATE = 0.0;
            //INSERT = 0.0;
            //DELETE = 0.0;

            //utils.putMarker("{\"EVENT\":\"SELECT_START\"}\n","trace_marker");
            for(int i = 0; i < benchmarkArray.length(); i ++){
                JSONObject operationJson = benchmarkArray.getJSONObject(i);
                Object operationObject = operationJson.get("op");
                String operation = operationObject.toString();

                switch (operation) {
                    case "query": {
                        //double startSql;
                        //double endSql;
                        long memBeforeQuery;
                        long memAfterQuery;
                        sqlException = 0;
                        Object queryObject = operationJson.get("sql");
                        String query = queryObject.toString();

                        try {

                            if(query.contains("SELECT")){

                                //startSql = System.currentTimeMillis();
                                memBeforeQuery = utils.memoryAvailable(context);
                                Cursor cursor = db.rawQuery(query,null);
                                memAfterQuery = utils.memoryAvailable(context);
                                //endSql = System.currentTimeMillis();
                                if(cursor.moveToFirst()) {
                                    int numColumns = cursor.getColumnCount();
                                    do {
                                        int j=0;
                                        while (j< numColumns) {
                                            j++;
                                        }
                                            //String temp = cursor.toString();
                                        //}
                                        //process cursor
                                    } while(cursor.moveToNext());
                                }
                                cursor.close();

                               // SELECT++;
                            }
                            else {
                                //startSql = System.currentTimeMillis();
                                memBeforeQuery = utils.memoryAvailable(context);
                                db.execSQL(query);
                                memAfterQuery = utils.memoryAvailable(context);
                                //endSql = System.currentTimeMillis();
                                /*
                                if(query.contains("UPDATE")) {
                                    UPDATE++;
                                }
                                if(query.contains("INSERT")){
                                    INSERT++;
                                }
                                if(query.contains("DELETE")){
                                    DELETE++;
                                }*/
                            }
                            /*
                            double delta = endSql - startSql;
                            double elapsedSeconds = delta / 1000.00000;
                            File file = new File(context.getFilesDir().getPath() + "/testSQL");
                            FileOutputStream fos = context.openFileOutput(file.getName(), Context.MODE_APPEND);
                            fos.write((elapsedSeconds + ": " + query + "\n").getBytes());
                            fos.close();
*/
                            /*
                            File file2 = new File(context.getFilesDir().getPath() + "/MemorySQL");
                            FileOutputStream fos2;
                            fos2 = context.openFileOutput(file2.getName(), Context.MODE_APPEND);
                            fos2.write(("B Available: " + memBeforeQuery + "\n").getBytes());
                            fos2.write(("B Available: " + memAfterQuery + '\n').getBytes());
                            fos2.close();
                            */


                        }
                        catch (SQLiteException e){
                            sqlException = 1;
                            continue;
                        }
                        break;
                    }
                    case "break": {

                        if(sqlException == 0) {
                            Object breakObject = operationJson.get("delta");
                            int breakTime = Integer.parseInt(breakObject.toString());
                            int tester = utils.sleepThread(breakTime);
                            if(tester != 0){
                                return 1;
                            }

                        }
                        sqlException = 0;
                        break;
                    }
                    default:
                        db.close();
                        return 1;
                }

            }

            //utils.putMarker("{\"EVENT\":\"SELECT_END\"}\n","trace_marker");

        } catch (JSONException e) {
            e.printStackTrace();
            db.close();
            return 1;
        } /*catch (FileNotFoundException e) {
            e.printStackTrace();
            db.close();
            return 1;
        } catch (IOException e) {
            e.printStackTrace();
            db.close();
            return 1;
        }*/
        db.close();
        return 0;
    }

}
