package com.example.kvbenchmark;

import android.app.ActivityManager;
import android.content.Context;
import android.os.Environment;

import org.json.JSONException;
import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StreamCorruptedException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

public class Utils {

    static JSONObject workloadJsonObject;

    public String jsonToString(Context context, int workload){

        String line;
        String finalString = "";

        try {

            InputStream is = context.getResources().openRawResource(workload);
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            while((line = br.readLine()) != null){
                if(!line.contains("sql") && !line.contains("value")) {
                    line = line.replaceAll("\\s+", "");
                    finalString = finalString + line;
                }
                else {
                    finalString = finalString + line;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return finalString;
    }

    public JSONObject jsonStringToObject(String jsonString){
        JSONObject jsonObject = null;
        try {
            jsonObject = new JSONObject(jsonString);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        workloadJsonObject = jsonObject;
        return jsonObject;
    }

    public int sleepThread(int interval) {
        try {
            Thread.sleep(interval);
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
        return 0;
    }

    public int findMissingQueries(Context context){
        try {
            BufferedReader br1;
            BufferedReader br2;
            String sCurrentLine;
            List<String> list1 = new ArrayList<>();
            List<String> list2 = new ArrayList<>();
            br1 = new BufferedReader(new FileReader(context.getFilesDir().getPath() + "/beforeSaveKV"));
            br2 = new BufferedReader(new FileReader(context.getFilesDir().getPath() + "/afterSaveKV"));
            while ((sCurrentLine = br1.readLine()) != null) {
                list1.add(sCurrentLine);
            }
            while ((sCurrentLine = br2.readLine()) != null) {
                list2.add(sCurrentLine);
            }
            List<String> tmpList = new ArrayList<>(list1);
            tmpList.removeAll(list2);

            tmpList = list2;
            tmpList.removeAll(list1);

            String dbMissingQueries;
            if(list1.size() < list2.size()){
                dbMissingQueries = "notInAfter";
            }
            else if(list1.size() > list2.size()){
                dbMissingQueries = "notInBefore";
            }
            else {
                dbMissingQueries = "bothRanSame";
            }

            for (int i = 0; i < tmpList.size(); i++) {
                File file = new File(context.getFilesDir().getPath() + "/" + dbMissingQueries);
                FileOutputStream fos = context.openFileOutput(file.getName(), Context.MODE_APPEND);
                String temp = tmpList.get(i) + "\n";
                fos.write(temp.getBytes());
                fos.close();
            }


        } catch (IOException e) {
            e.printStackTrace();
            return 1;
        }

        return 0;
    }

    public int putMarker(String mark,String filename) {
        PrintWriter outStream = null;
        try{
            FileOutputStream fos = new FileOutputStream("/sys/kernel/debug/tracing/" + filename);
            outStream = new PrintWriter(new OutputStreamWriter(fos));
            outStream.println(mark);
            outStream.flush();
        }
        catch(Exception e) {
            return 1;
        }
        finally {
            if (outStream != null) {
                outStream.close();
            }
        }
        return 0;
    }

    public boolean doesDBExist(Context context, String dbPath){
        File dbFile = new File(context.getFilesDir().getPath() + "/" + dbPath);
        return dbFile.exists();
    }

    public long memoryAvailable(Context context){
        ActivityManager.MemoryInfo mi = new ActivityManager.MemoryInfo();
        ActivityManager activityManager = (ActivityManager) context.getSystemService(Context.ACTIVITY_SERVICE);
        activityManager.getMemoryInfo(mi);
        //double availableMegs = mi.availMem / 1048576L;
        //return availableMegs;
        return mi.availMem;
    }

    public void restrictHeapTo50(){
        //noinspection MismatchedReadAndWriteOfArray
        MainActivity.a = new int[25165824];
        for(int i = 0; i < 25165824; i++){
            MainActivity.a[i] = i;
        }
        //return a;
    }

    public void restrictHeapTo25(){
        int temp = 25165824 + 12582912;
        //noinspection MismatchedReadAndWriteOfArray
        MainActivity.a = new int[temp];
        for(int i = 0; i < temp; i++){
            MainActivity.a[i] = i;
        }
        //return a;
    }

    public void restrictHeapTo12_5(){
        int temp = 25165824 + 12582912 + 6291456;
        //noinspection MismatchedReadAndWriteOfArray
        MainActivity.a = new int[temp];
        for(int i = 0; i < temp; i++){
            MainActivity.a[i] = i;
        }
    }

    public void getMaxHeapAppCanUse(Context context){
        Runtime rt = Runtime.getRuntime();
        long maxMemory = rt.maxMemory();
        File file2 = new File(context.getFilesDir().getPath() + "/max_memory");
        FileOutputStream fos2;
        try {
            fos2 = context.openFileOutput(file2.getName(), Context.MODE_APPEND);
            fos2.write(("Max memomy app can use : " + maxMemory + "\n").getBytes());
            fos2.write(("Max kb : " + maxMemory / 1024L + "\n").getBytes());
            fos2.write(("Max mb : " + maxMemory / 1048576L + "\n").getBytes());
            fos2.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void printMap(Context context, TreeMap<String,Object> map, String saveName){
        File file2 = new File(context.getFilesDir().getPath() + saveName);
        FileOutputStream fos;
        try {
            for (String key : map.keySet()) {
                fos = context.openFileOutput(file2.getName(), Context.MODE_APPEND);
                fos.write(("Key: " + key + "\n").getBytes());
                fos.write(("Value: " + (map.get(key)).toString() + "\n").getBytes());
                //fos.write(((opObj).toString() + "\n").getBytes());
                //fos.write(((valueObj).toString() + "\n").getBytes());
                fos.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeMapToDisk(Context context, TreeMap<String,Object> map){
        File file = new File(context.getFilesDir().getPath() + "/TreeMap.kv");
        FileOutputStream fileOutputStream = null;
        try {
            fileOutputStream = new FileOutputStream(file);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(map);
            //objectOutputStream.flush();
            objectOutputStream.close();
            //fileOutputStream.flush();
            fileOutputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public TreeMap<String,Object> readInMap(Context context){
        File file = new File(context.getFilesDir().getPath() + "/TreeMap.kv");
        TreeMap<String,Object> myNewlyReadInMap = null;
        FileInputStream fileInputStream  = null;
        try {
            fileInputStream = new FileInputStream(file);
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);

            myNewlyReadInMap = (TreeMap) objectInputStream.readObject();
            objectInputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (OptionalDataException e) {
            e.printStackTrace();
        } catch (StreamCorruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return myNewlyReadInMap;
    }

}