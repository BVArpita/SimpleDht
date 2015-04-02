package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;



public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    boolean dht=false;
    String successoris="";
    String predecessoris="";

    HashMap <String ,List<String>> hm = new HashMap<String,List<String>>();
    List<String> listofnodes = new LinkedList<String>();
    List<String> listofhashnodes = new LinkedList<String>();




    public class Message implements Serializable{
         String   msg;
        String toport;
        String amport;

        public Message(String message, String portnumber,String sendtoport){
            msg=message;
            amport=portnumber;
            toport=sendtoport;
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    public  String getport(){
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final  String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        return myPort;
    }
    @Override
    public Uri insert(Uri uri, ContentValues values)  {

        ArrayList<String> portstrings= new ArrayList<String>();
        portstrings.add("5554");
        portstrings.add("5556");
        portstrings.add("5558");
        portstrings.add("5560");
        portstrings.add("5562");


        String[] filelist=getContext().fileList();
        String key= values.getAsString("key");
        String val=values.getAsString("value");

        try {
            String keyid = genHash(key);

        String myport = getport();
        String portstring = Integer.toString(Integer.parseInt(myport)/2);
        if((genHash(portstring).compareTo(keyid) == 0 || genHash(portstring).compareTo(keyid) == 1) || dht == false){ // if nodeid >= keyid, store in the same content provider

        Log.d(TAG,"in insert"+" "+dht+" "+portstring);

        for(String f : filelist) { //to check if file with key name already exists and if so , update value.
            if (f.equals(key)) {
                try {
                    FileOutputStream fos =getContext().openFileOutput(key, Context.MODE_PRIVATE);


                    fos.write(val.getBytes());

                    fos.close();

                    return uri;
                }
                catch(IOException e){
                    Log.e(TAG, "Unable to open file");
                }
            }
        }

        try {
            FileOutputStream fos =getContext().openFileOutput(key, Context.MODE_PRIVATE);


            fos.write(val.getBytes());

            fos.close();

            return uri;
        }
        catch(IOException e){
            Log.e(TAG, "Unable to open file");
        }

        Log.v("insert", values.toString());
        //return uri;
        }
        else{
            Log.d(TAG,"in insert"+portstring+" "+dht);
             String successor_k="";
            //Log.d(TAG,)
            String successorid_k=successoris;
            for(int i=0;i<portstrings.size();i++){
                if (successorid_k.equals(genHash(portstrings.get(i)))){
                    String msg = "partitionin"+key +"-"+val;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,successor_k);
                    //successor_k =portstrings.get(i);
                }
            }



        }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
            return uri;



       // return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        Log.d(TAG,"hello");
       // SimpleDhtActivity ada=new SimpleDhtActivity();
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final  String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        if(!myPort.equals("11108")){
            String msg="joinring"+portStr;
            Log.d(TAG,"msg sent from client is"+" "+msg);
            String portnumber=myPort;
            String sendtoport="11108";
            Message m1 = new Message(msg,portnumber,sendtoport);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,sendtoport);
            Log.d(TAG,"Not 11108");

        }


        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {

            Log.e(TAG, "Can't create a ServerSocket");
            //return;
        }








        return false;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        // private final ContentResolver amContentResolver=getContentResolver();
        private Uri amUri;


        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Integer key = 0;
            //String value;
             amUri = buildUri("content", "edu.buffalo.cse.cse486586..simpledht.provider"); //change this


            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                     BufferedReader br = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                   // ObjectInputStream br = new ObjectInputStream(clientSocket.getInputStream());

                    String recievedmessage =  br.readLine();
                    if (recievedmessage.substring(0,8).equals("joinring")) {
                        if(!listofnodes.contains("5554")){
                            listofnodes.add("5554");

                        }
                        String masterhash=genHash("5554");
                        if(!listofhashnodes.contains(masterhash)){
                            listofhashnodes.add(masterhash);

                        }
                        Log.d(TAG,"at server"+" "+recievedmessage.substring(8,12));
                        //send awk
                        PrintWriter out1 = new PrintWriter(clientSocket.getOutputStream(), true);
                        out1.println("awk");
                        getnodeid(recievedmessage.substring(8, 12));

                    }
                    else if(recievedmessage.substring(0,8).equals("pointers")){
                        successoris=recievedmessage.substring(8,recievedmessage.lastIndexOf("-"));
                        predecessoris=recievedmessage.substring(recievedmessage.lastIndexOf("-")+1);
                        Log.d(TAG,"successoris"+" "+successoris);
                        Log.d(TAG,"prdecessoris"+" "+predecessoris);
                    }
                    else if(recievedmessage.substring(0,9).equals("partitionin")){
                        ContentValues val =new ContentValues();
                        val.put("key",recievedmessage.substring(9,recievedmessage.lastIndexOf("-")));
                        val.put("value",recievedmessage.substring(recievedmessage.lastIndexOf("-")+1));

                        amUri= insert(amUri,val);

                    }
                } catch (IOException e) {
                    Log.e(TAG, "Accept Failed");
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }


            }
        }



    }

    public void getnodeid(String port) throws NoSuchAlgorithmException {
        //String emid=(Integer.toString(Integer.parseInt(port)/2));

        Log.d(TAG,"getnodeid"+ " "+port);
        listofnodes.add(port);
        String hash2 = genHash(port);
        listofhashnodes.add(hash2);
        setpointers(listofhashnodes,listofnodes);
        //String master= genHash("5554");
        // Log.d(TAG,"hash of master is"+" "+master);
        Log.d(TAG,"hash of hash2 is"+" "+port+hash2);

    }
    public void setpointers(List listofhashnodes,List listofnodes) throws NoSuchAlgorithmException {
        Collections.sort(listofhashnodes);
        Integer numberofnodes = listofhashnodes.size();
        for(int i=0;i<numberofnodes-1;i++){ // adding successors
            //List<String> successor = new LinkedList<String>();
            //successor.add(listofnodes.get(i+1).toString());
            String successor = listofhashnodes.get(i+1).toString();
            List<String> pointersu = new LinkedList<String>();
            pointersu.add(successor);
            hm.put(listofhashnodes.get(i).toString(),pointersu);
        }
        List<String> pointersu = new LinkedList<String>();
        pointersu.add(listofhashnodes.get(0).toString());
        hm.put(listofhashnodes.get(numberofnodes-1).toString(),pointersu);

        for(int j=numberofnodes-1;j>0;j--){
            String predecessor= listofhashnodes.get(j-1).toString();
            List<String> pointerpr;
            //pointerpr.add(predecessor);
            pointerpr=hm.get(listofhashnodes.get(j).toString());
            pointerpr.add(predecessor);
            hm.put(listofhashnodes.get(j).toString(),pointerpr);
        }
        List<String> pointerpr ;
        pointerpr = hm.get(listofhashnodes.get(0).toString());
        pointerpr.add(listofhashnodes.get(numberofnodes-1).toString());
        hm.put(listofhashnodes.get(0).toString(),pointerpr);
        Log.d(TAG,"5554"+" "+hm.get(genHash("5554")));
        Log.d(TAG,"5556"+" "+hm.get(genHash("5556")));
         Log.d(TAG,"5558"+" "+hm.get(genHash("5558")));
         Log.d(TAG,"5560"+" "+hm.get(genHash("5560")));
        Log.d(TAG,"5562"+" "+hm.get(genHash("5562")));

      /* String msg5556="pointers"+hm.get(genHash("5556")).get(0)+"-"hm.get(genHash("5556")).get(1);
        String msg5558="pointers"+hm.get(genHash("5558")).get(0)+"-"hm.get(genHash("5558")).get(1);
        String msg5560="pointers"+hm.get(genHash("5560")).get(0)+"-"hm.get(genHash("5560")).get(1);
        String msg5562="pointers"+hm.get(genHash("5562")).get(0)+"-"hm.get(genHash("5562")).get(1);*/

        //send listofhashnodes to all ports
       for(int i=0;i<listofnodes.size();i++){
           String sendtoport =Integer.toString(Integer.parseInt(listofnodes.get(i).toString()) * 2);
           String msg = "pointers"+hm.get(genHash(listofnodes.get(i).toString())).get(0)+"-"+hm.get(genHash(listofnodes.get(i).toString())).get(1);
           new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,sendtoport);
       }


    }


    private class ClientTask extends AsyncTask<String, Void, Void> {
        protected Void doInBackground(String... m1) {
            try {

               // Log.d(TAG,"toport is"+" "+m1[0].toport);
                //Message msgToSend = m1[0];
                Socket socket0 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(m1[1]));
                //ObjectOutputStream out =new ObjectOutputStream(socket0.getOutputStream());

                PrintWriter out = new PrintWriter(socket0.getOutputStream(), true);
                out.println(m1[0]);
                Log.d(TAG,"msg sent");
                BufferedReader br = new BufferedReader(new InputStreamReader(socket0.getInputStream()));
                if(br.readLine()!=null ){
                    if(br.readLine().equals("awk"))
                        dht=true;
                }
                socket0.close();


            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                e.printStackTrace();
            }

            return null;
        }
    }
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        String[] filelist=getContext().fileList();
        for(String f : filelist){
            if(f.equals(selection)){
                try{                                //Reference from http://stackoverflow.com/questions/14768191/how-do-i-read-the-file-content-from-the-internal-storage-android-app
                    //String path=getContextt
                    FileInputStream fis = getContext().openFileInput(selection);
                    InputStreamReader isr = new InputStreamReader(fis);
                    BufferedReader bufferedReader = new BufferedReader(isr);
                    StringBuilder sb = new StringBuilder();
                    String line="";
                    //to debug


                    while ((line = bufferedReader.readLine()) != null) {
                        sb.append(line);
                    }
                    //Log.d("Value of line",line);
                    MatrixCursor cursor=new MatrixCursor(new String[]{"key","value"} );
                    cursor.addRow(new Object[] { selection, sb.toString() });
                    return cursor;
                }
                catch(IOException e){
                    Log.e(TAG, "Unable to read from file");
                }
            }

           }
        if(selection.equals("\"*\"" ) || selection.equals("\"@\"" )){ //returning all key-value pairs
            MatrixCursor cursor=new MatrixCursor(new String[]{"key","value"} );
            for(String f : filelist){

                    try{                                //Reference from http://stackoverflow.com/questions/14768191/how-do-i-read-the-file-content-from-the-internal-storage-android-app
                        //String path=getContextt
                        FileInputStream fis = getContext().openFileInput(f);
                        InputStreamReader isr = new InputStreamReader(fis);
                        BufferedReader bufferedReader = new BufferedReader(isr);
                        StringBuilder sb = new StringBuilder();
                        String line="";
                        //to debug


                        while ((line = bufferedReader.readLine()) != null) {
                            sb.append(line);
                        }
                        //Log.d("Value of line",line);
                        // cursor=new MatrixCursor(new String[]{"key","value"} );
                        cursor.addRow(new Object[] { f, sb.toString() });
                        //return cursor;
                    }
                    catch(IOException e){
                        Log.e(TAG, "Unable to read from file");
                    }


            }
            return cursor;
        }
        Log.v("query", selection);


        return null;
       // return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
