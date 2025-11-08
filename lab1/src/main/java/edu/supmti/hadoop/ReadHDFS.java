package edu.supmti.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {

        if (args.length != 1) {
             System.err.println("Usage: hadoop jar ... ReadHDFS /chemin/vers/achats.txt");
             System.exit(1);
        }
        
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://hadoop-master:9000");
        
        FileSystem fs = FileSystem.get(conf);

        Path nomcomplet = new Path(args[0]);
        
        System.out.println("--- Démarrage de la lecture du fichier : " + args[0] + " ---");
        
        FSDataInputStream inStream = fs.open(nomcomplet);
        InputStreamReader isr = new InputStreamReader(inStream);
        BufferedReader br = new BufferedReader(isr);
        
        String line = null;

        while((line = br.readLine()) != null) {
            System.out.println(line);
        }
        
        System.out.println("--- Lecture terminée ---");

        br.close();
        isr.close();
        inStream.close();
        fs.close();
    }
}