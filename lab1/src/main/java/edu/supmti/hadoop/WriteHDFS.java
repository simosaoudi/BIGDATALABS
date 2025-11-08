package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteHDFS { 
    public static void main(String[] args) throws IOException {
   
        if (args.length != 2) {
            System.err.println("Usage: hadoop jar ... WriteHDFS <HDFS_PATH> \"<Content>\"");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop-master:9000"); 
        
        FileSystem fs = FileSystem.get(conf);
        
        Path nomcomplet = new Path(args[0]);
        String contentToWrite = args[1]; 

        if (!fs.exists(nomcomplet)) { 
            
            System.out.println("Création du fichier : " + args[0]);

            FSDataOutputStream outStream = fs.create(nomcomplet); 
            
        
            outStream.writeUTF("Bonjour tout le monde !");
            outStream.writeUTF(contentToWrite); 
            
            outStream.close();
            
            System.out.println("Écriture terminée. Le contenu du TP et votre argument y sont.");

        } else {
            System.out.println("Le fichier existe déjà : " + args[0] + ". Écriture ignorée.");
        }
        
        fs.close();
    }
}