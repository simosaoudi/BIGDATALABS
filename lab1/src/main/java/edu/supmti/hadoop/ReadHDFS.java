package edu.supmti.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
// Assurez-vous d'avoir les imports pour Configuration, FileSystem et Path
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        
        // Vérification des arguments
        if (args.length != 1) {
             System.err.println("Usage: hadoop jar ... ReadHDFS /chemin/vers/achats.txt");
             System.exit(1);
        }
        
        Configuration conf = new Configuration();
        // Configuration de la connexion au NameNode
        conf.set("fs.defaultFS", "hdfs://hadoop-master:9000");
        
        FileSystem fs = FileSystem.get(conf);
        
        // Utilisation du premier argument comme chemin HDFS
        Path nomcomplet = new Path(args[0]);
        
        System.out.println("--- Démarrage de la lecture du fichier : " + args[0] + " ---");
        
        FSDataInputStream inStream = fs.open(nomcomplet);
        InputStreamReader isr = new InputStreamReader(inStream);
        BufferedReader br = new BufferedReader(isr);
        
        String line = null;
        
        // Lecture et affichage ligne par ligne
        while((line = br.readLine()) != null) {
            System.out.println(line);
        }
        
        System.out.println("--- Lecture terminée ---");

        // Fermeture des ressources (de br à fs)
        br.close();
        isr.close();
        inStream.close();
        fs.close();
    }
}