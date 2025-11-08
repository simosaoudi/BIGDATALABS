package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteHDFS { 
    public static void main(String[] args) throws IOException {
        
        // Vérification des arguments
        if (args.length != 2) {
            System.err.println("Usage: hadoop jar ... WriteHDFS <HDFS_PATH> \"<Content>\"");
            System.exit(1);
        }
        
        // 1. Correction de la Configuration HDFS (Connexion au NameNode)
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop-master:9000"); 
        
        FileSystem fs = FileSystem.get(conf);
        
        Path nomcomplet = new Path(args[0]);
        String contentToWrite = args[1]; // Contenu fourni en deuxième argument

        // 2. Création et Écriture du fichier
        // Note: Le code du TP vérifie si le fichier n'existe PAS.
        if (!fs.exists(nomcomplet)) { 
            
            System.out.println("Création du fichier : " + args[0]);
            
            // Crée le fichier
            FSDataOutputStream outStream = fs.create(nomcomplet); 
            
            // Écriture des données
            outStream.writeUTF("Bonjour tout le monde !");
            outStream.writeUTF(contentToWrite); // Écrit le contenu de args[1]
            
            outStream.close();
            
            System.out.println("Écriture terminée. Le contenu du TP et votre argument y sont.");

        } else {
            System.out.println("Le fichier existe déjà : " + args[0] + ". Écriture ignorée.");
        }
        
        fs.close();
    }
}