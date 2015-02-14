using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using SHDocVw;
using System.Net;
using System.Xml;
using System.Net.Sockets;
using System.Threading;
using System.Text.RegularExpressions;
namespace HydraAPI
{
    public class Initialize
    {
        bool hadoop_initialize = false;
        private string hadoop_path;
        private string hadoop_bin_path;
        private string hadoop_sbin_path;

        private string cmd_hadoop_sbin_path;
        private string cmd_hadoop_bin_path;


        public Initialize(string hadoop_path)
        {
            this.hadoop_path = hadoop_path;
            hadoop_bin_path = hadoop_path + "\\bin";
            hadoop_sbin_path = hadoop_path + "\\sbin";

            cmd_hadoop_bin_path = "cd " + hadoop_bin_path;
            cmd_hadoop_sbin_path = "cd " + hadoop_sbin_path;
        }


        public void start_hadoop()
        {
            //  ProcessStartInfo processStartInfo = new ProcessStartInfo("cmd", @"/c cd " +path+"&& start-dfs.cmd");

        //    string cmd_hadoop_sbin_path = "cd " + hadoop_sbin_path;
         //   string cmd_hadoop_bin_path = "cd " + hadoop_bin_path;
            string cmd_start_dfs = "start-dfs.cmd";
            string cmd_mkdir = "hadoop fs -mkdir -p " + "/user/" + Environment.UserName + "/Folder";



            //  processStartInfo.CreateNoWindow = false;  // remove the command line where you type start-dfs initially
            check_namenode_ip();

            Process process = new Process();
            process.StartInfo = initializeCmd();
            process.Start();


            StreamWriter write = process.StandardInput;

            if (hadoop_sbin_path.EndsWith("sbin"))
            {
                write.WriteLine(cmd_hadoop_sbin_path);
                write.WriteLine(cmd_start_dfs);
             //  System.Threading.Thread.Sleep(12000);

                // Internet Explorer Launch code 
            /*    try
                {


                    InternetExplorer IE = new InternetExplorer();
                    object Empty = 0;
                    object URL = "http://10.100.219.94:50070";
                    IE.Visible = true;
                    IE.Navigate2(ref URL, ref Empty, ref Empty, ref Empty, ref Empty);
                    write.WriteLine(cmd_hadoop_bin_path);
                    write.WriteLine(cmd_mkdir);
                    hadoop_initialize = true;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    Console.ReadKey();
                }
             
             */


            }
            else
            {
                Console.WriteLine("May mali");
                Console.ReadKey();
            }



            // process.WaitForExit();

            /* if (!process.Start())   
             {
                 throw new Exception("Error user input!");
             }
             else{
                 Console.WriteLine("Launch Success!");
                 Console.ReadKey();
             }*/
            process.Close();

        }



        public static ProcessStartInfo initializeCmd()
        {
            ProcessStartInfo processStartInfo = new ProcessStartInfo();
            processStartInfo.FileName = "cmd";
            processStartInfo.Verb = "runas"; // run as administrator
            processStartInfo.LoadUserProfile = true;
            processStartInfo.RedirectStandardInput = true;
           // processStartInfo.RedirectStandardOutput = true;
            processStartInfo.UseShellExecute = false;
            return processStartInfo;


        }

        public void formatNameNode()
        {
            string namenodeFormat = "hdfs namenode -format";
            if (hadoop_initialize == false)
            {
                Console.WriteLine("HAdoop di pa initialize");
                Console.ReadKey();
            }
            else
            {


                Process process = new Process();

                process.StartInfo = initializeCmd();
                process.Start();

                StreamWriter write = process.StandardInput;
                write.WriteLine(cmd_hadoop_bin_path);
                write.WriteLine(namenodeFormat);

                Console.WriteLine("Namenode format success!");

                Console.ReadKey();



            }


        }

        public void formatJournalNodes()
        {
            string journalNodeFormat = "hdfs namenode -initializeSharedEdits -force";
            if (hadoop_initialize == false)
            {
                Console.WriteLine("HAdoop di pa initialize");
                Console.ReadKey();
            }
            else
            {


                Process process = new Process();

                process.StartInfo = initializeCmd();
                process.Start();

                StreamWriter write = process.StandardInput;
                write.WriteLine(cmd_hadoop_bin_path);
                write.WriteLine(journalNodeFormat);

                Console.WriteLine("Journalnode format success!");

                Console.ReadKey();



            }


        }

        public void getLatestCheckpoint()
        {
            string journalNodeFormat = "hdfs namenode -bootstrapStandby -force";
            if (hadoop_initialize == false)
            {
                Console.WriteLine("HAdoop di pa initialize");
                Console.ReadKey();
            }
            else
            {


                Process process = new Process();

                process.StartInfo = initializeCmd();
                process.Start();

                StreamWriter write = process.StandardInput;
                write.WriteLine(cmd_hadoop_bin_path);
                write.WriteLine(journalNodeFormat);

                Console.WriteLine("Successfully copied latest checkpoint success!");

                Console.ReadKey();



            }
        }

        public void list_all_files_from_folder(String hdfs_file_path)
        {
            string output = string.Empty;
            string error = string.Empty;
            string ls = "hdfs dfs -ls " + hdfs_file_path;
            bool file_path_has_space = hdfs_file_path.Contains(" ");

            if ((hadoop_initialize == false && file_path_has_space == true) || hadoop_initialize == true && file_path_has_space == true)
            {
                Console.WriteLine("HAdoop di pa initialize or May ispace");
                Console.ReadKey();
                System.Environment.Exit(0);

            }
            else
            {

                ProcessStartInfo processStartInfo = new ProcessStartInfo("cmd", @"/c cd " + hadoop_bin_path + "&& " + ls);
                processStartInfo.RedirectStandardOutput = true;
                processStartInfo.RedirectStandardError = true;
                processStartInfo.WindowStyle = ProcessWindowStyle.Normal;
                processStartInfo.UseShellExecute = false;

                Process process =  Process.Start(processStartInfo);
 

              
           //     Console.WriteLine("Hadoop naka initialize na at Walang ispace at na kopya na");
             //   Console.ReadKey();
                String s = string.Empty;
                
                //gets the list of files in a directory and adds it in the array
                 List<string> myCollection = new List<string>();


                using (StreamReader streamReader = process.StandardOutput)
                {
                    while (!streamReader.EndOfStream)
                    {
                        s = streamReader.ReadLine();
                        if(!String.IsNullOrEmpty(s.Trim()))
                        {
                      
                            myCollection.Add(s);
              
                        }
                    }
        
                  //  output = streamReader.ReadToEnd();
                }
                // Remove Found x items from the array because it would useless
                myCollection.RemoveAt(0);

                using (StreamReader streamReader = process.StandardError)
                {
                    error = streamReader.ReadToEnd();
                }


                int startIndex = 0;
                int lastIndex =0;
            
                Console.WriteLine("The following output was detected:");
                foreach (string dirEntries in myCollection)
	            {
               //     startIndex = 0;
                dirEntries.Trim();
                   
                /*    for (int i = 0; i < dirEntries.Length; i++)
                    {
                        if (dirEntries[i] == '/' && dirEntries[i+1] == 'u')
                        {
                            startIndex = i;
                        }
                        else if(i+i>dirEntries.Length)
                        {
                            lastIndex=i;
                        }
                    }


                     Console.WriteLine("Index start of / " + startIndex);
                     Console.WriteLine("Index ends in / " + lastIndex);
                     Console.WriteLine("LAst index {0}", dirEntries[lastIndex]);
                     Console.WriteLine("Index  length/ " + dirEntries.Length);
                 * */



                  Console.WriteLine(dirEntries);
	            }
                if (!string.IsNullOrEmpty(error))
                {
                    Console.WriteLine("The following error was detected:");
                    Console.WriteLine(error);
                }

                Console.Read();


            }
        }


        public void file_store_hadoop_to_local(string hdfs_file_path,string local_file_path)
        {
            string copyToLocal = "hdfs dfs -copyToLocal " + hdfs_file_path + " " + local_file_path;

            bool file_path_has_space = hdfs_file_path.Contains(" ");
            bool file_path_has_space2 = local_file_path.Contains(" ");


            if ((hadoop_initialize == false && file_path_has_space == true) || hadoop_initialize == true && file_path_has_space == true)
            {
                Console.WriteLine("HAdoop di pa initialize or May ispace");
                Console.ReadKey();
                System.Environment.Exit(0);
               
            }
            else
            {


                Process process = new Process();
                process.StartInfo = initializeCmd();
                process.Start();

                StreamWriter write = process.StandardInput;
                write.WriteLine(cmd_hadoop_bin_path);
                write.WriteLine(copyToLocal);
                Console.WriteLine("Hadoop naka initialize na at Walang ispace at na kopya na");
                Console.ReadKey();



            }


        }

        public void file_store_local_to_hadoop(string local_file_path)
        {
          //  string hadoop_bin_path = "cd " + hadoop_path + "\\bin";
            string copyFromLocal = "hadoop fs -copyFromLocal " + local_file_path + " /user/" + Environment.UserName + "/Folder";

            bool file_path_has_space = local_file_path.Contains(" ");

            if ((hadoop_initialize == false && file_path_has_space == true) || hadoop_initialize == true && file_path_has_space == true)
            {
                Console.WriteLine("HAdoop di pa initialize or May ispace");
                Console.ReadKey();
            }
            else
            {


                Process process = new Process();

                process.StartInfo = initializeCmd();
                process.Start();

                StreamWriter write = process.StandardInput;
                write.WriteLine(cmd_hadoop_bin_path);
                write.WriteLine(copyFromLocal);

                Console.WriteLine("Hadoop naka initialize na at Walang ispace at na kopya na");

                Console.ReadKey();



            }

        }

       
  

      
        public void delete(string file_name)
        {
           // string hadoop_bin_path = "cd " + hadoop_path + "\\bin";
            string delete = "hdfs dfs -rm /user/" + Environment.UserName + "/Folder/" + file_name;

            bool file_path_has_space = file_name.Contains(" ");

            if ((hadoop_initialize == false && file_path_has_space == true) || hadoop_initialize == true && file_path_has_space == true)
            {
                Console.WriteLine("HAdoop di pa initialize or May ispace");
                Console.ReadKey();
            }
            else
            {


                Process process = new Process();
                process.StartInfo = initializeCmd();
                process.Start();

                StreamWriter write = process.StandardInput;
                write.WriteLine(cmd_hadoop_bin_path);
                write.WriteLine(delete);
                Console.WriteLine("Hadoop naka initialize na at Walang ispace at na delete na");
                Console.ReadKey();



            }


        }

        public void check_namenode_ip()
        {
            using (XmlReader reader = XmlReader.Create(hadoop_path + "\\etc\\hadoop\\core-site.xml"))
            {
                while (reader.Read())
                {
                    if (reader.IsStartElement())
                    {
                        switch (reader.Name)
                        {
                            case "configuration":
                                Console.WriteLine("Start <configuration> element.");
                                break;
                            case "property":
                                Console.WriteLine("Start <property> element.");
                                break;
                            case "name":
                                Console.WriteLine("Start <name> element.");
                                break;
                            case "value":
                                Console.WriteLine("Start <value> element.");

                                if (reader.Read())
                                {
                                    try
                                    {
                                        if (reader.Value.Trim() == "")
                                        {
                                            throw new ArgumentException("\nYou need to specify an IP at the <value> element of core-site.xml.");



                                        }
                                        /*    else
                                            {
                                                Console.WriteLine("  Text node: " + reader.Value.Trim());
                                            
                                            }

                                            */

                                    }
                                    catch (Exception e)
                                    {
                                        Console.WriteLine(e.ToString());
                                        Console.ReadKey();
                                        System.Environment.Exit(0);
                                    }
                                }

                                break;
                        }

                    }
                }

            }

        }



        public void setNameNodeIP(String IPAddress)
        {
            String NameNodeIPAddress = IPAddress;

            write_xml(NameNodeIPAddress);

        }

        public void write_xml(String NameNodeIPAddress)
        {
       /*     using (XmlWriter writer = XmlWriter.Create("C:\\hadoop-2.3.0\\etc\\hadoop\\core-site.xml"))
            {

                writer.WriteStartDocument();
                writer.WriteStartElement("configuration");
                writer.WriteStartElement("property");


                writer.WriteElementString("value", NameNodeIPAddress.Insert(0, "hdfs://"));
                writer.WriteEndElement();
                writer.WriteEndElement();
                writer.WriteEndDocument();
            }
            */
            XmlDocument docz = new XmlDocument();
            docz.Load("C:\\hadoop-2.3.0\\etc\\hadoop\\core-site.xml");
            XmlNode nodez = docz.DocumentElement;

            nodez.SelectSingleNode("//property//value").InnerText= NameNodeIPAddress.Insert(0, "hdfs://");
            Console.ReadKey();
            docz.Save("C:\\hadoop-2.3.0\\etc\\hadoop\\core-site.xml");

    

            XmlDocument doc = new XmlDocument();

            doc.Load("C:\\hadoop-2.3.0\\etc\\hadoop\\hdfs-site.xml");
            XmlNode node = doc.DocumentElement;

            int flag = 0;

            foreach (XmlElement element in node.SelectNodes("//property"))

            //     foreach (XmlNode node1 in element.ChildNodes)
            //foreach (XmlNode node2 in node1.ChildNodes)
            {

                if (element.ChildNodes[0].InnerText.Equals("dfs.http.address"))
                {
                    element.ChildNodes[1].InnerText = NameNodeIPAddress += ":50070";
                    doc.Save("C:\\hadoop-2.3.0\\etc\\hadoop\\hdfs-site.xml");

                }





            }



            //         Console.WriteLine(doc.GetElementsByTagName("property")[0].ChildNodes[0]);


            Console.ReadKey();
            System.Environment.Exit(0);
        }

        public void populate_slaves_file(string[] list_ip){
            string[] ip_list = list_ip;
            foreach(string value in ip_list){
                using (StreamWriter writer = new StreamWriter(hadoop_path+"\\etc\\hadoop\\slaves",true))
                {
                    writer.WriteLine(value);
                }
            }

        }
        public void clear_slaves_file()
        {
            StreamWriter strm = File.CreateText(@hadoop_path + "\\etc\\hadoop\\slaves");
            strm.Flush();
            strm.Close();   
        }
        public void setup_hadoop_configs(String NameNodeIPAddress,String replication_factor)
        {
            string six_indents = "            ";
            string five_indents = "     ";
            XmlWriterSettings settings = new XmlWriterSettings
            {
                Indent = true,
                IndentChars = "   ",
                NewLineChars = "\r\n",
                NewLineHandling = NewLineHandling.Replace
            };
            
            using (XmlWriter writer = XmlWriter.Create(hadoop_path+"\\etc\\hadoop\\core-site.xml",settings))
            {

                writer.WriteStartDocument();
                writer.WriteStartElement("configuration");
                writer.WriteStartElement("property");
                writer.WriteElementString("name", "fs.defaultFS");
                writer.WriteElementString("value", NameNodeIPAddress.Insert(0, "hdfs://"));
                writer.WriteEndElement();
                writer.WriteEndElement();
                writer.WriteEndDocument();
            }
            using (XmlWriter writer = XmlWriter.Create(hadoop_path+"\\etc\\hadoop\\hdfs-site.xml",settings))
            {

                writer.WriteStartDocument();
                writer.WriteStartElement("configuration");
                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.replication");
                writer.WriteElementString("value", replication_factor);
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.namenode.name.dir");
                writer.WriteElementString("value", "file:/hadoop/data/dfs/namenode");
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.datanode.data.dir");
                writer.WriteElementString("value", "file:/hadoop/data/dfs/datanode");
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.permissions");
                writer.WriteElementString("value", "false");
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.http.address");
                writer.WriteElementString("value", NameNodeIPAddress+":50070");
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.webhdfs.enbled");
                writer.WriteElementString("value", "true");
                writer.WriteElementString("description", "to enable webhdfs");
                writer.WriteElementString("final", "true");
                writer.WriteEndElement();



                writer.WriteEndElement();

                writer.WriteEndDocument();
            }
            using (XmlWriter writer = XmlWriter.Create(hadoop_path + "\\etc\\hadoop\\yarn-site.xml", settings))
            {

                writer.WriteStartDocument();
                writer.WriteStartElement("configuration");

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "yarn.nodemanager.aux-services");
                writer.WriteElementString("value", "mapreduce_shuffle");
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "yarn.nodemanager.aux-services.mapreduce.shuffle.class");
                writer.WriteElementString("value", "org.apache.hadoop.mapred.ShuffleHandler");
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "yarn.application.classpath");
                writer.WriteElementString("value", Environment.NewLine + six_indents + "%HADOOP_HOME%\\etc\\hadoop," +
                                                   Environment.NewLine + six_indents + "%HADOOP_HOME%\\share\\hadoop\\common\\*," +
                                                   Environment.NewLine + six_indents + "%HADOOP_HOME%\\share\\hadoop\\common\\lib\\*," +
                                                   Environment.NewLine + six_indents + "%HADOOP_HOME%\\share\\hadoop\\hdfs\\*," +
                                                   Environment.NewLine + six_indents + "%HADOOP_HOME%\\share\\hadoop\\hdfs\\lib\\*," +
                                                   Environment.NewLine + six_indents + "%HADOOP_HOME%\\share\\hadoop\\mapreduce\\*," +
                                                   Environment.NewLine + six_indents + "%HADOOP_HOME%\\share\\hadoop\\mapreduce\\lib\\*," +
                                                   Environment.NewLine + six_indents + "%HADOOP_HOME%\\share\\hadoop\\yarn\\*," +
                                                   Environment.NewLine + six_indents + "%HADOOP_HOME%\\share\\hadoop\\yarn\\lib\\*"+
                                                   Environment.NewLine + five_indents
                                                   ); 

                writer.WriteEndElement();

                writer.WriteEndElement();
                writer.WriteEndDocument();
            }

            Console.WriteLine("Finish!");
            Console.ReadKey();
        }

        public void setup_hadoop_configs_with_ha(String[] NameNodeIPAddress,String[] JournalNodeIPAddress, String replication_factor,String JNEditsDir)
        {
            string six_indents = "            ";
            string five_indents = "     ";

            string NN1=NameNodeIPAddress[0];
            string NN2=NameNodeIPAddress[1];

            string JN1 = JournalNodeIPAddress[0];
            string JN2 = JournalNodeIPAddress[1];
            string JN3 = JournalNodeIPAddress[2];

            string JNEditsDirectory = JNEditsDir;

            XmlWriterSettings settings = new XmlWriterSettings
            {
                Indent = true,
                IndentChars = "   ",
                NewLineChars = "\r\n",
                NewLineHandling = NewLineHandling.Replace
            };

            using (XmlWriter writer = XmlWriter.Create(hadoop_path + "\\etc\\hadoop\\core-site.xml", settings))
            {

                writer.WriteStartDocument();
                writer.WriteStartElement("configuration");
                writer.WriteStartElement("property");
                writer.WriteElementString("name", "fs.defaultFS");
                writer.WriteElementString("value", "hdfs://mycluster");
                writer.WriteEndElement();
                writer.WriteEndElement();
                writer.WriteEndDocument();
            }
            using (XmlWriter writer = XmlWriter.Create(hadoop_path + "\\etc\\hadoop\\hdfs-site.xml", settings))
            {

                writer.WriteStartDocument();
                writer.WriteStartElement("configuration");
                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.replication");
                writer.WriteElementString("value", replication_factor);
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.namenode.name.dir");
                writer.WriteElementString("value", "file:/hadoop/data/dfs/namenode");
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.datanode.data.dir");
                writer.WriteElementString("value", "file:/hadoop/data/dfs/datanode");
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.permissions");
                writer.WriteElementString("value", "false");
                writer.WriteEndElement();


                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.webhdfs.enbled");
                writer.WriteElementString("value", "true");
                writer.WriteElementString("description", "to enable webhdfs");
                writer.WriteElementString("final", "true");
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.nameservices");
                writer.WriteElementString("value", "mycluster");
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.ha.namenodes.mycluster");
                writer.WriteElementString("value", "nn1,nn2");
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.namenode.rpc-address.mycluster.nn1");
                writer.WriteElementString("value",NN1+":8020"   );
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.namenode.rpc-address.mycluster.nn2");
                writer.WriteElementString("value", NN2 + ":8020");
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.namenode.http-address.mycluster.nn1");
                writer.WriteElementString("value", NN1 + ":50070");
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.namenode.http-address.mycluster.nn2");
                writer.WriteElementString("value", NN2 + ":50070");
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.namenode.shared.edits.dir");
                writer.WriteElementString("value", "qjournal://" + JN1 + ":8485;" + JN2 + ":8485;" + JN3 + ":8485/mycluster");
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.client.failover.proxy.provider.mycluster");
                writer.WriteElementString("value", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.ha.fencing.methods");
                writer.WriteElementString("value", "shell(/bin/true)");
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "dfs.journalnode.edits.dir");
                writer.WriteElementString("value", JNEditsDirectory);
                writer.WriteEndElement();




                writer.WriteEndElement();

                writer.WriteEndDocument();
            }
            using (XmlWriter writer = XmlWriter.Create(hadoop_path + "\\etc\\hadoop\\yarn-site.xml", settings))
            {

                writer.WriteStartDocument();
                writer.WriteStartElement("configuration");

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "yarn.nodemanager.aux-services");
                writer.WriteElementString("value", "mapreduce_shuffle");
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "yarn.nodemanager.aux-services.mapreduce.shuffle.class");
                writer.WriteElementString("value", "org.apache.hadoop.mapred.ShuffleHandler");
                writer.WriteEndElement();

                writer.WriteStartElement("property");
                writer.WriteElementString("name", "yarn.application.classpath");
                writer.WriteElementString("value", Environment.NewLine + six_indents + "%HADOOP_HOME%\\etc\\hadoop," +
                                                   Environment.NewLine + six_indents + "%HADOOP_HOME%\\share\\hadoop\\common\\*," +
                                                   Environment.NewLine + six_indents + "%HADOOP_HOME%\\share\\hadoop\\common\\lib\\*," +
                                                   Environment.NewLine + six_indents + "%HADOOP_HOME%\\share\\hadoop\\hdfs\\*," +
                                                   Environment.NewLine + six_indents + "%HADOOP_HOME%\\share\\hadoop\\hdfs\\lib\\*," +
                                                   Environment.NewLine + six_indents + "%HADOOP_HOME%\\share\\hadoop\\mapreduce\\*," +
                                                   Environment.NewLine + six_indents + "%HADOOP_HOME%\\share\\hadoop\\mapreduce\\lib\\*," +
                                                   Environment.NewLine + six_indents + "%HADOOP_HOME%\\share\\hadoop\\yarn\\*," +
                                                   Environment.NewLine + six_indents + "%HADOOP_HOME%\\share\\hadoop\\yarn\\lib\\*" +
                                                   Environment.NewLine + five_indents
                                                   );

                writer.WriteEndElement();

                writer.WriteEndElement();
                writer.WriteEndDocument();
            }

            Console.WriteLine("Finish!");
            Console.ReadKey();
        }

        public void HA_setNNToActive(String NameNodeServiceID)
        {

            // string hadoop_bin_path = "cd " + hadoop_path + "\\bin";
            string transitionToActive = "hdfs haadmin -transitionToActive" + NameNodeServiceID;


            if (hadoop_initialize == false)
            {
                Console.WriteLine("HAdoop di pa initialize");
                Console.ReadKey();
            }
            else
            {


                Process process = new Process();
                process.StartInfo = initializeCmd();
                process.Start();

                StreamWriter write = process.StandardInput;
                write.WriteLine(cmd_hadoop_bin_path);
                write.WriteLine(transitionToActive);
                Console.WriteLine("TransitionToActive Success");
                Console.ReadKey();



            }

        }

        public void HA_setNNToStandby(String NameNodeServiceID)
        {

            // string hadoop_bin_path = "cd " + hadoop_path + "\\bin";
            string transitionToActive = "hdfs haadmin -transitionToStandby" + NameNodeServiceID;


            if (hadoop_initialize == false)
            {
                Console.WriteLine("HAdoop di pa initialize");
                Console.ReadKey();
            }
            else
            {


                Process process = new Process();
                process.StartInfo = initializeCmd();
                process.Start();

                StreamWriter write = process.StandardInput;
                write.WriteLine(cmd_hadoop_bin_path);
                write.WriteLine(transitionToActive);
                Console.WriteLine("TransitionToStandby Success");
                Console.ReadKey();



            }

        }

        public void HA_setGracefulFailover(String NameNodeServiceID1, String NameNodeServiceID2)
        {
            string NameNodeServiceID_1 = NameNodeServiceID1;
            string NameNodeServiceID_2 = NameNodeServiceID2;

            // string hadoop_bin_path = "cd " + hadoop_path + "\\bin";
            string failover = "hdfs haadmin -failover --forcefence --forceactive " + NameNodeServiceID_1 + " " + NameNodeServiceID_2;


            if (hadoop_initialize == false)
            {
                Console.WriteLine("HAdoop di pa initialize");
                Console.ReadKey();
            }
            else
            {


                Process process = new Process();
                process.StartInfo = initializeCmd();
                process.Start();

                StreamWriter write = process.StandardInput;
                write.WriteLine(cmd_hadoop_bin_path);
                write.WriteLine(failover);
                Console.WriteLine("Graceful Failover Success");
                Console.ReadKey();



            }

        }

        public void HA_checkNNState(String NameNodeServiceID)
        {
            string NameNodeServiceID_1 = NameNodeServiceID;

            // string hadoop_bin_path = "cd " + hadoop_path + "\\bin";
            string checkState = "hdfs haadmin -getServiceState " + NameNodeServiceID_1;


            if (hadoop_initialize == false)
            {
                Console.WriteLine("HAdoop di pa initialize");
                Console.ReadKey();
            }
            else
            {


                Process process = new Process();
                process.StartInfo = initializeCmd();
                process.Start();

                StreamWriter write = process.StandardInput;
                write.WriteLine(cmd_hadoop_bin_path);
                write.WriteLine(checkState);
                Console.WriteLine("Check Namenode State Success");
                Console.ReadKey();



            }

        }


    }

}
