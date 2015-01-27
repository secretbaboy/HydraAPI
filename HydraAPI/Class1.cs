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
namespace HydraAPI
{
    public class Initialize
    {
        bool hadoop_initialize = false;
        String hadoop_path;

        public void start_hadoop(string path)
        {
            //  ProcessStartInfo processStartInfo = new ProcessStartInfo("cmd", @"/c cd " +path+"&& start-dfs.cmd");
            this.hadoop_path = path;
            string hadoop_sbin_path = "cd " + hadoop_path + "\\sbin";
            string hadoop_bin_path = "cd " + hadoop_path + "\\bin";
            string start_dfs = "start-dfs.cmd";
            string mkdir = "hadoop fs -mkdir -p " + "/user/" + Environment.UserName + "/Folder";



            //  processStartInfo.CreateNoWindow = false;  // remove the command line where you type start-dfs initially
            check_namenode_ip(hadoop_path);

            Process process = new Process();
            process.StartInfo = initializeCmd();
            process.Start();


            StreamWriter write = process.StandardInput;

            if (hadoop_sbin_path.EndsWith("sbin"))
            {
                write.WriteLine(hadoop_sbin_path);
                write.WriteLine(start_dfs);
                System.Threading.Thread.Sleep(9000);
                try
                {


                    InternetExplorer IE = new InternetExplorer();
                    object Empty = 0;
                    object URL = "http://10.100.219.94:50070";
                    IE.Visible = true;
                    IE.Navigate2(ref URL, ref Empty, ref Empty, ref Empty, ref Empty);
                    write.WriteLine(hadoop_bin_path);
                    write.WriteLine(mkdir);
                    hadoop_initialize = true;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    Console.ReadKey();
                }


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

        }

        public static ProcessStartInfo initializeCmd()
        {
            ProcessStartInfo processStartInfo = new ProcessStartInfo();
            processStartInfo.FileName = "cmd";
            processStartInfo.Verb = "runas"; // run as administrator
            processStartInfo.LoadUserProfile = true;
            processStartInfo.RedirectStandardInput = true;
            processStartInfo.UseShellExecute = false;
            return processStartInfo;


        }


        public void file_store(string file_path)
        {
            string hadoop_bin_path = "cd " + hadoop_path + "\\bin";
            string copyFromLocal = "hadoop fs -copyFromLocal " + file_path + " /user/" + Environment.UserName + "/Folder";

            bool file_path_has_space = file_path.Contains(" ");

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
                write.WriteLine(hadoop_bin_path);
                write.WriteLine(copyFromLocal);
                Console.WriteLine("Hadoop naka initialize na at Walang ispace at na kopya na");
                Console.ReadKey();



            }

        }

        public void delete(string file_name)
        {
            string hadoop_bin_path = "cd " + hadoop_path + "\\bin";
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
                write.WriteLine(hadoop_bin_path);
                write.WriteLine(delete);
                Console.WriteLine("Hadoop naka initialize na at Walang ispace at na delete na");
                Console.ReadKey();



            }


        }

        public void check_namenode_ip(string hadoop_path)
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









    }

}
