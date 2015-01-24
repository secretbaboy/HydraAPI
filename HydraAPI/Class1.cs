using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using SHDocVw;
using System.Timers;
namespace HydraAPI
{
    public class Initialize :Exception
    {

        public void start_hadoop(string path)
        {
              //  ProcessStartInfo processStartInfo = new ProcessStartInfo("cmd", @"/c cd " +path+"&& start-dfs.cmd");
                ProcessStartInfo processStartInfo = new ProcessStartInfo();    
            string hadoop_sbin_path = "cd "+path+"\\sbin";
            string hadoop_bin_path = "cd " + path + "\\bin";
            string start_dfs = "start-dfs.cmd";
            string mkdir = "hadoop fs -mkdir -p " + "/user/" + Environment.UserName + "/Folder";

     

          
                
                processStartInfo.FileName = "cmd";
                processStartInfo.Verb = "runas"; // run as administrator
                processStartInfo.LoadUserProfile = true;
                processStartInfo.RedirectStandardInput = true;
                processStartInfo.UseShellExecute = false;
              //  processStartInfo.CreateNoWindow = false;  // remove the command line where you type start-dfs initially
                

                Process process = new Process();
                process.StartInfo = processStartInfo;
                process.Start();

           
                StreamWriter write = process.StandardInput;

                if (hadoop_sbin_path.EndsWith("sbin"))
                {
                    write.WriteLine(hadoop_sbin_path);
                        write.WriteLine(start_dfs);

                        System.Threading.Thread.Sleep(5000);
                        InternetExplorer IE = new InternetExplorer();
                        object Empty = 0;
                        object URL = "http://localhost:50070";
                        IE.Visible = true;
                        IE.Navigate2(ref URL, ref Empty, ref Empty, ref Empty, ref Empty);

                        
                       write.WriteLine(hadoop_bin_path);
                        write.WriteLine(mkdir);
                        process.WaitForExit();
                     
                        

                    //    System.Threading.Thread.Sleep(5000);

                      //  IE.Quit();
                       


                       
        
                   

                }
                else{
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

        public void openBrowser(object source, ElapsedEventArgs e)
        {
            InternetExplorer IE = new InternetExplorer();
            object Empty = 0;
            object URL = "www.live.com";
            IE.Visible = true;
            IE.Navigate2(ref URL, ref Empty, ref Empty, ref Empty, ref Empty);

            System.Threading.Thread.Sleep(5000);

            IE.Quit();

        }

       

        
        }
    
}
