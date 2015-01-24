using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
namespace HydraAPI
{
    public class Initialize :Exception
    {
        public string Path
        {
            get;
            set;
        }
        public void start_hadoop(string path)
        {
              //  ProcessStartInfo processStartInfo = new ProcessStartInfo("cmd", @"/c cd " +path+"&& start-dfs.cmd");
                ProcessStartInfo processStartInfo = new ProcessStartInfo();    
            string firstcom = "cd "+path;
            string secondcom = "start-dfs.cmd";

                processStartInfo.FileName = "cmd";
                processStartInfo.Verb = "runas";
                processStartInfo.RedirectStandardInput = true;
                processStartInfo.UseShellExecute = false;
 //               string args=@"/c cd "+ path + "&& start-dfs.cmd";
                Process process = new Process();
                process.StartInfo = processStartInfo;
                process.Start();

                StreamWriter write = process.StandardInput;
                
                if(firstcom.EndsWith("sbin")){
                write.WriteLine(firstcom);
                        write.WriteLine(secondcom);
                process.WaitForExit();

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

        
        }
    
}
