using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
namespace HydraAPI
{
    public class Initialize
    {
        public void start_hadoop(string path)
        {
            Process.Start("cmd", @"/c cd " + path + " && start-dfs.cmd");
        }
          
    }
}
