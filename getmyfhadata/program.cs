using System;
using System.Net;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Web;
using System.Diagnostics;
using System.Data;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using System.Configuration;

namespace getmyfhadata
{
    class Program
    {
        static void Main(string[] args)
        {
            DateTime dt = DateTime.Now;
            StringBuilder rawoutput = new StringBuilder();
            //String rowdata = "";

            rawoutput.AppendLine("option batch abort\n");
            rawoutput.AppendLine("option confirm off\n");
            //for local pc location of key
           // rawoutput.AppendLine("open sftp://www-data@www.myfha.net/ -hostkey=\"ecdsa-sha2-nistp256 256 46:fb:e4:37:f2:4c:a9:f1:32:0e:7f:df:5a:16:ea:62\" -privatekey=\"C:\\users\\mikesweb\\downloads\\newprivate.ppk\" -rawsettings AgentFwd=1 AuthKI=0 KEX=\"rsa, ecdh, dh - gex - sha1, dh - group14 - sha1, WARN, dh - group1 - sha1\"");
            //for server location of key
            rawoutput.AppendLine("open sftp://www-data@www.myfha.net/ -hostkey=\"ecdsa-sha2-nistp256 256 46:fb:e4:37:f2:4c:a9:f1:32:0e:7f:df:5a:16:ea:62\" -privatekey=\"C:\\inetpub\\newprivate.ppk\" -rawsettings AgentFwd=1 AuthKI=0 KEX=\"rsa, ecdh, dh - gex - sha1, dh - group14 - sha1, WARN, dh - group1 - sha1\"");
            rawoutput.AppendLine("lcd c:\\db_files\\"); 
            rawoutput.AppendLine("cd /var/www/exports/\n\r");
            rawoutput.AppendLine("get myfhaleads_" + dt.Month.ToString().PadLeft(2, '0') + "-" + dt.Day.ToString().PadLeft(2, '0') + "-" + dt.Year + ".csv\n\r");
            rawoutput.AppendLine("exit");

            String filename = "C:\\WinSCP\\script-from-myfha.txt";
            try
            {
                StreamWriter ioFile = new StreamWriter(filename.ToString());
                ioFile.Write(rawoutput.ToString());
                ioFile.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine("error is: " + ex.Message + ex.ToString());
            }

            Process process = new Process();
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.CreateNoWindow = true;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.StartInfo.WindowStyle = ProcessWindowStyle.Maximized;
            process.StartInfo.FileName = @"C:\\WinSCP\\WinSCP.com";
            process.StartInfo.Arguments = "/script=\"C:\\WinSCP\\script-from-myfha.txt\"";

            try
            {
                Console.WriteLine("process starting....");
                process.Start();
            }
            catch (Exception ex)
            {
                Console.WriteLine("error is: " + ex.Message + ex.ToString() + " \n" + process.StartInfo.WorkingDirectory + " \n" + process.StartInfo.FileName);
            }
            //Console.WriteLine(process.StandardOutput);
            process.WaitForExit();

            processfile();
            //updatecounts(); //--this is commented out since the stored procedure update_counts was built and scheduled in SQL Server
        }

        static string remquote(string val)
        {
            string val2 = val.Replace("\"", "");
            val2 = val2.Replace("'", "''");
            return val2;
        }

        public static void processfile()
        {

            DateTime dt = DateTime.Now;

            string filename = "C:\\db_files\\myfhaleads_" + dt.Month.ToString().PadLeft(2, '0') + "-" + dt.Day.ToString().PadLeft(2, '0') + "-" + dt.Year + ".csv";
            StreamReader inputFile = new StreamReader(filename.ToString());

            string rawdata = inputFile.ReadToEnd();
            inputFile.Close();

            string id, status, state_id, create_time, ip, hubspot_cookie, hubspot_posted, site, client_id, sold_time, free, cam_step, cam_due;
            string cam_last_email_time, firstname, lastname, street, city, state, zip, loantype, proptype, loanamount, inttype, spechome, propstate, downpayment, value;
            string balance, rate, workphone, homephone, cellphone, email, calltime, credit, notes, srlp, hash, trusted_form_url, grade, expanded_search_credit_repair;
            string householdincome, sr_token;

            SqlConnection putconnect = new SqlConnection();

            putconnect.ConnectionString = ConfigurationManager.ConnectionStrings["sqlconn"].ConnectionString;

            putconnect.Open();

            string[] lines = rawdata.Split(new string[] { ":::" }, StringSplitOptions.None);

            foreach (string line in lines)
            {
                string[] vals = line.SplitQuoted(',', '\"');

                id = remquote(vals[0]);

//                Console.WriteLine("id=" + id);

                if (!string.IsNullOrWhiteSpace(line))
                {
                    status = remquote(vals[1]);
                    state_id = remquote(vals[2]);
                    create_time = remquote(vals[3]);
                    ip = remquote(vals[4]);
                    hubspot_cookie = remquote(vals[5]);
                    hubspot_posted = remquote(vals[6]);
                    site = remquote(vals[7]);
                    client_id = remquote(vals[8]);
                    sold_time = remquote(vals[9]);
                    free = remquote(vals[10]);
                    cam_step = remquote(vals[11]);
                    cam_due = remquote(vals[12]);
                    cam_last_email_time = remquote(vals[13]);
                    firstname = remquote(vals[14]);
                    lastname = remquote(vals[15]);
                    street = remquote(vals[16]);
                    city = remquote(vals[17]);
                    state = remquote(vals[18]);
                    zip = remquote(vals[19]);
                    loantype = remquote(vals[20]);
                    proptype = remquote(vals[21]);
                    loanamount = remquote(vals[22]);
                    inttype = remquote(vals[23]);
                    spechome = remquote(vals[24]);
                    propstate = remquote(vals[25]);
                    downpayment = remquote(vals[26]);
                    value = remquote(vals[27]);
                    balance = remquote(vals[28]);
                    rate = remquote(vals[29]);
                    workphone = remquote(vals[30]);
                    homephone = remquote(vals[31]);
                    cellphone = remquote(vals[32]);
                    email = remquote(vals[33]);
                    calltime = remquote(vals[34]);
                    credit = remquote(vals[35]);
                    notes = remquote(vals[36]);
                    srlp = remquote(vals[37]);
                    hash = remquote(vals[38]);
                    trusted_form_url = remquote(vals[39]);
                    grade = remquote(vals[40]);
                    expanded_search_credit_repair = remquote(vals[41]);
                    householdincome = remquote(vals[42]);
                    sr_token = remquote(vals[43]);

                    var putsql = "INSERT INTO myfhaleads " + "(id, status, state_id, create_time, ip, hubspot_cookie, hubspot_posted, site, client_id, sold_time, free, cam_step, cam_due, ";
                    putsql = putsql + "cam_last_email_time, firstname, lastname, street, city, state, zip, loantype, proptype, loanamount, inttype, spechome, propstate, downpayment, value, ";
                    putsql = putsql + "balance, rate, workphone, homephone, cellphone, email, calltime, credit, notes, srlp, hash, trusted_form_url, grade, expanded_search_credit_repair, ";
                    putsql = putsql + "householdincome, sr_token) ";
                    putsql = putsql + "VALUES ('" + id + "','" + status + "','" + state_id + "','" + create_time + "','" + ip + "','" + hubspot_cookie + "','" + hubspot_posted + "','" + site + "','" + client_id;
                    putsql = putsql + "','" + sold_time + "','" + free + "','" + cam_step + "','" + cam_due + "','" + cam_last_email_time + "','" + firstname + "','" + lastname + "','" + street + "','" + city + "','" + state + "','" + zip;
                    putsql = putsql + "','" + loantype + "','" + proptype + "','" + loanamount + "','" + inttype + "','" + spechome + "','" + propstate + "','" + downpayment + "','" + value + "','" + balance + "','" + rate + "','" + workphone;
                    putsql = putsql + "','" + homephone + "','" + cellphone + "','" + email + "','" + calltime + "','" + credit + "','" + notes + "','" + srlp + "','" + hash + "','" + trusted_form_url + "','" + grade + "','" + expanded_search_credit_repair;
                    putsql = putsql + "','" + householdincome + "','" + sr_token + "') ";

                    SqlCommand putcmd = new SqlCommand(putsql, putconnect);

                    try
                    {
                        if (id != "id")
                        {
                            var udop = putcmd.ExecuteNonQuery();
                            //result = MessageBox.Show(msg1,caption,buttons);
                        }
                        else
                        {
                            //Console.WriteLine("id test failed");
                        }
                    }
                    catch (Exception ex)
                    {
                        //var result = MessageBox.Show(msg2,caption,buttons);
                        Console.WriteLine(ex.Message + "\n\r" + putsql);
                    }
                    finally
                    {
                        //udop = "";
                    }

                }

            }

                putconnect.Close();
            Console.WriteLine("processing completed");
        }

        public static void updatecounts()
        {

            SqlConnection putconnect = new SqlConnection();
            Console.WriteLine("counts started");
            putconnect.ConnectionString =
                "Data Source=ec2-35-163-9-120.us-west-2.compute.amazonaws.com;" +
                "Initial Catalog=myfhabackend;" +
                "User ID=mikesweb;" +
                "Password=Anthony1;" +
                "Timeout = 3000; ";

            putconnect.Open();

            //get the counts and save to databable

            var putsql = "select total.id, total.Client, isnull(ex.cnt,0) as ex, isnull(gd.cnt,0) as gd, isnull(sp.cnt,0) as sp, isnull(mp.cnt,0) as mp, isnull(total.cnt,0) as Total from ";
            putsql = putsql + "(select c.[name] as Client, client_id, count(m.id) as cnt, c.id from myfhaleads m ";
            putsql = putsql + "right join clients c on m.client_id = c.id where c.currentflag = 1 group by c.[name], client_id, c.id) total ";
            putsql = putsql + "left join (select client_id, count(m.id) as cnt from myfhaleads m where credit = 'Excellent' group by client_id) ex ";
            putsql = putsql + "on total.client_id = ex.client_id ";
            putsql = putsql + "left join (select client_id, count(m.id) as cnt from myfhaleads m where credit = 'Good' group by client_id) gd ";
            putsql = putsql + "on total.client_id = gd.client_id ";
            putsql = putsql + "left join (select client_id, count(m.id) as cnt from myfhaleads m where credit = 'Some Credit Problems' group by client_id) sp ";
            putsql = putsql + "on total.client_id = sp.client_id ";
            putsql = putsql + "left join (select client_id, count(m.id) as cnt from myfhaleads m where credit = 'Many Credit Problems' group by client_id) mp ";
            putsql = putsql + "on total.client_id = mp.client_id";

            var tb = new DataTable();

            //Console.WriteLine("sql=" + putsql);

            SqlCommand putcmd = new SqlCommand(putsql, putconnect);

            putcmd.CommandTimeout = 3000;

            using (SqlDataReader reader = putcmd.ExecuteReader())
                {
                tb.Load(reader);
                }

            putsql = "TRUNCATE TABLE lead_counts";
            putcmd = new SqlCommand(putsql, putconnect);
            var udop = putcmd.ExecuteNonQuery();

            foreach (DataRow row in tb.Rows)
            {
                    
                    putsql = "INSERT INTO lead_counts " + "(id, Client, ex, gd, sp, mp, total) ";
                    putsql = putsql + "VALUES (" + row["id"] + ",'" + row["Client"] + "'," + row["ex"] + "," + row["gd"] + "," + row["sp"] + "," + row["mp"];
                    putsql = putsql + "," + row["total"] + ") ";

                    putcmd = new SqlCommand(putsql, putconnect);

                    try
                    {
                            udop = putcmd.ExecuteNonQuery();

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message + "\n\r" + putsql);
                    }
                    finally
                    {
                    }

            }

            putconnect.Close();
            Console.WriteLine("counts completed");
        }
    }
}

public static class StringExtensions
{
    public static string[] SplitQuoted(this string input, char separator, char quotechar)
    {
        List<string> tokens = new List<string>();

        StringBuilder sb = new StringBuilder();
        bool escaped = false;
        foreach (char c in input)
        {
            if (c.Equals(separator) && !escaped)
            {
                // we have a token
                tokens.Add(sb.ToString().Trim());
                sb.Clear();
            }
            else if (c.Equals(separator) && escaped)
            {
                // ignore but add to string
                sb.Append(c);
            }
            else if (c.Equals(quotechar))
            {
                escaped = !escaped;
                sb.Append(c);
            }
            else
            {
                sb.Append(c);
            }
        }
        tokens.Add(sb.ToString().Trim());

        return tokens.ToArray();
    }
}
