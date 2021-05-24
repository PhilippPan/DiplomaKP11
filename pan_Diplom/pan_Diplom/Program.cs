using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using System.IO;

namespace pan_Diplom
{
    class Program
    {
        private static string Host = "localhost";
        private static string User = "postgres";
        private static string DBname = "pan_Diploma";
        private static string Password = "123";
        private static string Port = "5432";

        static void Main(string[] args)
        {
            // Build connection string using parameters from portal
            string connString =
                String.Format(
                    "Server={0};Username={1};Database={2};Port={3};Password={4};SSLMode=Prefer",
                    Host,
                    User,
                    DBname,
                    Port,
                    Password);


            using (var conn = new NpgsqlConnection(connString))

            {
                Console.Out.WriteLine("Opening connection");
                conn.Open();
                int onoff = 0;
                Console.WriteLine("\nDo you want to add query files?   y/n");
                switch (Console.ReadLine())
                {
                    case "y":
                        onoff = 1;
                        break;
                    case "n":
                        onoff = 0;
                        break;
                }
                while (onoff == 1)
                {
                    Console.WriteLine("------------ Drop scripts for tables here");

                    // получаем файл и все его строки записываем в массив
                    string queryPath = Console.ReadLine();
                    FileInfo fileInf = new FileInfo(queryPath);
                    String[] queryArray = File.ReadAllLines(queryPath);

                    // определяем первую часть запроса, в котором создается таблица. Ищем номер строки, на которой этот запрос заканчивается по символу ;
                    string checkchar = ";";
                    int Arlength = queryArray.Length;
                    int RowN = 0; // номер сроки, в которой находится нужный символ
                    for (int i = 0; i < Arlength; i++)
                    {
                        if (queryArray[i].Contains(checkchar))
                        {
                            RowN = i;
                            break;
                        }
                    }
                    // из всех строк до этого символа формируем одну строку запроса
                    String queryString = "";
                    for (int i = 0; i <= RowN; i++)
                    {
                        queryString += queryArray[i];
                    }
                    // отправляем в БД бполученную строку с запросом на создание таблицы
                    using (var command = new NpgsqlCommand(queryString, conn))
                    {
                        command.ExecuteNonQuery();
                        Console.Out.WriteLine("Finished creating table");
                        queryString = "";
                    }

                    // потом отправляем по одной все остальные строки с добавлением данных в таблицу
                    int AddedRowsN = 0;
                    Console.Out.WriteLine("Adding Data. Please wait...");
                    for (int i = RowN + 1; i > RowN; i++)
                    {
                        if (i < Arlength)
                        {
                            queryString += queryArray[i];
                            using (var command2 = new NpgsqlCommand(queryString, conn))
                            {
                                command2.ExecuteNonQuery();
                                AddedRowsN++;
                                Console.Write("\r" + AddedRowsN);
                            }
                        }
                    }
                    Console.WriteLine(AddedRowsN + " rows were added");
                    // даем возможность пользователю добавить еще файлов
                    Console.WriteLine("\nDo you want to add more files?   y/n");
                    switch (Console.ReadLine())
                    {
                        case "y":
                            onoff = 1;
                            break;
                        case "n":
                            onoff = 0;
                            break;
                    }

                    
                }
                /////////////////////////////////////////////////////


                Console.WriteLine("\n*** Press RETURN to start creating table ***");
                Console.ReadLine();

                // Создание таблицы activities
                string createActQuery = "CREATE TABLE IF NOT EXISTS activities (case_id text, timestamp timestamp with time zone, status text);";
                using (var command = new NpgsqlCommand(createActQuery, conn))
                {
                    command.ExecuteNonQuery();
                    Console.Out.WriteLine("Finishing creating table activities");
                }

                // создание таблицы cases
                string caseQuery = "CREATE TABLE IF NOT EXISTS cases AS SELECT z.po_number ||'_'|| z.sku AS case_id, z.product, x.d_type, z.supplier, z.jira_request, z.required_by, z.received_date, z.m_lead_time_in_weeks, z.m_lead_time_in_days, z.item_type, z.next_delivery_date, x.d_created, x.updated, x.location, x.total_amount, x.tax_amount FROM airtable_po_gr_value z  JOIN dear_po x ON z.po_number = x.order_number";
                using (var command = new NpgsqlCommand(caseQuery, conn))
                {
                    command.ExecuteNonQuery();
                    Console.Out.WriteLine("Finishing creating table cases");
                }

                // Обозначение нужных переменных
                string getPOnumber = "SELECT po_number, sku FROM airtable_po_gr_value";
                List<String> POnumber = new List<string>();
                List<String> Itemnumber = new List<string>();
                string IssueKey = "";
                String[,] JiraResArray = new string[15, 4];


                // Получения списка всех PO в лист
                NpgsqlCommand GetPOQ = new NpgsqlCommand(getPOnumber, conn);
                NpgsqlDataReader Dreader = GetPOQ.ExecuteReader();
                var o = 0;
                for(int i=0; i < 90000; i++)
                {
                    try
                    {
                        Dreader.Read();
                        POnumber.Add(Dreader.GetString(0));
                        Itemnumber.Add(Dreader.GetString(1));
                    }
                    catch { break; }
                }
                Dreader.Close();

                
                // В этом цикле мы проверяем каждый PO_number из списка
                for (int i = 0; i < POnumber.Count; i++) //POnumber.Count
                {
                    // Получение ключа запроса в таблице jira для текущего РО
                    string getIssueKey = "SELECT jira_request FROM airtable_po_gr_value WHERE po_number = '" + POnumber[i] + "' limit 1";
                    NpgsqlCommand GetIssueQ = new NpgsqlCommand(getIssueKey, conn);
                    NpgsqlDataReader Dreader2 = GetIssueQ.ExecuteReader();
                    Dreader2.Read();
                    try
                    {
                        IssueKey = Dreader2.GetString(0);
                    }
                    catch { Dreader2.Close(); continue; }
                    Console.WriteLine(IssueKey);
                    Dreader2.Close();

                    // Получение данных по текущему РО
                    string activityQuery = "SELECT cl_created_from, cl_created, cl_status_from, cl_status_to FROM jira_purchases_changelog WHERE issue_key = '" + IssueKey + "'";
                    NpgsqlCommand ActivityQ = new NpgsqlCommand(activityQuery, conn);
                    NpgsqlDataReader Dreader3 = ActivityQ.ExecuteReader();

                    // В этом цикле происходит перезапись всех полученных данных в массив для дальнейшей работы
                    for (int ii = 0; ii < 11; ii++)
                    {
                        Dreader3.Read();
                        try
                        {
                            for (int j = 0; j <= 3; j++)
                            {
                                try
                                {
                                    JiraResArray[ii, j] = Dreader3.GetString(j);
                                }
                                catch { JiraResArray[ii, j] = Dreader3.GetDateTime(j).ToString(); }
                            }
                        }
                        catch { continue; }
                    }

                    // Отображение данных из массива
                    if (JiraResArray[0, 0] != null)
                    {

                        /*for (int aa = 0; aa < 11; aa++)
                        {
                            for (int jj = 0; jj <= 3; jj++)
                            {
                                Console.Write(String.Format("{0,5} | ", JiraResArray[aa, jj]));
                            }
                            Console.WriteLine();
                        }*/

                        Dreader3.Close();
                        //Console.ReadLine();
                    }
                    else
                    {
                        Dreader3.Close();
                        //Console.ReadLine();
                        continue;
                    }

                    // Получение значений для записи в таблицу
                    String status = "", timestamp = "";
                    try
                    {
                        for (int z = 0; z <= 15; z++)
                        {
                            for (int x = 0; x < 2; x++)
                            {
                                switch (x)
                                {
                                    case 0:
                                        try
                                        {
                                            if ((JiraResArray[z, 0] == JiraResArray[z - 1, 1]) && (JiraResArray[z, 2] == JiraResArray[z - 1, 3]))
                                            {
                                                timestamp = JiraResArray[z, 1].ToString();
                                                status = JiraResArray[z, 3].ToString();
                                                x = 2; 
                                            }
                                            
                                        } 
                                        catch
                                        {
                                            timestamp = JiraResArray[z, 0].ToString();
                                            status = JiraResArray[z, 2].ToString();
                                        }
                                        break;
                                    case 1:
                                        try
                                        {
                                            timestamp = JiraResArray[z, 1].ToString();
                                            status = JiraResArray[z, 3].ToString();
                                        }
                                        catch { z = 16; }
                                        break;
                                }
                                Console.WriteLine("STATUS   " + status);
                                Console.WriteLine("TIME   " + timestamp);


                                string checkQuery = "SELECT * FROM public.activities WHERE case_id = '" + POnumber[i] + "_" + Itemnumber[i] + "' AND timestamp = '" + timestamp + "' AND status = '" + status + "'";
                                NpgsqlCommand CheckQ = new NpgsqlCommand(checkQuery, conn);
                                NpgsqlDataReader CHreader = CheckQ.ExecuteReader();
                                //CHreader.Read();
                                if (!CHreader.Read())
                                {
                                    CHreader.Close();
                                    string rowsActivityQuery = "INSERT INTO public.activities (case_id, timestamp, status) VALUES ('" + POnumber[i] + "_" + Itemnumber[i] + "', '" + timestamp + "', '" + status + "');";
                                    using (var command = new NpgsqlCommand(rowsActivityQuery, conn))
                                    {
                                        command.ExecuteNonQuery();
                                        Console.Out.WriteLine("Row added");
                                    }
                                }
                                else { z = 16; CHreader.Close(); break; }

                                //Console.ReadKey();
                            }
                        }
                    } catch { Console.WriteLine("error"); }

                } Console.WriteLine("All rows were successfully added to activities!");

            }
                Console.WriteLine("\n*** Press RETURN to exit ***");
            Console.ReadLine();
        }

        
    }
}
