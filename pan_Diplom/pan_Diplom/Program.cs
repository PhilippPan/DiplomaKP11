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
            //
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
                int onoff = 1;
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
            }            

            Console.WriteLine("\n*** Press RETURN to exit ***");
            Console.ReadLine();
        } 
    }
}
