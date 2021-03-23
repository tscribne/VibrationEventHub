using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data;
using System.Data.SqlClient;

using System.Text.Json;
using System.Text.Json.Serialization;

using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;


namespace Company.Function
{


    // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse); 
    public class Telemetry
    {
        public double azureconnecttime { get; set; }
        public double accelerationX { get; set; }
        public double accelerationY { get; set; }
        public double accelerationZ { get; set; }
        public double temperature { get; set; }
        public double humidity { get; set; }
        public int rssi { get; set; }
        public double wififrequency_tel { get; set; }
        public double wificonnecttime { get; set; }
        public int wififails { get; set; }
        public int wifiresets { get; set; }
        public int azurefails { get; set; }
        public int fatals { get; set; }
        public double vibration { get; set; }

    }

    public class MessageProperties
    {
        public string type { get; set; }
    }

    public class Enrichments
    {
    }

    public class TelemetryRoot
    {
        public string applicationId { get; set; }
        public string messageSource { get; set; }
        public string deviceId { get; set; }
        public string schema { get; set; }
        public string templateId { get; set; }
        public DateTime enqueuedTime { get; set; }
        public Telemetry telemetry { get; set; }
        public MessageProperties messageProperties { get; set; }
        public Enrichments enrichments { get; set; }
    }


    // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse); 
    public class Property
    {
        public string name { get; set; }
        public object value { get; set; }
    }


    public class PropertyRoot
    {
        public string applicationId { get; set; }
        public string messageSource { get; set; }
        public string messageType { get; set; }
        public string deviceId { get; set; }
        public string schema { get; set; }
        public string templateId { get; set; }
        public DateTime enqueuedTime { get; set; }
        public List<Property> properties { get; set; }
        public Enrichments enrichments { get; set; }
    }


    public static class VibrationEventHubTrigger
    {
        [FunctionName("VibrationEventHubTrigger")]
        public static async Task Run([
            EventHubTrigger("vibrationeventhub",
            Connection = "VibrationEvent_RootManageSharedAccessKey_EVENTHUB")]
            EventData[] events, ILogger log)
        {
            string connectionString = "Server=tcp:vibrationpoc.database.windows.net,1433;Initial Catalog=VibrationPOCServer;Persist Security Info=False;User ID=user;Password=Pass@word1;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
            SqlConnection connection;

            log.LogInformation("--> VibratinEventHubTrigger");


            try
            {
                connection = new SqlConnection(connectionString);

                //log.LogInformation("SQL CONNECTED!!!!");
            }

            catch (System.Exception)
            {
                log.LogError("Unable to connect to SQL database");
                throw;
            }

            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {

                try
                {


                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);


                    //log.LogInformation( "=====> [EVENTS]");
                    //log.LogInformation( messageBody );


                    var options = new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true,
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                    };

                    //Console.WriteLine("================================================================");

                    var jsonData = JsonSerializer.Deserialize<TelemetryRoot>(messageBody, options);


                    if (jsonData.messageSource.Equals("telemetry"))
                    {
                        log.LogInformation("TELEMETRY DATA");


                        var vibration = jsonData;

                        /*
                        Console.WriteLine($"MessageBody [{messageBody}]");

                        
                        log.LogInformation($"ApplicationID        {vibration.applicationId}");
                        log.LogInformation($"Message Source       {vibration.messageSource}");
                        log.LogInformation($"DeviceID             {vibration.deviceId}");
                        log.LogInformation($"Schema               {vibration.schema}");
                        log.LogInformation($"TemplateId           {vibration.templateId}");
                        log.LogInformation($"EnqueueTime          {vibration.enqueuedTime}");
                        log.LogInformation($"AZ Connection Time   {vibration.telemetry.azureconnecttime}");
                        log.LogInformation($"RSSI                 {vibration.telemetry.rssi}");
                        log.LogInformation($"Vibration            {vibration.telemetry.vibration}");
                        log.LogInformation($"Acceleration X       {vibration.telemetry.accelerationX}");
                        log.LogInformation($"Acceleration Y       {vibration.telemetry.accelerationY}");
                        log.LogInformation($"Acceleration Z       {vibration.telemetry.accelerationZ}");
                        log.LogInformation($"Temperature          {vibration.telemetry.temperature}");
                        log.LogInformation($"Humidity             {vibration.telemetry.humidity}");
                        log.LogInformation($"WIFI Freq            {vibration.telemetry.wififrequency_tel}");
                        log.LogInformation($"WIFI Connect Time    {vibration.telemetry.wificonnecttime}");
                        log.LogInformation($"WIFI Fails           {vibration.telemetry.wififails}");
                        log.LogInformation($"WIFI Resets          {vibration.telemetry.wifiresets}");
                        log.LogInformation($"Azure Fails          {vibration.telemetry.azurefails}");
                        log.LogInformation($"Fatals               {vibration.telemetry.fatals}");
                        log.LogInformation($"Msg Type             {vibration.messageProperties.type}");
                        */

                        // Insert into SQL table

                        string values = $"( '{vibration.applicationId}'," +
                            $"'{vibration.messageSource}'," +
                            $"'{vibration.deviceId}'," +
                            $"'{vibration.schema}'," +
                            $"'{vibration.templateId}'," +
                            $"'{vibration.enqueuedTime}'," +
                            $"'{vibration.telemetry.azureconnecttime}'," +
                            $"'{vibration.telemetry.rssi}'," +
                            $"'{vibration.telemetry.vibration}'," +
                            $"'{vibration.telemetry.wificonnecttime}'," +
                            $"'{vibration.telemetry.wififrequency_tel}'," +
                            $"'{vibration.telemetry.accelerationX}'," +
                            $"'{vibration.telemetry.accelerationY}'," +
                            $"'{vibration.telemetry.accelerationZ}'," +
                            $"'{vibration.telemetry.temperature}'," +
                            $"'{vibration.telemetry.humidity}'," +
                            $"'{vibration.telemetry.wififails}'," +
                            $"'{vibration.telemetry.wifiresets}'," +
                            $"'{vibration.telemetry.azurefails}'," +
                            $"'{vibration.telemetry.fatals}'" +
                            $")";


                        string query = "INSERT INTO dbo.Vibration " + " VALUES" + values;
                        SqlCommand cmd = new SqlCommand(query, connection);

                        try
                        {
                            connection.Open();
                            cmd.ExecuteNonQuery();
                            log.LogInformation("Telemetry Data Inserted Successfully!!!");
                        }
                        catch (SqlException e)
                        {
                            log.LogError("Error!!! Details: " + e.ToString());
                        }
                        finally
                        {
                            connection.Close();
                        }

                    }



                    if (jsonData.messageSource.Equals("properties"))
                    {
                        log.LogInformation("PROPERTIES DATA");


                        var prop = JsonSerializer.Deserialize<PropertyRoot>(messageBody, options);

                        //Console.WriteLine($"*** Properties *** MessageBody [{messageBody}]");

                        /*                
                        log.LogInformation($"ApplicationID        {prop.applicationId}");
                        log.LogInformation($"Message Source       {prop.messageSource}");
                        log.LogInformation($"Message Type         {prop.messageType}");
                        log.LogInformation($"DeviceID             {prop.deviceId}");
                        log.LogInformation($"Schema               {prop.schema}");
                        log.LogInformation($"TemplateID           {prop.templateId}");
                        log.LogInformation($"EnqueuedTime         {prop.enqueuedTime}");
                        */


                        /*                        
                        foreach (var i in prop.properties)
                        {
                            // In order of sleeptime, manufacturer, model, builddate, fwversion, ssid, wifi freq
                            log.LogInformation(i.name + " " + i.value);
                        }
                        */


                        
                        string values = $"('{prop.applicationId}','{prop.messageSource}','{prop.messageType}','{prop.deviceId}','{prop.schema}','{prop.templateId}','{prop.enqueuedTime}',";

                        //
                        // This is all added due to the two different properties code that
                        // Brady put it, some of the devices have the old code with does not
                        // include the two conntime variables.  We need to add two additional
                        // columns so that SQL will be happy when we add the row.  We can remove
                        // all of the if String.Compare's once all the devices are updated.
                        //

                        Property last = prop.properties.Last();
                        //log.LogInformation($"Last is '{last.name}'");

                        foreach (var i in prop.properties)
                        {
                            String s1 = $"'{last.name}'";
                            String s2 = $"'{i.name}'";

                            //log.LogInformation($"Comparing " + s1 + "==" + s2);

                            if( String.Compare(s1,s2) == 0 )
                            {
                                //log.LogInformation($"Ending '{i.name}'");
                                values += $"'{i.value}'";

                                //log.LogInformation($"Comparing {i.name} to wififrequency ");
                                
                                if( String.Compare($"{i.name}",$"wififrequency") == 0)
                                {
                                    //log.LogInformation("OLD OUTPUT");
                                    values += $",'0','0')";
                                }
                                else
                                {
                                    values += $")";
                                }
                            }
                            else
                            {
                                //log.LogInformation($"Adding '{i.name}'");
                                values += $"'{i.value}',";
                            }
                        }


                        values += $";";

                        //log.LogInformation($"***VALUES*** " + values);

                        string query = "INSERT INTO dbo.Properties " + "VALUES" + values;
                        SqlCommand cmd2 = new SqlCommand(query, connection);
                        try
                        {
                            connection.Open();
                            cmd2.ExecuteNonQuery();
                            log.LogInformation("Properties Data Inserted Successfully!!!");
                        }
                        catch (SqlException e)
                        {
                            log.LogError("Error!!! Details: " + e.ToString());
                        }
                        finally
                        {
                            connection.Close();
                        }

                       

                    }



                    await Task.Yield();

                }

                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);

                    log.LogError(e.ToString());
                }


            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }

    }


}

