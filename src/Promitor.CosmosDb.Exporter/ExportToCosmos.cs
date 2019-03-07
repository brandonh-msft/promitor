using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace Promitor.CosmosDb.Exporter
{
    public static class ExportToCosmos
    {
        [FunctionName("ExportToCosmos")]
        public static async Task Run(
            [EventHubTrigger("insights-operational-logs", Connection = "AzureMonitorEHConnectionString")] EventData[] events,
            [CosmosDB("azuremonitor", "from-prometheus", ConnectionStringSetting = @"CosmosDbConnectionString", CreateIfNotExists = true)]IAsyncCollector<dynamic> cosmos,
            ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    dynamic messageBody = JObject.Parse(Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count));

                    foreach (var r in (JArray)messageBody.records)
                    {
                        try
                        {
                            await cosmos.AddAsync(r);
                            log.LogInformation($"C# Event Hub trigger function processed a message: {r}");
                        }
                        catch (Exception)
                        {
                        }
                    }


                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
            {
                throw new AggregateException(exceptions);
            }

            if (exceptions.Count == 1)
            {
                throw exceptions.Single();
            }
        }
    }
}
