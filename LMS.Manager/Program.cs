using System;
using System.Net;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using LMS.Core;
using LMS.Models;
using Microsoft.Extensions.Configuration;

namespace LMS.Manager
{
    internal class Program
    {
        private static SchemaRegistryConfig _schemaRegistryConfig;
        private static ConsumerConfig _consumerConfig;
        private static ProducerConfig _producerConfig;

        public static IConfiguration Configuration { get; private set; }

        private static async Task Main(string[] args)
        {
            Console.WriteLine("LMS Manager Terminal\n");

            Configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", true, true)
                .Build();

            _schemaRegistryConfig = Configuration.GetSection(nameof(SchemaRegistryConfig)).Get<SchemaRegistryConfig>();
            _consumerConfig = Configuration.GetSection(nameof(ConsumerConfig)).Get<ConsumerConfig>();
            // Read messages from start if no commit exists.
            _consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
            _producerConfig = Configuration.GetSection(nameof(ProducerConfig)).Get<ProducerConfig>();
            _producerConfig.ClientId = Dns.GetHostName();

            await StartManagerConsumer();
        }


        private static async Task StartManagerConsumer()
        {
            using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
            using var consumer = new ConsumerBuilder<string, LeaveApplication>(_consumerConfig)
                .SetKeyDeserializer(new AvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
                .SetValueDeserializer(new AvroDeserializer<LeaveApplication>(schemaRegistry).AsSyncOverAsync())
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                .Build();
            {
                try
                {
                    Console.WriteLine("");
                    consumer.Subscribe(ApplicationConstants.ManagerApprovalTopicName);
                    while (true)
                    {
                        Console.WriteLine("Consumer Started...");
                        var result = consumer.Consume();
                        var leaveRequest = result.Message.Value;
                        Console.WriteLine(
                            $"Received message: {result.Message.Key} from partition: {result.Partition.Value} Value: {JsonSerializer.Serialize(leaveRequest)}");

                        // Make decision on leave request.
                        var isApproved = ReadLine.Read("Approve request? (Y/N): ", "Y")
                            .Equals("Y", StringComparison.OrdinalIgnoreCase);
                        await SendMessageToResultTopicAsync(leaveRequest, isApproved, result.Partition.Value);
                        consumer.Commit(result);
                        consumer.StoreOffset(result);
                        Console.WriteLine("\nOffset committed");
                        Console.WriteLine("----------\n\n");
                    }
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Consume error: {e.Error.Reason}");
                }
                finally
                {
                    consumer.Close();
                }
            }
        }

        private static async Task SendMessageToResultTopicAsync(LeaveApplication leaveRequest, bool isApproved,
            int partitionId
        )
        {
            using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
            using var producer = new ProducerBuilder<string, LeaveApplicationResult>(_producerConfig)
                .SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
                .SetValueSerializer(new AvroSerializer<LeaveApplicationResult>(schemaRegistry))
                .Build();
            {
                var leaveApplicationResult = new LeaveApplicationResult
                {
                    EmpDepartment = leaveRequest.EmpDepartment,
                    EmpEmail = leaveRequest.EmpEmail,
                    LeaveDurationInHours = leaveRequest.LeaveDurationInHours,
                    LeaveStartDateTicks = leaveRequest.LeaveStartDateTicks,
                    ProcessedBy = $"Manager #{partitionId}",
                    Result = isApproved
                        ? "Approved: Your leave application has been approved."
                        : "Declined: Your leave application has been declined."
                };

                var result = await producer.ProduceAsync(ApplicationConstants.LeaveApplicationResultsTopicName,
                    new Message<string, LeaveApplicationResult>
                    {
                        Key = $"{leaveRequest.EmpEmail}-{DateTime.UtcNow.Ticks}",
                        Value = leaveApplicationResult
                    });
                Console.WriteLine(
                    $"\nMsg: Leave request processed and queued at offset {result.Offset.Value} in the Topic {result.Topic}");
            }
        }
    }
}