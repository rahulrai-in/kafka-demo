using System;
using System.Collections.Generic;
using System.Linq;
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

namespace LMS.AutoValidator
{
    internal class Program
    {
        private static SchemaRegistryConfig _schemaRegistryConfig;
        private static ConsumerConfig _consumerConfig;
        private static AdminClientConfig _adminConfig;
        private static ProducerConfig _producerConfig;

        public static IConfiguration Configuration { get; private set; }

        private static async Task Main(string[] args)
        {
            Console.WriteLine("LMS Auto Validator\n");

            Configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", true, true)
                .Build();

            _schemaRegistryConfig = Configuration.GetSection(nameof(SchemaRegistryConfig)).Get<SchemaRegistryConfig>();
            _consumerConfig = Configuration.GetSection(nameof(ConsumerConfig)).Get<ConsumerConfig>();
            // Read messages from start if no commit exists.
            _consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
            _adminConfig = Configuration.GetSection(nameof(AdminClientConfig)).Get<AdminClientConfig>();
            _producerConfig = Configuration.GetSection(nameof(ProducerConfig)).Get<ProducerConfig>();
            _producerConfig.ClientId = Dns.GetHostName();

            await KafkaHelper.CreateTopicAsync(_adminConfig, ApplicationConstants.ManagerApprovalTopicName, 3);
            await KafkaHelper.CreateTopicAsync(_adminConfig, ApplicationConstants.LeaveApplicationResultsTopicName, 1);
            await ProcessLeaveApplicationsAsync();
        }

        private static async Task ProcessLeaveApplicationsAsync()
        {
            var availableLeaveHours = Configuration.GetSection("AvailableLeaveHours").Get<Dictionary<string, int>>();
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
                    consumer.Subscribe(ApplicationConstants.LeaveApplicationsTopicName);
                    while (true)
                    {
                        var result = consumer.Consume();
                        var leaveRequest = result.Message.Value;
                        Console.WriteLine(
                            $"Received message: {result.Message.Key} Value: {JsonSerializer.Serialize(leaveRequest)}");

                        // Make decision on leave request.
                        var isEligibleRequest = availableLeaveHours.Any(e =>
                            e.Key.Equals(leaveRequest.EmpEmail) && e.Value >= leaveRequest.LeaveDurationInHours);
                        if (isEligibleRequest)
                        {
                            await SendMessageToManagerTopicAsync(leaveRequest);
                        }
                        else
                        {
                            await SendMessageToResultTopicAsync(leaveRequest);
                        }

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

        private static async Task SendMessageToResultTopicAsync(LeaveApplication leaveRequest)
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
                    ProcessedBy = "LMS Bot",
                    Result = "Declined: You do not have sufficient leave balance."
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

        private static async Task SendMessageToManagerTopicAsync(LeaveApplication leaveRequest)
        {
            using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
            using var producer = new ProducerBuilder<string, LeaveApplication>(_producerConfig)
                .SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
                .SetValueSerializer(new AvroSerializer<LeaveApplication>(schemaRegistry))
                .Build();
            {
                var partition = new TopicPartition(ApplicationConstants.ManagerApprovalTopicName,
                    new Partition((int) Enum.Parse<Departments>(leaveRequest.EmpDepartment)));
                var result = await producer.ProduceAsync(partition,
                    new Message<string, LeaveApplication>
                    {
                        Key = $"{leaveRequest.EmpEmail}-{DateTime.UtcNow.Ticks}",
                        Value = leaveRequest
                    });
                Console.WriteLine(
                    $"\nMsg: Leave request sent to manager and queued at offset {result.Offset.Value} in the Topic {result.Topic}:{result.Partition.Value}\n\n");
            }
        }
    }
}