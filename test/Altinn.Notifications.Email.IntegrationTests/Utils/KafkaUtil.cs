using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Altinn.Notifications.Email.IntegrationTests.Utils;

public static class KafkaUtil
{
    private const string _brokerAddress = "localhost:9092";

    public static async Task DeleteTopicAsync(string topic)
    {
        using var adminClient = new AdminClientBuilder(new Dictionary<string, string>() { { "bootstrap.servers", _brokerAddress } }).Build();
        await adminClient.DeleteTopicsAsync(new string[] { topic });
    }

    public static async Task CreateTopicsAsync(string topic)
    {
        using var adminClient = new AdminClientBuilder(new Dictionary<string, string>() { { "bootstrap.servers", _brokerAddress } }).Build();

        await adminClient.CreateTopicsAsync(new TopicSpecification[]
        {
            new TopicSpecification
            {
                Name = topic,
                NumPartitions = 1, // Set the desired number of partitions
                ReplicationFactor = 1 // Set the desired replication factor
            }
        });
    }
}
