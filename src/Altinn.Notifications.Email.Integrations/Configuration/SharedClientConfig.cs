using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Altinn.Notifications.Email.Integrations.Configuration;

/// <summary>
/// Container class for configuration objects used by producers and consumers. Will also help with
/// initialization of some default settings common across all Kafka clients.
/// </summary>
public class SharedClientConfig
{
    /// <summary>
    /// Admin client configuration to use for kafka admin
    /// </summary>
    public AdminClientConfig AdminClientConfig { get; }

    /// <summary>
    /// Generic client configuration to use for kafka producer and consumer 
    /// </summary>
    public ClientConfig ClientConfig { get; }

    /// <summary>
    /// TopicSpecification
    /// </summary>
    public TopicSpecification TopicSpecification { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="SharedClientConfig"/> class.
    /// </summary>
    public SharedClientConfig(KafkaSettings kafkaSettings)
    {
        var adminConfig = new AdminClientConfig()
        {
            BootstrapServers = kafkaSettings.BrokerAddress,
        };

        var config = new ClientConfig
        {
            BootstrapServers = kafkaSettings.BrokerAddress,
        };

        var topicSpec = new TopicSpecification()
        {
            NumPartitions = 1,
            ReplicationFactor = 1
        };

        bool isDevelopment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") == "Development";

        if (!isDevelopment)
        {
            adminConfig.SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.Https;
            adminConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
            adminConfig.SaslMechanism = SaslMechanism.Plain;
            adminConfig.SaslUsername = kafkaSettings.SaslUsername;
            adminConfig.SaslPassword = kafkaSettings.SaslPassword;

            config.SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.Https;
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslMechanism = SaslMechanism.Plain;
            config.SaslUsername = kafkaSettings.SaslUsername;
            config.SaslPassword = kafkaSettings.SaslPassword;

            topicSpec.NumPartitions = 6;
            topicSpec.ReplicationFactor = 3;
        }

        AdminClientConfig = adminConfig;
        ClientConfig = config;
        TopicSpecification = topicSpec;
    }
}