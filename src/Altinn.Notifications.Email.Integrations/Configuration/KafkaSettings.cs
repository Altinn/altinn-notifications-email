using Altinn.Notifications.Email.Integrations.Consumers;

namespace Altinn.Notifications.Email.Integrations.Configuration;

/// <summary>
/// Configuration object used to hold integration settings for a Kafka.
/// </summary>
public class KafkaSettings
{
    /// <summary>
    /// The address of the Kafka broker
    /// </summary>
    public string BrokerAddress { get; set; } = string.Empty;

    /// <summary>
    /// The SASL username
    /// </summary>
    public string SaslUsername { get; set; } = string.Empty;

    /// <summary>
    /// The SASL password
    /// </summary>
    public string SaslPassword { get; set; } = string.Empty;

    /// <summary>
    /// Settings specific for the <see cref="EmailSendingConsumer"/> consumer.
    /// </summary>
    public EmailSendingConsumerSettings EmailSendingConsumerSettings { get; set; } = new();

    /// <summary>
    /// Settings specific for the <see cref="EmailSendingConsumer"/> consumer.
    /// </summary>
    public EmailSendingAcceptedProducerSettings EmailSendingAcceptedProducerSettings { get; set; } = new();

    /// <summary>
    /// List of topics this application will write to.
    /// </summary>
    public List<string> TopicList { get; set; } = new List<string>();
}

/// <summary>
/// Configuration object for the <see cref="EmailSendingConsumer"/>.
/// </summary>
public class EmailSendingConsumerSettings
{
    /// <summary>
    /// The group id for all consumers of the Altinn Notifications service
    /// </summary>
    public string ConsumerGroupId { get; set; } = string.Empty;
    
    /// <summary>
    /// The name of the past due orders topic 
    /// </summary>
    public string TopicName { get; set; } = string.Empty;
}

/// <summary>
/// Configuration object for the email sending accepted producer.
/// </summary>
public class EmailSendingAcceptedProducerSettings
{
    /// <summary>
    /// The name of the topic used when the email seding accepted event occur.
    /// </summary>
    public string TopicName { get; set; } = string.Empty;
}
