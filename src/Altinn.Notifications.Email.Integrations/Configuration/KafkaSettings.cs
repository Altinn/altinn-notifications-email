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
    /// The group id for all consumers of the Altinn Notifications service
    /// </summary>
    public string ConsumerGroupId { get; set; } = string.Empty;

    /// <summary>
    /// The SASL username
    /// </summary>
    public string SaslUsername { get; set; } = string.Empty;

    /// <summary>
    /// The SASL password
    /// </summary>
    public string SaslPassword { get; set; } = string.Empty;

    /// <summary>
    /// The name of the email sending accepted topic
    /// </summary>
    public string EmailSendingAcceptedTopicName { get; set; } = string.Empty;

    /// <summary>
    /// The name of the send email queue topic
    /// </summary>
    public string SendEmailQueueTopicName { get; set; } = string.Empty;

    /// <summary>
    /// List of topics this application will write to.
    /// </summary>
    public List<string> TopicList { get; set; } = new List<string>();
}