namespace Altinn.Notifications.Email.Core.Configuration;

/// <summary>
/// Configuration object used to hold integration settings for a Kafka.
/// </summary>
public class TopicSettings
{
    /// <summary>
    /// The name of the email sending accepted topic
    /// </summary>
    /// Todo: Consider renaming, content on topic is operation id and notifications. e.g. NewOprationsTopic? 
    public string EmailSendingAcceptedTopicName { get; set; } = string.Empty;

    /// <summary>
    /// The name of the email sending accepted retry topic
    /// </summary>
    /// Todo: Consider renaming, content on topic is operation id and notifications. e.g. NewOprationsTopic? 
    public string EmailSendingAcceptedRetryTopicName { get; set; } = string.Empty;
    
    /// <summary>
    /// The name of the email operation result topic
    /// </summary>    
    public string EmailOperationResultTopicName { get; set; } = string.Empty;
}