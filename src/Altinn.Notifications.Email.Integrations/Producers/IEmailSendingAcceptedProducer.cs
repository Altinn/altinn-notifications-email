namespace Altinn.Notifications.Email.Integrations.Producers;

/// <summary>
/// Interface for handling all producer actions for the email sending accepted Kafka topic
/// </summary>
public interface IEmailSendingAcceptedProducer
{
    /// <summary>
    /// Produces a message on the email sending accepted topic.
    /// </summary>
    /// <param name="message">The message to post</param>
    public Task<bool> ProduceAsync(string message);
}