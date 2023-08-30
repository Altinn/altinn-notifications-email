using Altinn.Notifications.Email.Core.Configuration;
using Altinn.Notifications.Email.Core.Dependencies;
using Altinn.Notifications.Email.Core.Models;

namespace Altinn.Notifications.Email.Core;

/// <summary>
/// A service implementation of the <see cref="IEmailService"/> class
/// </summary>
public class EmailService : IEmailService
{
    private readonly IEmailServiceClient _emailServiceClient;
    private readonly TopicSettings _settings;
    private readonly ICommonProducer _producer;

    /// <summary>
    /// Initializes a new instance of the <see cref="EmailService"/> class.
    /// </summary>
    /// <param name="emailServiceClient">A client that can perform actual mail sending.</param>
    /// <param name="producer">A kafka producer.</param>
    /// <param name="settings">The topic settings.</param>
    public EmailService(
        IEmailServiceClient emailServiceClient,
        ICommonProducer producer,
        TopicSettings settings)
    {
        _emailServiceClient = emailServiceClient;
        _producer = producer;
        _settings = settings;
    }

    /// <inheritdoc/>
    public async Task SendAsync(Models.Email email)
    {
        string operationId = await _emailServiceClient.SendEmail(email);

        var operationIdentifier = new SendNotificationOperationIdentifier()
        {
            NotificationId = email.NotificationId,
            OperationId = operationId
        };

        await _producer.ProduceAsync(_settings.EmailSendingAcceptedTopicName, operationIdentifier.Serialize());
    }

    /// <inheritdoc/>
    public async Task UpdateSendStatus(SendNotificationOperationIdentifier operationIdentifier)
    {
        EmailSendResult result = await _emailServiceClient.GetOperationUpdate(operationIdentifier.OperationId);

        var operationResult = new SendOperationResult()
        {
            NotificationId = operationIdentifier.NotificationId,
            OperationId = operationIdentifier.OperationId,
            SendResult = result
        };

        if (result != EmailSendResult.Sending)
        {
            await _producer.ProduceAsync(_settings.EmailStatusUpdatedTopicName, operationResult.Serialize());
        }
        else
        {
            await _producer.ProduceAsync(_settings.EmailSendingAcceptedRetryTopicName, operationIdentifier.Serialize());
        }
    }
}
