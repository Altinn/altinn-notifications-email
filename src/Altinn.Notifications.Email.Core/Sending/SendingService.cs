using Altinn.Notifications.Email.Core.Configuration;
using Altinn.Notifications.Email.Core.Dependencies;
using Altinn.Notifications.Email.Core.Models;
using Altinn.Notifications.Email.Core.Status;

namespace Altinn.Notifications.Email.Core.Sending;

/// <summary>
/// Service responsible for handling email sending requests.
/// </summary>
public class SendingService : ISendingService
{
    private readonly IEmailServiceClient _emailServiceClient;
    private readonly TopicSettings _settings;
    private readonly ICommonProducer _producer;
    private readonly string _failedInvalidEmailFormatErrorMessage = "Invalid format for email address";

    /// <summary>
    /// Initializes a new instance of the <see cref="SendingService"/> class.
    /// </summary>
    /// <param name="emailServiceClient">A client that can perform actual mail sending.</param>
    /// <param name="producer">A kafka producer.</param>
    /// <param name="settings">The topic settings.</param>
    public SendingService(
        IEmailServiceClient emailServiceClient,
        ICommonProducer producer,
        TopicSettings settings)
    {
        _emailServiceClient = emailServiceClient;
        _producer = producer;
        _settings = settings;
    }

    /// <inheritdoc/>
    public async Task SendAsync(Email email)
    {
        (string? operationId, ServiceError? serviceError) = await _emailServiceClient.SendEmail(email);

        if (operationId != null)
        {
            var operationIdentifier = new SendNotificationOperationIdentifier()
            {
                NotificationId = email.NotificationId,
                OperationId = operationId
            };

            await _producer.ProduceAsync(_settings.EmailSendingAcceptedTopicName, operationIdentifier.Serialize());
        }
        else
        {
            var operationResult = new SendOperationResult()
            {
                NotificationId = email.NotificationId,
                SendResult = EmailSendResult.Failed
            };
            
            if (serviceError!.ErrorMessage!.Contains(_failedInvalidEmailFormatErrorMessage))
            {
                operationResult.SendResult = EmailSendResult.Failed_InvalidEmailFormat;
            }

            await _producer.ProduceAsync(_settings.EmailStatusUpdatedTopicName, operationResult.Serialize());
        }
    }
}
