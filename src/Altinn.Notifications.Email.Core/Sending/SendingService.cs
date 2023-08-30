﻿using Altinn.Notifications.Email.Core.Configuration;
using Altinn.Notifications.Email.Core.Dependencies;

namespace Altinn.Notifications.Email.Core.Sending;

/// <summary>
/// A service implementation of the <see cref="ISendingService"/> class
/// </summary>
public class SendingService : ISendingService
{
    private readonly IEmailServiceClient _emailServiceClient;
    private readonly TopicSettings _settings;
    private readonly ICommonProducer _producer;

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
        string operationId = await _emailServiceClient.SendEmail(email);

        var operationIdentifier = new SendNotificationOperationIdentifier()
        {
            NotificationId = email.NotificationId,
            OperationId = operationId
        };

        await _producer.ProduceAsync(_settings.EmailSendingAcceptedTopicName, operationIdentifier.Serialize());
    }
}