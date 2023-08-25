using Altinn.Notifications.Email.Core;
using Altinn.Notifications.Email.Core.Integrations.Interfaces;
using Altinn.Notifications.Email.Core.Models;
using Altinn.Notifications.Email.Integrations.Configuration;
using Altinn.Notifications.Integrations.Kafka.Consumers;

using Confluent.Kafka;

using Microsoft.Extensions.Logging;

namespace Altinn.Notifications.Email.Integrations.Consumers;

/// <summary>
/// Kafka consumer class for handling the email queue.
/// </summary>
public sealed class EmailOperationConsumer : KafkaConsumerBase<EmailOperationConsumer>
{
    private readonly IEmailService _emailService;
    private readonly ICommonProducer _producer;
    private readonly string _retryTopicName;

    /// <summary>
    /// Initializes a new instance of the <see cref="EmailOperationConsumer"/> class.
    /// </summary>
    public EmailOperationConsumer(
        IEmailService emailService,
        ICommonProducer producer,
        KafkaSettings kafkaSettings,
        ILogger<EmailOperationConsumer> logger)
        : base(kafkaSettings, logger, kafkaSettings.EmailSendingAcceptedTopicName)
    {
        _emailService = emailService;
        _producer = producer;
        _retryTopicName = kafkaSettings.EmailSendingAcceptedRetryTopicName;
    }

    /// <inheritdoc/>
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() => ConsumeMessage(ConsumeOperation, RetryOperation, stoppingToken), stoppingToken);
    }

    private async Task ConsumeOperation(string message)
    {
        bool succeeded = SendNotificationOperationIdentifier.TryParse(message, out SendNotificationOperationIdentifier operationIdentifier);

        if (!succeeded)
        {
            return;
        }

        await _emailService.UpdateSendStatus(operationIdentifier);
    }

    private async Task RetryOperation(string message)
    {
        await _producer.ProduceAsync(_retryTopicName, message);
    }
}