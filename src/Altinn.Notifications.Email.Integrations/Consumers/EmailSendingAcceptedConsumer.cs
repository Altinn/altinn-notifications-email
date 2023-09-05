using Altinn.Notifications.Email.Core;
using Altinn.Notifications.Email.Core.Dependencies;
using Altinn.Notifications.Email.Integrations.Configuration;
using Altinn.Notifications.Integrations.Kafka.Consumers;

using Microsoft.Extensions.Logging;

namespace Altinn.Notifications.Email.Integrations.Consumers;

/// <summary>
/// Kafka consumer class for handling the email queue.
/// </summary>
public sealed class EmailSendingAcceptedConsumer : KafkaConsumerBase<EmailSendingAcceptedConsumer>
{
    private readonly IStatusService _statusService;
    private readonly ICommonProducer _producer;
    private readonly ILogger<EmailSendingAcceptedConsumer> _logger;
    private readonly string _retryTopicName;

    /// <summary>
    /// Initializes a new instance of the <see cref="EmailSendingAcceptedConsumer"/> class.
    /// </summary>
    public EmailSendingAcceptedConsumer(
        IStatusService statusService,
        ICommonProducer producer,
        KafkaSettings kafkaSettings,
        ILogger<EmailSendingAcceptedConsumer> logger)
        : base(kafkaSettings, logger, kafkaSettings.EmailSendingAcceptedTopicName)
    {
        _statusService = statusService;
        _producer = producer;
        _retryTopicName = kafkaSettings.EmailSendingAcceptedRetryTopicName;
        _logger = logger;
    }

    /// <inheritdoc/>
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() => ConsumeMessage(ConsumeOperation, RetryOperation, stoppingToken), stoppingToken);
    }

    private async Task ConsumeOperation(string message)
    {
        Console.WriteLine("// EmailSendingAcceptedConsumer // ConsumeOperation // " + message);
        bool succeeded = SendNotificationOperationIdentifier.TryParse(message, out SendNotificationOperationIdentifier operationIdentifier);

        if (!succeeded)
        {
            _logger.LogError("// EmailSendingAcceptedConsumer // ConsumeOperation // Deserialization of message failed. {Message}", message);
            Console.WriteLine("// EmailSendingAcceptedConsumer // ConsumeOperation // Deserialization of message failed.");
            return;
        }

        Console.WriteLine("// EmailSendingAcceptedConsumer // ConsumeOperation // Deserialization successful");

        await _statusService.UpdateSendStatus(operationIdentifier);
    }

    private async Task RetryOperation(string message)
    {
        Console.WriteLine("// EmailSendingAcceptedConsumer // RetryOperation // Pushing to retry topic " + _retryTopicName);
        await _producer.ProduceAsync(_retryTopicName, message);
    }
}
