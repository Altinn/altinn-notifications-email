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
    private readonly string _retryTopicName;
    private const int _processingDelay = 8000;

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
        _retryTopicName = kafkaSettings.EmailSendingAcceptedTopicName;
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

        int diff = (int)(DateTime.UtcNow - operationIdentifier.LastStatusCheck).TotalMilliseconds;

        if (diff < _processingDelay)
        {
            await Task.Delay(_processingDelay - diff);
        }

        Console.WriteLine($"// EmailSendingAcceptedConsumer // ConsumeOperation // {operationIdentifier.OperationId} Calling service: " + DateTime.UtcNow);
        await _statusService.UpdateSendStatus(operationIdentifier);
    }

    private async Task RetryOperation(string message)
    {
        await _producer.ProduceAsync(_retryTopicName, message);
    }
}
