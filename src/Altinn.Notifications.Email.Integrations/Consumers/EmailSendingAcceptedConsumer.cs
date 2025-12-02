using Altinn.Notifications.Email.Core;
using Altinn.Notifications.Email.Core.Dependencies;
using Altinn.Notifications.Email.Integrations.Configuration;
using Altinn.Notifications.Integrations.Kafka.Consumers;

using Microsoft.Extensions.Logging;

namespace Altinn.Notifications.Email.Integrations.Consumers;

/// <summary>
/// Kafka consumer class for handling the email queue using batch processing.
/// </summary>
public sealed class EmailSendingAcceptedConsumer : KafkaConsumerBase
{
    private readonly IStatusService _statusService;
    private readonly ICommonProducer _producer;
    private readonly string _retryTopicName;
    private const int _processingDelay = 8000;
    private readonly IDateTimeService _dateTime;
    private readonly ILogger<EmailSendingAcceptedConsumer> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="EmailSendingAcceptedConsumer"/> class.
    /// </summary>
    public EmailSendingAcceptedConsumer(
        IStatusService statusService,
        ICommonProducer producer,
        KafkaSettings kafkaSettings,
        IDateTimeService dateTime,
        ILogger<EmailSendingAcceptedConsumer> logger)
        : base(kafkaSettings.EmailSendingAcceptedTopicName, kafkaSettings, logger)
    {
        _statusService = statusService;
        _producer = producer;
        _retryTopicName = kafkaSettings.EmailSendingAcceptedTopicName;
        _dateTime = dateTime;
        _logger = logger;
    }

    /// <inheritdoc/>
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return ConsumeMessageBatch(ConsumeOperationBatch, RetryOperation, stoppingToken);
    }

    private async Task ConsumeOperationBatch(IEnumerable<string> messages)
    {
        var messageList = messages.ToList();
        if (messageList.Count == 0)
        {
            return;
        }

        _logger.LogInformation(
            "// EmailSendingAcceptedConsumer // ConsumeOperationBatch // Processing batch of {Count} messages",
            messageList.Count);

        // Parse all messages first to validate them
        var operationIdentifiers = new List<SendNotificationOperationIdentifier>();
        var invalidMessages = new List<string>();

        foreach (var message in messageList)
        {
            if (SendNotificationOperationIdentifier.TryParse(message, out SendNotificationOperationIdentifier operationIdentifier))
            {
                operationIdentifiers.Add(operationIdentifier);
            }
            else
            {
                _logger.LogError(
                    "// EmailSendingAcceptedConsumer // ConsumeOperationBatch // Deserialization of message failed. {Message}",
                    message);
                invalidMessages.Add(message);
            }
        }

        if (operationIdentifiers.Count == 0)
        {
            _logger.LogWarning(
                "// EmailSendingAcceptedConsumer // ConsumeOperationBatch // No valid messages in batch of {Count}",
                messageList.Count);
            return;
        }

        // Apply delay once per batch based on the first valid operation
        var firstOperation = operationIdentifiers[0];
        int diff = (int)(_dateTime.UtcNow() - firstOperation.LastStatusCheck).TotalMilliseconds;

        if (diff > 0 && diff < _processingDelay)
        {
            var delayTime = _processingDelay - diff;
            _logger.LogInformation(
                "// EmailSendingAcceptedConsumer // ConsumeOperationBatch // Applying batch delay of {DelayMs}ms for {Count} messages",
                delayTime,
                operationIdentifiers.Count);

            await Task.Delay(delayTime);
        }

        // Process all valid operations
        var processingTasks = operationIdentifiers.Select(async operationIdentifier =>
        {
            try
            {
                await _statusService.UpdateSendStatus(operationIdentifier);

                _logger.LogDebug(
                    "// EmailSendingAcceptedConsumer // ConsumeOperationBatch // Processed operation {OperationId}",
                    operationIdentifier.OperationId);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "// EmailSendingAcceptedConsumer // ConsumeOperationBatch // Failed to process operation {OperationId}",
                    operationIdentifier.OperationId);
                throw;
            }
        });

        await Task.WhenAll(processingTasks);

        _logger.LogInformation(
            "// EmailSendingAcceptedConsumer // ConsumeOperationBatch // Successfully processed {ValidCount} operations, {InvalidCount} invalid messages",
            operationIdentifiers.Count,
            invalidMessages.Count);
    }

    private async Task RetryOperation(string message)
    {
        await _producer.ProduceAsync(_retryTopicName, message);
    }
}
