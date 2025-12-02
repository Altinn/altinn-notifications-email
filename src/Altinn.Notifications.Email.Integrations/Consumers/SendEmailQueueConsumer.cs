using Altinn.Notifications.Email.Core.Dependencies;
using Altinn.Notifications.Email.Core.Sending;
using Altinn.Notifications.Email.Integrations.Configuration;
using Altinn.Notifications.Integrations.Kafka.Consumers;

using Microsoft.Extensions.Logging;

namespace Altinn.Notifications.Email.Integrations.Consumers;

/// <summary>
/// Kafka consumer class for handling the email queue using batch processing.
/// </summary>
public sealed class SendEmailQueueConsumer : KafkaConsumerBase
{
    private readonly string _retryTopicName;
    private readonly ICommonProducer _producer;
    private readonly ISendingService _emailService;
    private readonly ILogger<SendEmailQueueConsumer> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="SendEmailQueueConsumer"/> class.
    /// </summary>
    public SendEmailQueueConsumer(
        KafkaSettings kafkaSettings,
        ISendingService emailService,
        ICommonProducer producer,
        ILogger<SendEmailQueueConsumer> logger)
        : base(kafkaSettings.SendEmailQueueTopicName, kafkaSettings, logger)
    {
        _logger = logger;
        _producer = producer;
        _emailService = emailService;
        _retryTopicName = kafkaSettings.SendEmailQueueRetryTopicName;
    }

    /// <inheritdoc/>
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return ConsumeMessageBatch(ConsumeEmailBatch, RetryEmail, stoppingToken);
    }

    private async Task ConsumeEmailBatch(IEnumerable<string> messages)
    {
        var messageList = messages.ToList();
        if (messageList.Count == 0)
        {
            return;
        }

        _logger.LogInformation(
            "// SendEmailQueueConsumer // ConsumeEmailBatch // Processing batch of {Count} messages",
            messageList.Count);

        // Parse all messages first to validate them
        var emails = new List<Core.Sending.Email>();
        var invalidMessages = new List<string>();

        foreach (var message in messageList)
        {
            if (Core.Sending.Email.TryParse(message, out Core.Sending.Email email))
            {
                emails.Add(email);
            }
            else
            {
                _logger.LogError(
                    "// SendEmailQueueConsumer // ConsumeEmailBatch // Deserialization of message failed. {Message}",
                    message);
                invalidMessages.Add(message);
            }
        }

        if (emails.Count == 0)
        {
            _logger.LogWarning(
                "// SendEmailQueueConsumer // ConsumeEmailBatch // No valid emails in batch of {Count}",
                messageList.Count);
            return;
        }

        // Process all valid emails concurrently
        var processingTasks = emails.Select(async email =>
        {
            try
            {
                await _emailService.SendAsync(email);

                _logger.LogDebug(
                    "// SendEmailQueueConsumer // ConsumeEmailBatch // Processed email {NotificationId}",
                    email.NotificationId);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "// SendEmailQueueConsumer // ConsumeEmailBatch // Failed to send email {NotificationId}",
                    email.NotificationId);
                throw;
            }
        });

        await Task.WhenAll(processingTasks);

        _logger.LogInformation(
            "// SendEmailQueueConsumer // ConsumeEmailBatch // Successfully processed {ValidCount} emails, {InvalidCount} invalid messages",
            emails.Count,
            invalidMessages.Count);
    }

    private async Task RetryEmail(string message)
    {
        await _producer.ProduceAsync(_retryTopicName, message);
    }
}
