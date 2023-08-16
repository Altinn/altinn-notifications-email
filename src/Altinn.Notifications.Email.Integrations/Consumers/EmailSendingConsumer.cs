using System.Text.Json;

using Altinn.Notifications.Email.Core;
using Altinn.Notifications.Email.Integrations.Configuration;

using Confluent.Kafka;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Altinn.Notifications.Email.Integrations.Consumers;

/// <summary>
/// Kafka consumer class for handling the email queue.
/// </summary>
public sealed class EmailSendingConsumer : BackgroundService
{
    private readonly KafkaSettings _kafkaSettings;
    private readonly IEmailService _emailService;
    private readonly ILogger<EmailSendingConsumer> _logger;

    private readonly IConsumer<string, string> _consumer;

    /// <summary>
    /// Initializes a new instance of the <see cref="EmailSendingConsumer"/> class.
    /// </summary>
    public EmailSendingConsumer(
        KafkaSettings kafkaSettings,
        IEmailService emailService,
        ILogger<EmailSendingConsumer> logger)
    {
        _kafkaSettings = kafkaSettings;
        _emailService = emailService;
        _logger = logger;

        var config = new SharedClientConfig(_kafkaSettings);
        var consumerConfig = new ConsumerConfig(config.ClientConfig)
        {
            GroupId = _kafkaSettings.EmailSendingConsumerSettings.ConsumerGroupId,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };

        _consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetErrorHandler((_, e) => _logger.LogError("Error: {reason}", e.Reason))
            .Build();
    }

    /// <inheritdoc/>
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() => ConsumeEmail(stoppingToken), stoppingToken);
    }

    /// <inheritdoc/>
    public override void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();

        base.Dispose();
    }

    private async Task ConsumeEmail(CancellationToken cancellationToken)
    {
        _consumer.Subscribe(_kafkaSettings.EmailSendingConsumerSettings.TopicName);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var consumeResult = _consumer.Consume(cancellationToken);
                if (consumeResult != null)
                {
                    string serializedEmail = consumeResult.Message.Value;
                    Core.Models.Email? email = JsonSerializer.Deserialize<Core.Models.Email>(serializedEmail);
                    if (email != null)
                    {
                        await _emailService.SendEmail(email);
                    }
                    else
                    {
                        _logger.LogError(
                            "Message on topic {topic} did not produce an Core.Models.Email instance",
                            _kafkaSettings.EmailSendingConsumerSettings.TopicName);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "An error occurred while consuming messages");
            throw;
        }
    }
}