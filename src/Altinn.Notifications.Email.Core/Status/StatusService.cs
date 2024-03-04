using System.Text.Json;

using Altinn.Notifications.Email.Core.Configuration;
using Altinn.Notifications.Email.Core.Dependencies;
using Altinn.Notifications.Email.Core.Status;

using Azure.Messaging.EventGrid;
using Azure.Messaging.EventGrid.SystemEvents;

namespace Altinn.Notifications.Email.Core;

/// <summary>
/// A service implementation of the <see cref="IStatusService"/> class
/// </summary>
public class StatusService : IStatusService
{
    private readonly IEmailServiceClient _emailServiceClient;
    private readonly TopicSettings _settings;
    private readonly ICommonProducer _producer;
    private readonly IDateTimeService _dateTime;

    /// <summary>
    /// Initializes a new instance of the <see cref="StatusService"/> class.
    /// </summary>
    /// <param name="emailServiceClient">A client that can perform actual mail sending.</param>
    /// <param name="producer">A kafka producer.</param>
    /// <param name="dateTime">A datetime service.</param>
    /// <param name="settings">The topic settings.</param>
    public StatusService(
        IEmailServiceClient emailServiceClient,
        ICommonProducer producer,
        IDateTimeService dateTime,
        TopicSettings settings)
    {
        _emailServiceClient = emailServiceClient;
        _producer = producer;
        _settings = settings;
        _dateTime = dateTime;
    }

    /// <inheritdoc/>
    public async Task UpdateSendStatus(SendNotificationOperationIdentifier operationIdentifier)
    {
        EmailSendResult result = await _emailServiceClient.GetOperationUpdate(operationIdentifier.OperationId);

        if (result != EmailSendResult.Sending)
        {
            var operationResult = new SendOperationResult()
            {
                NotificationId = operationIdentifier.NotificationId,
                OperationId = operationIdentifier.OperationId,
                SendResult = result
            };

            await _producer.ProduceAsync(_settings.EmailStatusUpdatedTopicName, operationResult.Serialize());
        }
        else
        {
            operationIdentifier.LastStatusCheck = _dateTime.UtcNow();
            await _producer.ProduceAsync(_settings.EmailSendingAcceptedTopicName, operationIdentifier.Serialize());
        }
    }

    /// <inheritdoc/>
    public async Task<string> ProcessDeliveryReports(EventGridEvent[] eventList)
    {
        foreach (EventGridEvent eventgridevent in eventList)
        {
            // If the event is a system event, TryGetSystemEventData will return the deserialized system event
            if (eventgridevent.TryGetSystemEventData(out object systemEvent))
            {
                switch (systemEvent)
                {
                    case SubscriptionValidationEventData subscriptionValidated:
                        Console.WriteLine(subscriptionValidated.ValidationCode);
                        var responseData = new SubscriptionValidationResponse()
                        {
                            ValidationResponse = subscriptionValidated.ValidationCode
                        };
                        return JsonSerializer.Serialize(responseData);
                    case AcsEmailDeliveryReportReceivedEventData deliveryReport:
                        deliveryReport.Status.ToString();
                        Console.WriteLine(deliveryReport.MessageId);
                        var operationResult = new SendOperationResult()
                        {
                            OperationId = deliveryReport.MessageId,
                            SendResult = EmailSendResultMapper.ParseDeliveryStatus(deliveryReport.Status)
                        };
                        await _producer.ProduceAsync(_settings.EmailStatusUpdatedTopicName, operationResult.Serialize());
                        break;
                }
            }
        }

        return string.Empty;
    }
}
