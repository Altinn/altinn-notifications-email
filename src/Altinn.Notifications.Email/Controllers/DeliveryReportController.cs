using System.Text.Json;

using Altinn.Notifications.Email.Core;
using Altinn.Notifications.Email.Core.Status;

using Azure.Messaging.EventGrid;
using Azure.Messaging.EventGrid.SystemEvents;

using Microsoft.AspNetCore.Mvc;
using Swashbuckle.AspNetCore.Annotations;

namespace Altinn.Notifications.Email.Controllers;

/// <summary>
/// Controller for handling delivery reports from Azure Communication Services
/// </summary>
[Route("notifications/email/api/v1/reports")]
[ApiController]
[SwaggerResponse(401, "Caller is unauthorized")]
public class DeliveryReportController
{
    private readonly IStatusService _statusService;

    /// <summary>
    /// Initializes a new instance of the <see cref="DeliveryReportController"/> class.
    /// </summary>
    public DeliveryReportController(IStatusService statusService)
    {
        _statusService = statusService;
    }

    /// <summary>
    /// Post method for handling delivery reports from Azure Communication Services
    /// </summary>
    [HttpPost]
    [Produces("application/json")]
    [SwaggerResponse(200, "The delivery report is received")]
    [SwaggerResponse(400, "The delivery report is invalid")]
    public async Task<ActionResult<string>> Post([FromBody] EventGridEvent[] eventList)
    {
        foreach (EventGridEvent eventgridevent in eventList)
        {
            // If the event is a system event, TryGetSystemEventData will return the deserialized system event
            if (eventgridevent.TryGetSystemEventData(out object systemEvent))
            {
                switch (systemEvent)
                {
                    case SubscriptionValidationEventData subscriptionValidated:
                        var responseData = new SubscriptionValidationResponse()
                        {
                            ValidationResponse = subscriptionValidated.ValidationCode
                        };
                        return JsonSerializer.Serialize(responseData);
                    case AcsEmailDeliveryReportReceivedEventData deliveryReport:
                        var operationResult = new SendOperationResult()
                        {
                            OperationId = deliveryReport.MessageId,
                            SendResult = EmailSendResultMapper.ParseDeliveryStatus(deliveryReport.Status?.ToString())
                        };
                        await _statusService.UpdateSendStatus(operationResult);
                        break;
                }
            }
        }

        return string.Empty;
    }
}
