using Altinn.Notifications.Email.Core;
using Azure.Messaging.EventGrid;
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
        return await _statusService.ProcessDeliveryReports(eventList);
    }
}
