using System.Diagnostics;
using System.Text;
using System.Text.Json;

using Azure.Messaging.EventGrid;
using Azure.Messaging.EventGrid.SystemEvents;

using Microsoft.ApplicationInsights.DataContracts;

namespace Altinn.Notifications.Email.Telemetry;

/// <summary>
/// Middleware that captures HTTP request bodies for POST and PUT requests and adds them to Application Insights telemetry.
/// </summary>
/// <param name="next">The next middleware delegate in the request pipeline.</param>
public class RequestBodyTelemetryMiddleware(RequestDelegate next)
{
    private readonly RequestDelegate _next = next;

    /// <summary>
    /// Invokes the middleware to capture and log request body content.
    /// </summary>
    /// <param name="context">The HTTP context for the current request.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    public async Task InvokeAsync(HttpContext context)
    {
        // 1. Check if it's a POST request
        if (context.Request.Method == HttpMethods.Post)
        {
            // Allow the body to be read multiple times (rewindable)
            context.Request.EnableBuffering();

            // Leave the body stream open after reading
            using var reader = new StreamReader(
                context.Request.Body,
                Encoding.UTF8,
                detectEncodingFromByteOrderMarks: false,
                bufferSize: 1024,
                leaveOpen: true);
            var body = await reader.ReadToEndAsync();

            // Reset the stream's position to 0 so the next middleware/controller can read it
            context.Request.Body.Position = 0;

            // Extract operation IDs if the body contains EventGrid events
            var operationIds = ExtractOperationIds(body);
            
            // Get the current Activity (OpenTelemetry equivalent of RequestTelemetry)
            var activity = Activity.Current;
            if (activity != null && operationIds.Count > 0)
            {
                // Add a custom tag to the activity - this will appear in Application Insights customDimensions
                activity.SetTag("OperationIds", string.Join(", ", operationIds));
            }
        }

        // Continue to the next middleware in the pipeline
        await _next(context);
    }

    /// <summary>
    /// Extracts operation IDs from EventGrid events containing AcsEmailDeliveryReportReceivedEventData.
    /// </summary>
    /// <param name="body">The request body as a string.</param>
    /// <returns>A list of operation IDs (message IDs) from delivery reports.</returns>
    private static List<string> ExtractOperationIds(string body)
    {
        var operationIds = new List<string>();

        if (string.IsNullOrWhiteSpace(body))
        {
            return operationIds;
        }

        try
        {
            // Use EventGridEvent.ParseMany to properly deserialize with BinaryData support
            var eventList = EventGridEvent.ParseMany(BinaryData.FromString(body));
            if (eventList == null)
            {
                return operationIds;
            }

            foreach (EventGridEvent eventGridEvent in eventList)
            {
                // If the event is a system event, TryGetSystemEventData will return the deserialized system event
                if (eventGridEvent.TryGetSystemEventData(out object systemEvent))
                {
                    if (systemEvent is AcsEmailDeliveryReportReceivedEventData deliveryReport)
                    {
                        operationIds.Add(deliveryReport.MessageId);
                    }
                }
            }
        }
        catch (Exception)
        {
            // Not a valid EventGrid event array, skip operation ID extraction
        }

        return operationIds;
    }
}
