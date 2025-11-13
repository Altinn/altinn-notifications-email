using System.Diagnostics;
using System.Text;
using System.Text.Json;

using Altinn.Notifications.Email.Core.Status;
using Altinn.Notifications.Email.Mappers;

using Azure.Messaging.EventGrid;
using Azure.Messaging.EventGrid.SystemEvents;

namespace Altinn.Notifications.Email.Telemetry;

/// <summary>
/// Middleware that extracts send operation results from EventGrid email delivery report events
/// in POST request bodies and adds them as tags to OpenTelemetry Activity for Application Insights tracking.
/// </summary>
/// <param name="next">The next middleware delegate in the request pipeline.</param>
public class RequestBodyTelemetryMiddleware(RequestDelegate next)
{
    private readonly RequestDelegate _next = next;

    /// <summary>
    /// Invokes the middleware to extract send operation results from EventGrid email delivery report events.
    /// The extracted data is added as tags to the current OpenTelemetry Activity, which will appear in 
    /// Application Insights customDimensions.
    /// </summary>
    /// <param name="context">The HTTP context for the current request.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    public async Task InvokeAsync(HttpContext context)
    {
        // Check if it's a POST request
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

            // Extract send operation results if the body contains EventGrid email delivery report events
            var sendOperationResults = ExtractSendOperationResults(body);
            
            // Get the current Activity (OpenTelemetry tracing context)
            var activity = Activity.Current;
            if (activity != null && sendOperationResults.Count > 0)
            {
                // Add send operation results as a custom tag - will appear in Application Insights customDimensions
                activity.SetTag("SendOperationResults", JsonSerializer.Serialize(sendOperationResults));
            }
        }

        // Continue to the next middleware in the pipeline
        await _next(context);
    }

    /// <summary>
    /// Extracts send operation results from EventGrid events containing AcsEmailDeliveryReportReceivedEventData.
    /// Each result contains the operation ID (message ID) and the parsed email send result (delivery status).
    /// </summary>
    /// <param name="body">The request body as a string containing EventGrid events in JSON format.</param>
    /// <returns>A list of <see cref="SendOperationResult"/> objects extracted from email delivery report events.</returns>
    private static List<SendOperationResult> ExtractSendOperationResults(string body)
    {
        var sendOperationResults = new List<SendOperationResult>();

        if (string.IsNullOrWhiteSpace(body))
        {
            return sendOperationResults;
        }

        try
        {
            // Use EventGridEvent.ParseMany to properly deserialize with BinaryData support
            var eventList = EventGridEvent.ParseMany(BinaryData.FromString(body));
            if (eventList == null)
            {
                return sendOperationResults;
            }

            foreach (EventGridEvent eventGridEvent in eventList)
            {
                // If the event is a system event, TryGetSystemEventData will return the deserialized system event
                if (eventGridEvent.TryGetSystemEventData(out object systemEvent))
                {
                    if (systemEvent is AcsEmailDeliveryReportReceivedEventData deliveryReport)
                    {
                        sendOperationResults.Add(new SendOperationResult 
                        { 
                            OperationId = deliveryReport.MessageId, 
                            SendResult = EmailSendResultMapper.ParseDeliveryStatus(deliveryReport.Status?.ToString()) 
                        });
                    }
                }
            }
        }
        catch (Exception)
        {
            // Not a valid EventGrid event array or parsing failed, return empty list
        }

        return sendOperationResults;
    }
}
