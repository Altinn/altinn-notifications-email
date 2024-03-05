using Azure.Messaging.EventGrid.SystemEvents;

namespace Altinn.Notifications.Email.Core.Status;

/// <summary>
/// Mapper handling parsing to EmailSendResult
/// </summary>
public static class EmailSendResultMapper
{
    /// <summary>
    /// Parse AcsEmailDeliveryReportStatus to EmailSendResult
    /// </summary>
    /// <param name="deliveryStatus">Delivery status from Azure Communication Service</param>
    /// <returns></returns>
    /// <exception cref="ArgumentException">Throws exception if unknown delivery status</exception>
    public static EmailSendResult ParseDeliveryStatus(AcsEmailDeliveryReportStatus? deliveryStatus)
    {
        if (deliveryStatus == null)
        {
            return EmailSendResult.Failed;
        }

        switch (deliveryStatus.ToString())
        {
            case "Bounced":
                return EmailSendResult.Failed_Bounced;
            case "Delivered":
                return EmailSendResult.Delivered;
            case "Failed":
                return EmailSendResult.Failed;
            case "FilteredSpam":
                return EmailSendResult.Failed_FilteredSpam;
            case "Quarantined":
                return EmailSendResult.Failed_Quarantined;
            case "Suppressed":
                return EmailSendResult.Failed_SupressedRecipient;
            default:
                throw new ArgumentException($"Unhandled DeliveryStatus: {deliveryStatus}");
        }
    }
}
