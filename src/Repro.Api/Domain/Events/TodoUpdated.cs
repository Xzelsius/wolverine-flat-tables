namespace Repro.Api.Domain.Events;

public record TodoUpdated(Guid TodoId, string Description);
