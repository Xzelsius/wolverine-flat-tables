namespace Repro.Api.Domain.Events;

public record TodoCreated(Guid TodoId, string Description);
