using Marten.Metadata;
using Repro.Api.Domain.Events;

namespace Repro.Api.Domain;

public class Todo
{
    public Guid Id { get; set; }

    public string Description { get; set; } = null!;

    public static Todo Create(TodoCreated @event) => new()
    {
        Id = @event.TodoId,
        Description = @event.Description,
    };

    public void Apply(TodoUpdated @event)
    {
        Description = @event.Description;
    }
}
