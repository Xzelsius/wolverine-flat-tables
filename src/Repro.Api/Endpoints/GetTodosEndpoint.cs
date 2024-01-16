using Marten;
using Repro.Api.Domain;
using Wolverine.Http;

namespace Repro.Api.Endpoints;

public static class GetTodosEndpoint
{
    [WolverineGet("/todos")]
    public static async Task<IEnumerable<Todo>> Get(IDocumentSession session)
    {
        return await session.Query<Todo>().ToListAsync();
    }
}
