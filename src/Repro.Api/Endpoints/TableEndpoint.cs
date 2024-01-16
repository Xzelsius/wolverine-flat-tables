namespace Repro.Api.Endpoints;

using Bogus;
using Repro.Api.Domain;
using Wolverine.Http;
using Wolverine.Marten;

public static class TableEndpoint
{
    private static readonly Randomizer Randomizer = new();

    [WolverinePut("/table/create-row")]
    public static IStartStream CreateRow()
    {
        var now = DateTimeOffset.UtcNow;

        var created = new RowCreated(Guid.NewGuid(), Randomizer.Words(3), now.AddDays(-Randomizer.Int(5, 10)));
        RowUpdated updated;

        if (Randomizer.Bool())
        {
            updated = new RowUpdated(
                created.Id,
                Randomizer.Words(3),
                null,
                Guid.NewGuid(),
                null,
                Randomizer.Int(),
                null,
                Randomizer.Double(),
                null,
                now.AddDays(-Randomizer.Int(1, 4)));
        }
        else
        {
            updated = new RowUpdated(
                created.Id,
                Randomizer.Words(3),
                Randomizer.Words(5),
                Guid.NewGuid(),
                Guid.NewGuid(),
                Randomizer.Int(),
                Randomizer.Int(),
                Randomizer.Double(),
                Randomizer.Double(),
                now.AddDays(-Randomizer.Int(0, 5)));
        }

        return MartenOps.StartStream<TableProjection>(created.Id, created, updated);
    }
}
