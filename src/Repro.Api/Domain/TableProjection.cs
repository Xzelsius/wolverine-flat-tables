namespace Repro.Api.Domain;

using Marten;
using Marten.Events.Projections;
using SqlKata;
using SqlKata.Compilers;
using Weasel.Core;
using Weasel.Postgresql.Tables;

public record RowCreated(Guid Id, string Description, DateTimeOffset CreatedAt);

public record RowUpdated(
    Guid Id,
    string Description,
    string? Remarks,
    Guid SomeGuid,
    Guid? SomeNullableGuid,
    int SomeInt,
    int? SomeNullableInt,
    double SomeDouble,
    double? SomeNullableDouble,
    DateTimeOffset ModifiedAt);

public class TableProjection : EventProjection
{
    private readonly DbObjectName _tableIdentifier;

    public TableProjection()
    {
        var table = new Table("mt_table");

        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn<string>("description");
        table.AddColumn<string?>("remarks").AllowNulls();
        table.AddColumn<Guid>("some_guid");
        table.AddColumn<Guid?>("some_nullable_guid").AllowNulls();
        table.AddColumn<int>("some_int");
        table.AddColumn<int?>("some_nullable_int").AllowNulls();
        table.AddColumn<double>("some_double");
        table.AddColumn<double?>("some_nullable_double").AllowNulls();
        table.AddColumn<DateTimeOffset>("created_at");
        table.AddColumn<DateTimeOffset?>("modified_at").AllowNulls();

        _tableIdentifier = table.Identifier;
        SchemaObjects.Add(table);
    }

    public void Project(RowCreated @event, IDocumentOperations ops)
    {
        var sql = ToStatement(
            new Query(_tableIdentifier.ToString())
                .AsInsert(new
                {
                    id = @event.Id,
                    description = @event.Description,
                    created_at = @event.CreatedAt
                }));

        ops.QueueSqlCommand(sql.RawSql, sql.Bindings.ToArray());
    }

    public void Project(RowUpdated @event, IDocumentOperations ops)
    {
        var sql = ToStatement(
            new Query(_tableIdentifier.ToString())
                .Where(new { id = @event.Id })
                .AsUpdate(new
                {
                    description = @event.Description,
                    remarks = @event.Remarks ?? (object)DBNull.Value,
                    some_guid = @event.SomeGuid,
                    some_nullable_guid = @event.SomeNullableGuid ?? (object)DBNull.Value,
                    some_int = @event.SomeInt,
                    some_nullable_int = @event.SomeNullableInt ?? (object)DBNull.Value,
                    some_double = @event.SomeDouble,
                    some_nullable_double = @event.SomeNullableDouble ?? (object)DBNull.Value,
                    modified_at = @event.ModifiedAt,
                }));

        ops.QueueSqlCommand(sql.RawSql, sql.Bindings.ToArray());
    }

    private static SqlResult ToStatement(Query query)
    {
        var compiled = new PostgresCompiler().Compile(query);
        return compiled;
    }
}
