defmodule AirtableSnapshotTest do
  @bucket_name "dialer-airtable-snapshots"

  use ExUnit.Case
  doctest AirtableSnapshot

  # Note this test will fail if Airtable is down
  test "fetches call sync data" do
    resp =
      AirtableSnapshot.fetch(%{
        bucket_name: @bucket_name,
        key: System.get_env("AIRTABLE_KEY"),
        table: System.get_env("AIRTABLE_TABLE"),
        base: System.get_env("AIRTABLE_BASE")
      })

    assert is_map(resp)

    first_key = resp |> Map.keys() |> List.first()
    assert is_map(resp[first_key])
  end

  test "failure fetches latest snapshot" do
    resp =
      AirtableSnapshot.fetch(%{
        bucket_name: @bucket_name,
        key: "!!!! BAD KEY FORCE FAILURE !!!!",
        table: System.get_env("AIRTABLE_TABLE"),
        base: System.get_env("AIRTABLE_BASE")
      })

    assert is_map(resp)

    first_key = resp |> Map.keys() |> List.first()
    assert is_map(resp[first_key])
  end
end
