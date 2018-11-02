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
        base: System.get_env("AIRTABLE_BASE"),
        filter_records: fn records ->
          Enum.filter(records, &Map.has_key?(&1["fields"], "Service Names"))
        end,
        process_records: fn records ->
          Enum.map(records, fn %{"fields" => fields} ->
            {
              fields["Reference Name"],
              %{
                "service_names" =>
                  String.split(fields["Service Names"], ",") |> Enum.map(&String.trim(&1)),
                "district_abbreviation" => fields["District Abbreviation"],
                "system" => fields["System"],
                "api_key" => fields["API Key"],
                "tag_ids" => fields["Tag Ids"],
                "active" => fields["Active"],
                "reference_name" => fields["Reference Name"],
                "strategy" => fields["Strategy"],
                "report_to" => fields["Send Report To"],
                "timezone" => fields["Timezone"] |> String.downcase(),
                "reseller" => fields["Reseller"],
                "reseller_emails" => fields["Reseller Emails"]
              }
            }
          end)
          |> Enum.into(%{})
        end
      })

    assert is_map(resp)

    first_key = Map.keys(resp) |> List.first()
    assert is_map(resp[first_key])
  end

  test "failure fetches latest snapshot" do
    resp =
      AirtableSnapshot.fetch(%{
        bucket_name: @bucket_name,
        key: "!!!! BAD KEY FORCE FAILURE !!!!",
        table: System.get_env("AIRTABLE_TABLE"),
        base: System.get_env("AIRTABLE_BASE"),
        filter_records: fn records ->
          Enum.filter(records, &Map.has_key?(&1["fields"], "Service Names"))
        end,
        process_records: fn records ->
          Enum.map(records, fn %{"fields" => fields} ->
            {
              fields["Reference Name"],
              %{
                "service_names" =>
                  String.split(fields["Service Names"], ",") |> Enum.map(&String.trim(&1)),
                "district_abbreviation" => fields["District Abbreviation"],
                "system" => fields["System"],
                "api_key" => fields["API Key"],
                "tag_ids" => fields["Tag Ids"],
                "active" => fields["Active"],
                "reference_name" => fields["Reference Name"],
                "strategy" => fields["Strategy"],
                "report_to" => fields["Send Report To"],
                "timezone" => fields["Timezone"] |> String.downcase(),
                "reseller" => fields["Reseller"],
                "reseller_emails" => fields["Reseller Emails"]
              }
            }
          end)
          |> Enum.into(%{})
        end
      })

    assert is_map(resp)

    first_key = Map.keys(resp) |> List.first()
    assert is_map(resp[first_key])
  end
end
