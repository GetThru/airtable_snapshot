defmodule AirtableSnapshot do
  require Logger

  def fetch(%{key: key, table: table, base: base} = opts)
      when is_binary(key) and is_binary(table) and is_binary(base) do
    filter_records = Map.get(opts, :filter_records, fn _ -> true end)
    process_records = Map.get(opts, :process_records, & &1)
    bucket_name = Map.get(opts, :bucket_name, "dialer-airtable-snapshots")

    fetch_records(%{
      key: key,
      table: table,
      base: base,
      filter_records: filter_records,
      process_records: process_records,
      bucket_name: bucket_name
    })
  end

  def fetch_records(
        opts = %{
          key: key,
          table: table,
          base: base,
          filter_records: filter_records,
          process_records: process_records
        },
        prev_records \\ [],
        offset \\ 0
      ) do
    %{body: raw_body} =
      HTTPotion.get(
        "https://api.airtable.com/v0/#{base}/#{URI.encode(table)}",
        headers: [
          Authorization: "Bearer #{key}"
        ],
        query: [offset: offset],
        timeout: :infinity
      )

    body = Jason.decode!(raw_body)

    case body do
      %{"offset" => next_offset, "records" => records} ->
        accumulated_records = Enum.concat(prev_records, records)
        fetch_records(opts, accumulated_records, next_offset)

      %{"records" => records} ->
        prev_records
        |> Enum.concat(records)
        |> filter_records.()
        |> process_records.()
        |> cache_snapshot(opts)
    end
  rescue
    error ->
      Logger.error("""
      Failed to fetch from Airtable key=#{key} table=#{table} base=#{base}
      #{inspect(error)}
      Falling back to S3 cache.
      """)

      fetch_latest_snapshot(opts)
  end

  defp cache_snapshot(contents, opts = %{bucket_name: bucket_name}) do
    spawn(fn ->
      timestamp = DateTime.utc_now() |> DateTime.to_unix()
      postfix = "#{9_999_999_999 - timestamp}"
      object_name = "#{format_name_prefix(opts)}-#{postfix}"

      binary_contents = Jason.encode!(contents)

      ExAws.S3.put_object(bucket_name, object_name, binary_contents)
      |> ExAws.request!()
    end)

    contents
  end

  def fetch_latest_snapshot(opts = %{bucket_name: bucket_name}) do
    object_stream =
      ExAws.S3.list_objects(bucket_name, prefix: format_name_prefix(opts))
      |> ExAws.stream!()

    case object_stream |> Enum.take(1) do
      [%{key: latest_key}] ->
        %{body: body} =
          ExAws.S3.get_object(bucket_name, latest_key)
          |> ExAws.request!()

        Poison.decode!(body)

      [] ->
        :error_on_missing_cache
    end
  end

  def format_name_prefix(%{table: table, base: base}) do
    slugified_table = table |> String.downcase() |> String.replace(" ", "_", global: true)
    "#{slugified_table}@#{base}"
  end
end
