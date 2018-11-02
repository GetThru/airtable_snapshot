defmodule AirtableSnapshot do
  def fetch(%{
        key: key,
        table: table,
        base: base,
        filter_records: filter_records,
        process_records: process_records,
        bucket_name: bucket_name
      })
      when is_binary(key) and is_binary(table) and is_binary(base) and is_binary(bucket_name) do
    try do
      filtered_and_processed =
        fresh_fetch(%{
          key: key,
          table: table,
          base: base,
          filter_records: filter_records,
          process_records: process_records
        })

      store_snapshot(filtered_and_processed, %{
        table: table,
        base: base,
        bucket_name: bucket_name
      })

      filtered_and_processed
    rescue
      _error ->
        fetch_latest_snapshot(%{
          table: table,
          base: base,
          bucket_name: bucket_name
        })
    end
  end

  def fresh_fetch(
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
        fresh_fetch(opts, accumulated_records, next_offset)

      %{"records" => records} ->
        Enum.concat(prev_records, records)
        |> (fn rs -> filter_records.(rs) end).()
        |> (fn rs -> process_records.(rs) end).()
    end
  end

  def store_snapshot(contents, opts = %{bucket_name: bucket_name}) do
    timestamp = DateTime.utc_now() |> DateTime.to_unix()
    postfix = "#{9_999_999_999 - timestamp}"
    object_name = "#{format_name_prefix(opts)}-#{postfix}"

    binary_contents = Jason.encode!(contents)

    ExAws.S3.put_object(bucket_name, object_name, binary_contents)
    |> ExAws.request!()
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
