defmodule AirtableSnapshot do
  require Logger

  def fetch(%{key: key, table: table, base: base} = opts)
      when is_binary(key) and is_binary(table) and is_binary(base) do
    if Map.has_key?(opts, :filter_records) or Map.has_key?(opts, :process_records) do
      raise """
      `:filter_records` and `:process_records` options have been removed.
      use Enum.map/2, Enum.filter/2 on the results of this function instead.
      """
    end

    bucket_name = Map.get(opts, :bucket_name, "dialer-airtable-snapshots")

    fetch_records(%{
      key: key,
      table: table,
      base: base,
      bucket_name: bucket_name
    })
  end

  defp fetch_records(opts, prev_records \\ [], offset \\ 0) do
    %{
      key: key,
      table: table,
      base: base
    } = opts

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
        Enum.concat(prev_records, records)
    end
  rescue
    error ->
      Logger.error("""
      Failed to fetch from Airtable key=#{key} table=#{table} base=#{base}
      #{inspect(error)}
      """)
  end
end
