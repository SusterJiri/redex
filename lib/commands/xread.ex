defmodule Commands.Xread do
  @behaviour RedisCommand

  @impl RedisCommand
  def execute([streams_keyword | keys_ids]) do
    case parse_streams([streams_keyword | keys_ids]) do
      {:error, reason} ->
        {:error, reason}

      {stream_keys, ids} ->
        case Store.xread(stream_keys, ids) do
          {:ok, response} -> {:ok, encode_response(response)}
        end
    end
  end

  def execute(_) do
    {:error, "XREAD command expects at least stream, start, and end arguments"}
  end

  defp parse_streams([streams_keyword | keys_ids]) do
    # Validate and parse the streams
    if String.downcase(streams_keyword) == "streams" do
      keys_ids_len = length(keys_ids)

      {keys, ids} = Enum.split(keys_ids, div(keys_ids_len, 2))
      {keys, ids}
    else
      {:error, "Invalid XREAD syntax"}
    end
  end

  defp encode_response(response) do
    # response should be: [{"stream_key", [entries]}, ...]
    number_of_streams = length(response)
    start_string = "*#{number_of_streams}\r\n"

    IO.inspect(response, label: "XREAD response streams")

    stream_strings =
      Enum.map(response, fn {stream_key, entries} ->
        # Each stream result is an array of 2 elements: [stream_key, [entries]]
        number_of_entries = length(entries)

        "*2\r\n" <>
          "$#{byte_size(stream_key)}\r\n#{stream_key}\r\n" <>
          "*#{number_of_entries}\r\n" <>
          encode_entries(entries)
      end)

    start_string <> Enum.join(stream_strings, "")
  end

  defp encode_entries(entries) do
    Enum.map_join(entries, "", fn {entry_id, field_value_pairs} ->
      # Flatten field-value pairs into a list of strings
      flattened_fields =
        Enum.flat_map(field_value_pairs, fn {field, value} ->
          [field, value]
        end)

      field_count = length(flattened_fields)

      # Each entry is an array of 2 elements: [id, [field1, value1, field2, value2, ...]]
      "*2\r\n" <>
        "$#{byte_size(entry_id)}\r\n#{entry_id}\r\n" <>
        "*#{field_count}\r\n" <>
        Enum.map_join(flattened_fields, "", fn field_or_value ->
          "$#{byte_size(field_or_value)}\r\n#{field_or_value}\r\n"
        end)
    end)
  end
end
