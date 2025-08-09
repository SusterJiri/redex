defmodule Commands.Xrange do
  @behaviour RedisCommand

  @impl RedisCommand
  def execute([stream, start, end_range]) do
    case Store.xrange(stream, start, end_range) do
      {:ok, response} -> {:ok, encode_response(response)}
      {:error, reason} -> {:error, reason}
    end
  end

  def execute(_) do
    {:error, "XRANGE command expects at least stream, start, and end arguments"}
  end

  defp encode_response(response) do
    number_of_entries = length(response)
    start_string = "*#{number_of_entries}\r\n"

    entry_strings =
      Enum.map(response, fn {id, field_value_pairs} ->
        # Flatten field-value pairs into a list of strings
        flattened_fields =
          Enum.flat_map(field_value_pairs, fn {field, value} ->
            [field, value]
          end)

        # Each entry is an array of 2 elements: [id, [field1, value1, field2, value2, ...]]
        field_count = length(flattened_fields)

        # Format: *2 (array of 2), then id, then array of fields
        "*2\r\n" <>
          "$#{byte_size(id)}\r\n#{id}\r\n" <>
          "*#{field_count}\r\n" <>
          Enum.map_join(flattened_fields, "", fn field_or_value ->
            "$#{byte_size(field_or_value)}\r\n#{field_or_value}\r\n"
          end)
      end)

    start_string <> Enum.join(entry_strings, "")
  end
end
