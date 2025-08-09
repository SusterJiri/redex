defmodule Commands.Xadd do
  @behaviour RedisCommand

  # @impl RedisCommand
  # def execute([stream, field_value_args]) when length(field_value_args) >= 2 do
  #   # Parse field-value pairs from remaining arguments
  #   case parse_field_value_pairs(field_value_args) do
  #     {:ok, field_value_pairs} ->
  #       case Store.xadd(stream, entry_id, field_value_pairs) do
  #         {:ok, response} ->
  #           {:ok, "$#{byte_size(response)}\r\n#{response}\r\n"}

  #         {:error, reason} ->
  #           {:error, reason}
  #       end

  #     {:error, reason} ->
  #       {:error, reason}
  #   end
  # end

  @impl RedisCommand
  def execute([stream, entry_id | field_value_args]) when length(field_value_args) >= 2 do
    with {timestamp, sequence} <- parse_timestamp_sequence(entry_id),
         {:ok, field_value_pairs} <- parse_field_value_pairs(field_value_args),
         {:ok, response} <- Store.xadd(stream, {timestamp, sequence}, field_value_pairs) do
      {:ok, "$#{byte_size(response)}\r\n#{response}\r\n"}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  def execute(_),
    do: {:error, "XADD command expects at least stream, entry_id, and one field-value pair"}

  # Parse field-value pairs from list of arguments
  # ["foo", "bar", "baz", "qux"] -> [{"foo", "bar"}, {"baz", "qux"}]
  defp parse_field_value_pairs(args) do
    if rem(length(args), 2) != 0 do
      {:error, "Field-value pairs must be even number of arguments"}
    else
      pairs =
        args
        |> Enum.chunk_every(2)
        |> Enum.map(fn [field, value] -> {field, value} end)

      {:ok, pairs}
    end
  end

  defp parse_timestamp_sequence(entry_id) do
    case String.split(entry_id, "-") do
      [timestamp, sequence] when is_binary(timestamp) and is_binary(sequence) ->
        {String.to_integer(timestamp), String.to_integer(sequence)}

      _ ->
        {:error, "Invalid entry ID format"}
    end
  end
end
