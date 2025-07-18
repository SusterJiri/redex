defmodule Parser do

  def parse_command([]), do: {:error, "Empty input"}

  def parse_command([array_header | rest]) do
    # Extract number of arguments from "*2" -> 2
    case String.slice(array_header, 0, 1) do
      "*" ->
        arg_count = array_header |> String.slice(1..-1//1) |> String.to_integer()
        IO.puts("Expecting #{arg_count} arguments")

        # Parse the arguments with validation
        case parse_arguments(rest, arg_count, []) do
          {:ok, args} ->
            IO.puts("Parsed command: #{inspect(args)}")
            {:ok, args}
          {:error, reason} ->
            IO.puts("Parse error: #{reason}")
            {:error, reason}
        end
      _ ->
        {:error, "Invalid RESP format: expected array"}
    end
  end

  defp parse_arguments([], 0, acc), do: {:ok, Enum.reverse(acc)}
  defp parse_arguments([], _remaining, _acc), do: {:error, "Not enough arguments"}

  defp parse_arguments([size_header, value | rest], remaining, acc) when remaining > 0 do
    case String.slice(size_header, 0, 1) do
      "$" ->
        expected_size = size_header |> String.slice(1..-1//1) |> String.to_integer()
        actual_size = byte_size(value)

        if actual_size == expected_size do
          IO.puts("Validated argument: '#{value}' (#{actual_size} bytes)")
          parse_arguments(rest, remaining - 1, [value | acc])
        else
          {:error, "Size mismatch: expected #{expected_size}, got #{actual_size} bytes"}
        end

      _ ->
        {:error, "Invalid RESP format: expected bulk string"}
    end
  end

  defp parse_arguments(_, _, _), do: {:error, "Invalid argument format"}
end
