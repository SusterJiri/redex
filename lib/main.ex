defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """
  use Application

  def start(_type, _args) do

    children = [
      {Task.Supervisor, name: Server.TaskSupervisor},
      Supervisor.child_spec({Task, fn -> Server.listen() end}, restart: :permanent),
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end

  @doc """
  Listen for incoming connections
  """
  def listen() do
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    IO.puts("Logs from your program will appear here!")

    # Uncomment this block to pass the first stage
    #
    # # Since the tester restarts your program quite often, setting SO_REUSEADDR
    # # ensures that we don't run into 'Address already in use' errors
    {:ok, socket} = :gen_tcp.listen(6379, [:binary, active: false, reuseaddr: true])

    loop_acceptor(socket)

  end


  defp loop_acceptor(socket) do
    {:ok, client} = :gen_tcp.accept(socket)
    {:ok, pid} = Task.Supervisor.start_child(Server.TaskSupervisor, fn ->
      serve(client)
    end)
    :ok = :gen_tcp.controlling_process(client, pid)
    loop_acceptor(socket)
  end

  defp serve(socket) do
    case read_line(socket) do
      {:ok, data} ->
        split_data = data |> String.split("\r\n") |> Enum.filter(&(&1 != ""))
        case parse_command(split_data) do
          {:ok, [command | args]} ->
            case execute_command(command, args) do
              {:ok, response} ->
                write_line(response, socket)
              {:error, reason} ->
                error_response = "-ERR #{reason}\r\n"
                write_line(error_response, socket)
            end
          {:error, reason} ->
            error_response = "-ERR #{reason}\r\n"
            write_line(error_response, socket)
        end
        serve(socket)
      {:error, data} ->
        :gen_tcp.close(socket)
        IO.puts("Connection closed due to #{inspect(data)}")
    end
  end

  defp read_line(socket) do
    {status, data} = :gen_tcp.recv(socket, 0)
    case status do
      :ok ->
        IO.puts("Received data: #{inspect(data)}") # Debugging output
        {:ok, data}
      :error ->
        {:error, data}
    end

  end

  defp write_line(line, socket) do
    :gen_tcp.send(socket, line)
  end

  defp parse_command([array_header | rest]) do
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

  defp execute_command(command, args) do
    command = String.upcase(command)
    IO.puts("Executing command: #{command} with args: #{inspect(args)}")
    case command do
      "ECHO" ->
        if length(args) == 1 do
          response = "$#{byte_size(hd(args))}\r\n#{hd(args)}\r\n"
          {:ok, response}
        else
          {:error, "ECHO command expects exactly one argument"}
        end
      "PING" ->
        {:ok, "+PONG\r\n"}
      _ ->
        {:error, "Unknown command: #{command}"}
    end
  end

end

defmodule CLI do
  def main(_args) do
    # Start the Server application
    {:ok, _pid} = Application.ensure_all_started(:codecrafters_redis)

    # Run forever
    Process.sleep(:infinity)
  end
end
