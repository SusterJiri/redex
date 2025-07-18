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
    _ = Store.setup_store()

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
        case Parser.parse_command(split_data) do
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

  defp execute_command(command, args) do
    command = String.upcase(command)
    IO.puts("Executing command: #{command} with args: #{inspect(args)}")
    case command do
      "ECHO" -> Commands.echo_command(args)
      "PING" -> Commands.ping_command()
      "SET" -> Commands.set_command(args)
      "GET" -> Commands.get_command(args)
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
