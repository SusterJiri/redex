defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """
  use Application

  def start(_type, _args) do
    # Check if we should start the server (disabled in tests)
    start_server = Application.get_env(:codecrafters_redis, :start_server, true)

    if start_server do
      children = [
        {Task.Supervisor, name: Server.TaskSupervisor},
        Store.Cleaner,
        BlockingQueue,
        Supervisor.child_spec({Task, fn -> Server.listen() end}, restart: :permanent)
      ]

      Supervisor.start_link(children, strategy: :one_for_one)
    else
      # In test mode, just start the store setup without the server
      Store.setup_store()
      {:ok, self()}
    end
  end

  @doc """
  Listen for incoming connections
  """
  def listen() do
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    IO.puts("Logs from your program will appear here!")

    # Use active mode for truly async connection handling
    {:ok, socket} = :gen_tcp.listen(6379, [:binary, active: false, reuseaddr: true])
    _ = Store.setup_store()

    # Start multiple acceptor processes for parallel connection acceptance
    acceptor_count = System.schedulers_online()
    IO.puts("Starting #{acceptor_count} acceptor processes")

    Enum.each(1..acceptor_count, fn i ->
      spawn_link(fn -> loop_acceptor(socket, i) end)
    end)

    # Keep the main process alive
    Process.sleep(:infinity)
  end

  defp loop_acceptor(socket, acceptor_id) do
    case :gen_tcp.accept(socket) do
      {:ok, client} ->
        IO.puts("Acceptor #{acceptor_id} accepted connection")

        {:ok, pid} =
          Task.Supervisor.start_child(Server.TaskSupervisor, fn ->
            serve(client)
          end)

        :ok = :gen_tcp.controlling_process(client, pid)
        loop_acceptor(socket, acceptor_id)

      {:error, reason} ->
        IO.puts("Acceptor #{acceptor_id} error: #{reason}")
        # Wait a bit before retrying to avoid busy loop
        Process.sleep(100)
        loop_acceptor(socket, acceptor_id)
    end
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
                serve(socket)

              {:block, {key, timeout}} ->
                BlockingQueue.add_blocked_client(key, self(), socket, timeout)
                # Pass socket here
                wait_for_unblock(socket)

              {:block, {stream_keys, ids, timeout}} ->
                BlockingQueue.add_blocked_client(stream_keys, ids, self(), socket, timeout)
                # Pass socket here
                wait_for_unblock(socket)

              {:error, reason} ->
                error_response = "-ERR #{reason}\r\n"
                write_line(error_response, socket)
                serve(socket)
            end

          {:error, reason} ->
            error_response = "-ERR #{reason}\r\n"
            write_line(error_response, socket)
            serve(socket)
        end

      {:error, _data} ->
        # Client disconnected, remove from blocking queues
        BlockingQueue.remove_client(self())
        :gen_tcp.close(socket)
    end
  end

  defp wait_for_unblock(socket) do
    receive do
      {:unblock, response} ->
        # Send the response and continue serving
        write_line(response, socket)
        serve(socket)

      {:timeout} ->
        # Send timeout response and continue serving
        write_line("$-1\r\n", socket)
        serve(socket)
    end
  end

  defp read_line(socket) do
    {status, data} = :gen_tcp.recv(socket, 0)

    case status do
      :ok ->
        # Debugging output
        IO.puts("Received data: #{inspect(data)}")
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
      "ECHO" ->
        RedisCommand.echo_command(args)

      "PING" ->
        RedisCommand.ping_command()

      "SET" ->
        RedisCommand.set_command(args)

      "GET" ->
        RedisCommand.get_command(args)

      "RPUSH" ->
        RedisCommand.rpush_command(args)

      "LPUSH" ->
        RedisCommand.lpush_command(args)

      "LRANGE" ->
        RedisCommand.lrange_command(args)

      "LLEN" ->
        RedisCommand.llen_command(args)

      "LPOP" ->
        RedisCommand.lpop_command(args)

      "BLPOP" ->
        RedisCommand.blpop_command(args)

      "TYPE" ->
        RedisCommand.type_command(args)

      "XADD" ->
        RedisCommand.xadd_command(args)

      "XRANGE" ->
        RedisCommand.xrange_command(args)

      "XREAD" ->
        RedisCommand.xread_command(args)

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
