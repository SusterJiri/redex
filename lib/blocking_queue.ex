defmodule BlockingQueue do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(_) do
    {:ok, %{}}
  end

  def handle_cast({:add_blocked_client, key, client_pid, socket}, state) do
    timestamp = :os.system_time(:millisecond)
    client_info = {client_pid, socket, timestamp}
    IO.puts("BlockingQueue: Adding blocked client #{inspect(client_pid)} for key '#{key}'")

    new_state =
      Map.update(state, key, [client_info], fn clients ->
        clients ++ [client_info]
      end)

    IO.puts("BlockingQueue: State now has #{map_size(new_state)} keys, key '#{key}' has #{length(Map.get(new_state, key, []))} clients")
    {:noreply, new_state}
  end

  def handle_cast({:notify_client, key}, state) do
    IO.puts("BlockingQueue: notify_client called for key '#{key}'")
    case Map.get(state, key) do
      nil ->
        IO.puts("BlockingQueue: No clients waiting for key '#{key}'")
        {:noreply, state}

      [] ->
        IO.puts("BlockingQueue: Empty client list for key '#{key}'")
        {:noreply, state}

      [first_client | remaining_clients] ->
        # Try to LPOP an element for the blocked client
        case Store.lpop(key) do
          {:ok, :not_found} ->
            # No elements available, keep waiting
            {:noreply, state}

          {:ok, element} ->
            # Got an element! Send it to the blocked client
            {_client_pid, socket, _timestamp} = first_client

            # Format BLPOP response: [key, element]
            key_len = byte_size(key)
            element_len = byte_size(element)
            response = "*2\r\n$#{key_len}\r\n#{key}\r\n$#{element_len}\r\n#{element}\r\n"

            # Send response to the client
            :gen_tcp.send(socket, response)

            # Update state: remove first client, keep the rest
            new_state = case remaining_clients do
              [] -> Map.delete(state, key)  # No more clients waiting
              _ -> Map.put(state, key, remaining_clients)  # Other clients still waiting
            end

            {:noreply, new_state}

          {:error, _reason} ->
            # Error with LPOP, keep waiting
            {:noreply, state}
        end
    end
  end

  def handle_cast({:remove_client, client_pid}, state) do
    # Remove this client from all keys
    new_state =
      state
      |> Enum.map(fn {key, clients} ->
        filtered_clients = Enum.reject(clients, fn {pid, _socket, _timestamp} -> pid == client_pid end)
        {key, filtered_clients}
      end)
      |> Enum.reject(fn {_key, clients} -> clients == [] end)
      |> Map.new()

    {:noreply, new_state}
  end

  def add_blocked_client(key, client_pid, socket) do
    GenServer.cast(__MODULE__, {:add_blocked_client, key, client_pid, socket})
  end

  def notify_client(key) do
    GenServer.cast(__MODULE__, {:notify_client, key})
  end

  def remove_client(client_pid) do
    GenServer.cast(__MODULE__, {:remove_client, client_pid})
  end

end
