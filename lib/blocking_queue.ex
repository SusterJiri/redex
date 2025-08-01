defmodule BlockingQueue do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec init(any()) :: {:ok, %{}}
  def init(_) do
    {:ok, %{}}
  end

  def handle_info({:timeout, key, client_pid}, state) do
    # Find and remove the timed-out client
    case Map.get(state, key) do
      nil ->
        {:noreply, state}

      clients ->
        case Enum.find(clients, fn {pid, _socket, _timestamp, _timer_ref} -> pid == client_pid end) do
          nil ->
            # Client not found (maybe already served)
            {:noreply, state}

          {_pid, socket, _timestamp, _timer_ref} ->
            # Send timeout response and remove client
            :gen_tcp.send(socket, "$-1\r\n")

            remaining_clients =
              Enum.reject(clients, fn {pid, _socket, _timestamp, _timer_ref} ->
                pid == client_pid
              end)

            new_state =
              case remaining_clients do
                [] -> Map.delete(state, key)
                _ -> Map.put(state, key, remaining_clients)
              end

            {:noreply, new_state}
        end
    end
  end

  def handle_cast({:add_blocked_client, key, client_pid, socket, timeout}, state) do
    timestamp = :os.system_time(:millisecond)
    IO.puts("BlockingQueue: Adding client #{inspect(client_pid)} for key '#{key}' with timeout #{timeout}")

    if timeout == 0 || timeout == 0.0 do
      # No timeout - wait forever
      IO.puts("BlockingQueue: Client will wait forever (no timeout)")
      client_info = {client_pid, socket, timestamp, nil}

      new_state =
        Map.update(state, key, [], fn clients ->
          # Add client and sort by timestamp to ensure FIFO order
          updated_clients = (clients ++ [client_info])
          |> Enum.sort_by(fn {_pid, _socket, timestamp, _timer_ref} -> timestamp end)

          IO.puts("BlockingQueue: Clients for '#{key}' now: #{inspect(Enum.map(updated_clients, fn {pid, _, ts, _} -> {pid, ts} end))}")
          updated_clients
        end)

      {:noreply, new_state}
    else
      # Set timeout timer
      timeout_ms = round(timeout * 1000)
      timer_ref = Process.send_after(self(), {:timeout, key, client_pid}, timeout_ms)
      client_info = {client_pid, socket, timestamp, timer_ref}

      new_state =
        Map.update(state, key, [client_info], fn clients ->
          # Add client and sort by timestamp to ensure FIFO order
          (clients ++ [client_info])
          |> Enum.sort_by(fn {_pid, _socket, timestamp, _timer_ref} -> timestamp end)
        end)

      {:noreply, new_state}
    end
  end

  def handle_cast({:notify_client, key}, state) do
    case Map.get(state, key) do
      nil ->
        {:noreply, state}

      [] ->
        {:noreply, state}

      [first_client | remaining_clients] ->
        # Try to LPOP an element for the blocked client
        case Store.lpop(key) do
          {:ok, :not_found} ->
            # No elements available, keep waiting
            {:noreply, state}

          {:ok, element} ->
            # Got an element! Send it to the blocked client
            {_client_pid, socket, _timestamp, timer_ref} = first_client

            # Cancel the timeout timer if it exists
            if timer_ref, do: Process.cancel_timer(timer_ref)

            # Format BLPOP response: [key, element]
            key_len = byte_size(key)
            element_len = byte_size(element)
            response = "*2\r\n$#{key_len}\r\n#{key}\r\n$#{element_len}\r\n#{element}\r\n"

            # Send response to the client
            :gen_tcp.send(socket, response)

            # Update state: remove first client, keep the rest
            new_state =
              case remaining_clients do
                # No more clients waiting
                [] -> Map.delete(state, key)
                # Other clients still waiting
                _ -> Map.put(state, key, remaining_clients)
              end

            {:noreply, new_state}

          {:error, _reason} ->
            # Error with LPOP, keep waiting
            {:noreply, state}
        end
    end
  end

  def handle_cast({:remove_client, client_pid}, state) do
    # Remove this client from all keys and cancel their timers
    new_state =
      state
      |> Enum.map(fn {key, clients} ->
        filtered_clients =
          Enum.reject(clients, fn {pid, _socket, _timestamp, timer_ref} ->
            if pid == client_pid do
              # Cancel timer before removing
              if timer_ref, do: Process.cancel_timer(timer_ref)
              true
            else
              false
            end
          end)

        {key, filtered_clients}
      end)
      |> Enum.reject(fn {_key, clients} -> clients == [] end)
      |> Map.new()

    {:noreply, new_state}
  end

  def add_blocked_client(key, client_pid, socket, timeout \\ 0) do
    GenServer.cast(__MODULE__, {:add_blocked_client, key, client_pid, socket, timeout})
  end

  def notify_client(key) do
    GenServer.cast(__MODULE__, {:notify_client, key})
  end

  def remove_client(client_pid) do
    GenServer.cast(__MODULE__, {:remove_client, client_pid})
  end
end
