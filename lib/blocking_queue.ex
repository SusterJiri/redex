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
        IO.puts("BlockingQueue: Clients for '#{key}' are: #{inspect(clients)}")

        # Handle both BLPOP (4-tuple) and XREAD (6-tuple) clients
        found_client =
          Enum.find(clients, fn
            # BLPOP client format
            {pid, _socket, _timestamp, _timer_ref} when pid == client_pid ->
              true

            # XREAD client format
            {pid, _socket, _timestamp, _timer_ref, :xread, _stream_info} when pid == client_pid ->
              true

            # No match
            _ ->
              false
          end)

        case found_client do
          nil ->
            # Client not found (maybe already served)
            {:noreply, state}

          _client_tuple ->
            # Send timeout message to the client process
            send(client_pid, {:timeout})

            # Remove the client (handle both formats)
            remaining_clients =
              Enum.reject(clients, fn
                # BLPOP client format
                {pid, _socket, _timestamp, _timer_ref} when pid == client_pid ->
                  true

                # XREAD client format
                {pid, _socket, _timestamp, _timer_ref, :xread, _stream_info}
                when pid == client_pid ->
                  true

                # Keep other clients
                _ ->
                  false
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
    timestamp = :os.system_time(:microsecond)

    IO.puts(
      "BlockingQueue: Adding client #{inspect(client_pid)} for key '#{key}' with timeout #{timeout}"
    )

    IO.puts("BlockingQueue: Timeout type: #{inspect(timeout)}")
    IO.puts("BlockingQueue: Timeout == 0? #{timeout == 0}")
    IO.puts("BlockingQueue: Timeout == 0.0? #{timeout == 0.0}")

    client_info =
      if timeout == 0 || timeout == 0.0 do
        # No timeout - wait forever
        IO.puts("BlockingQueue: Client will wait forever (no timeout)")
        {client_pid, socket, timestamp, nil}
      else
        # Set timeout timer
        timeout_ms = round(timeout * 1000)
        IO.puts("Key: #{key}")
        timer_ref = Process.send_after(self(), {:timeout, key, client_pid}, timeout_ms)
        IO.puts("BlockingQueue: Client will timeout in #{timeout_ms}ms")
        {client_pid, socket, timestamp, timer_ref}
      end

    new_state =
      Map.update(state, key, [client_info], fn clients ->
        # Simple append - FIFO order naturally maintained
        updated_clients = clients ++ [client_info]

        IO.puts(
          "BlockingQueue: Clients for '#{key}' now: #{inspect(Enum.map(updated_clients, fn {pid, _, ts, _} -> {pid, ts} end))}"
        )

        updated_clients
      end)

    {:noreply, new_state}
  end

  def handle_cast({:add_blocked_client, stream_keys, ids, client_pid, socket, timeout}, state) do
    timestamp = :os.system_time(:microsecond)

    IO.puts(
      "BlockingQueue: Adding XREAD client #{inspect(client_pid)} for streams #{inspect(stream_keys)} with timeout #{timeout}"
    )

    # Convert timeout to number if it's a string
    timeout_num =
      case timeout do
        timeout when is_binary(timeout) ->
          case Integer.parse(timeout) do
            {num, ""} -> num
            _ -> 0
          end

        timeout when is_number(timeout) ->
          timeout
      end

    IO.puts("BlockingQueue: Timeout type: #{inspect(timeout_num)}")
    IO.puts("BlockingQueue: Timeout == 0? #{timeout_num == 0}")
    IO.puts("BlockingQueue: Timeout == 0.0? #{timeout_num == 0.0}")

    client_info =
      if timeout_num == 0 || timeout_num == 0.0 do
        # No timeout - wait forever
        IO.puts("BlockingQueue: Client will wait forever (no timeout)")
        {client_pid, socket, timestamp, nil, :xread, {stream_keys, ids}}
      else
        # Set timeout timer
        # Now this will work
        timeout_ms = round(timeout_num)
        key = List.first(stream_keys)
        IO.puts("Key: #{key}")
        timer_ref = Process.send_after(self(), {:timeout, key, client_pid}, timeout_ms)
        IO.puts("BlockingQueue: Client will timeout in #{timeout_ms}ms")
        {client_pid, socket, timestamp, timer_ref, :xread, {stream_keys, ids}}
      end

    # Register the client for ALL stream keys they're watching
    new_state =
      Enum.reduce(stream_keys, state, fn stream_key, acc_state ->
        Map.update(acc_state, stream_key, [client_info], fn clients ->
          updated_clients = clients ++ [client_info]
          IO.puts("BlockingQueue: Clients for '#{stream_key}' now: #{length(updated_clients)}")
          updated_clients
        end)
      end)

    {:noreply, new_state}
  end

  def handle_cast({:notify_client, key}, state) do
    case Map.get(state, key) do
      nil ->
        {:noreply, state}

      [] ->
        {:noreply, state}

      [first_client | remaining_clients] ->
        # Check the client type (4-tuple for BLPOP, 6-tuple for XREAD)
        case first_client do
          # BLPOP client (original format)
          {_client_pid, socket, _timestamp, timer_ref} ->
            handle_blpop_client(key, first_client, remaining_clients, state)

          # XREAD client (new format with operation type and stream positions)
          {client_pid, socket, _timestamp, timer_ref, :xread, stream_positions} ->
            handle_xread_client(key, first_client, remaining_clients, state)

          # Handle any other format gracefully
          _ ->
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

  defp handle_xread_client(
         stream_key,
         {client_pid, socket, _timestamp, timer_ref, :xread, {stream_keys, ids}},
         remaining_clients,
         state
       ) do
    # Find the position for this specific stream
    stream_index = Enum.find_index(stream_keys, &(&1 == stream_key))

    case stream_index do
      nil ->
        # This client wasn't watching this stream, keep waiting
        {:noreply,
         Map.put(state, stream_key, [
           {client_pid, socket, _timestamp, timer_ref, :xread, {stream_keys, ids}}
           | remaining_clients
         ])}

      index ->
        stream_position = Enum.at(ids, index)

        # For $ in blocking XREAD, we need to check if there are any entries
        # newer than what existed when the command was issued
        actual_position =
          if stream_position == "$" do
            # Get the current max ID from the stream at the time of notification
            # This ensures we only get entries added after the XREAD was issued
            case Store.get_max_id_from_stream(stream_key) do
              # Stream was empty when XREAD was issued
              {0, 0} -> "0-0"
              {ts, seq} -> "#{ts}-#{seq}"
            end
          else
            stream_position
          end

        # Check only this stream for new entries using Store.xread with single stream
        case Store.xread([stream_key], [actual_position]) do
          {:ok, []} ->
            # No new entries, keep waiting
            {:noreply,
             Map.put(state, stream_key, [
               {client_pid, socket, _timestamp, timer_ref, :xread, {stream_keys, ids}}
               | remaining_clients
             ])}

          {:ok, response} ->
            # Got new entries! Send them to the blocked client
            if timer_ref, do: Process.cancel_timer(timer_ref)

            # Encode and send response
            encoded_response = encode_response(response)
            send(client_pid, {:unblock, encoded_response})

            # Remove this client from ALL streams they were watching
            new_state = remove_client_from_all_streams(client_pid, state)

            {:noreply, new_state}

          {:error, _reason} ->
            # Error checking stream, keep waiting
            {:noreply,
             Map.put(state, stream_key, [
               {client_pid, socket, _timestamp, timer_ref, :xread, {stream_keys, ids}}
               | remaining_clients
             ])}
        end
    end
  end

  defp handle_blpop_client(
         key,
         {_client_pid, socket, _timestamp, timer_ref},
         remaining_clients,
         state
       ) do
    # Your existing BLPOP logic
    case Store.lpop(key) do
      {:ok, :not_found} ->
        # No elements available, keep waiting
        {:noreply, state}

      {:ok, element} ->
        # Got an element! Send it to the blocked client
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

  def add_blocked_client(key, client_pid, socket, timeout \\ 0) do
    IO.puts(
      "BlockingQueue.add_blocked_client called with: key=#{key}, pid=#{inspect(client_pid)}, timeout=#{timeout}"
    )

    result = GenServer.cast(__MODULE__, {:add_blocked_client, key, client_pid, socket, timeout})
    IO.puts("GenServer.cast result: #{inspect(result)}")
    result
  end

  def add_blocked_client(stream_keys, ids, client_pid, socket, timeout) do
    IO.puts(
      "BlockingQueue.add_blocked_client called with: stream_keys=#{inspect(stream_keys)}, ids=#{inspect(ids)}, pid=#{inspect(client_pid)}, timeout=#{timeout}"
    )

    result =
      GenServer.cast(
        __MODULE__,
        {:add_blocked_client, stream_keys, ids, client_pid, socket, timeout}
      )

    IO.puts("GenServer.cast result: #{inspect(result)}")
    result
  end

  def handle_call({:has_blocked_clients, key}, _from, state) do
    has_clients =
      case Map.get(state, key) do
        nil -> false
        [] -> false
        _clients -> true
      end

    {:reply, has_clients, state}
  end

  def has_blocked_clients(key) do
    GenServer.call(__MODULE__, {:has_blocked_clients, key})
  end

  def notify_client(key) do
    GenServer.cast(__MODULE__, {:notify_client, key})
  end

  def remove_client(client_pid) do
    GenServer.cast(__MODULE__, {:remove_client, client_pid})
  end

  defp remove_client_from_all_streams(client_pid, state) do
    state
    |> Enum.map(fn {stream_key, clients} ->
      filtered_clients =
        Enum.reject(clients, fn
          # Handle BLPOP clients (4-tuple)
          {pid, _socket, _timestamp, timer_ref} when pid == client_pid ->
            if timer_ref, do: Process.cancel_timer(timer_ref)
            true

          # Handle XREAD clients (6-tuple)
          {pid, _socket, _timestamp, timer_ref, :xread, _stream_info} when pid == client_pid ->
            if timer_ref, do: Process.cancel_timer(timer_ref)
            true

          # Keep other clients
          _ ->
            false
        end)

      {stream_key, filtered_clients}
    end)
    # Remove empty stream entries
    |> Enum.reject(fn {_stream_key, clients} -> clients == [] end)
    |> Map.new()
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
