#!/usr/bin/env elixir

defmodule RedisBenchmark do
  @moduledoc """
  Performance testing script for the Redis server implementation.

  Usage:
    elixir benchmark.exs

  Or with custom parameters:
    elixir benchmark.exs --operations 10000 --concurrent 10
  """

  def main(args \\ []) do
    opts = parse_args(args)

    IO.puts("=== Redis Server Performance Benchmark ===")
    IO.puts("Operations per test: #{opts.operations}")
    IO.puts("Concurrent connections: #{opts.concurrent}")
    IO.puts("Server: localhost:6379")
    IO.puts("")

    # Test if server is running
    case :gen_tcp.connect(~c"localhost", 6379, [:binary, active: false], 1000) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        IO.puts("✓ Server is running")
      {:error, reason} ->
        IO.puts("✗ Cannot connect to server: #{reason}")
        IO.puts("Please start your Redis server first with: ./your_program.sh")
        System.halt(1)
    end

    IO.puts("")

    # Run benchmarks
    run_ping_benchmark(opts)
    run_echo_benchmark(opts)
    run_set_get_benchmark(opts)
    run_rpush_benchmark(opts)
    run_mixed_workload_benchmark(opts)
    run_concurrent_benchmark(opts)

    IO.puts("\n=== Benchmark Complete ===")
  end

  defp parse_args(args) do
    defaults = %{operations: 1000, concurrent: 5}

    Enum.reduce(args, defaults, fn
      "--operations", acc -> acc
      "--concurrent", acc -> acc
      ops, acc when acc == defaults ->
        case Integer.parse(ops) do
          {num, ""} -> Map.put(acc, :operations, num)
          _ -> acc
        end
      conc, %{operations: _ops} = acc ->
        case Integer.parse(conc) do
          {num, ""} -> Map.put(acc, :concurrent, num)
          _ -> acc
        end
      _, acc -> acc
    end)
  end

  defp run_ping_benchmark(opts) do
    IO.puts("🏓 PING Benchmark")

    {time_us, _} = :timer.tc(fn ->
      Enum.each(1..opts.operations, fn _i ->
        send_command("PING")
      end)
    end)

    ops_per_sec = round(opts.operations / (time_us / 1_000_000))
    avg_latency = round(time_us / opts.operations)

    IO.puts("  Operations: #{opts.operations}")
    IO.puts("  Time: #{time_us / 1000} ms")
    IO.puts("  Ops/sec: #{ops_per_sec}")
    IO.puts("  Avg latency: #{avg_latency} μs")
    IO.puts("")
  end

  defp run_echo_benchmark(opts) do
    IO.puts("📢 ECHO Benchmark")

    messages = [
      "hello",
      "world",
      "this is a longer message for testing",
      "🚀 unicode test 🎉",
      String.duplicate("x", 100)
    ]

    {time_us, _} = :timer.tc(fn ->
      Enum.each(1..opts.operations, fn i ->
        message = Enum.at(messages, rem(i, length(messages)))
        send_command("ECHO", [message])
      end)
    end)

    ops_per_sec = round(opts.operations / (time_us / 1_000_000))
    avg_latency = round(time_us / opts.operations)

    IO.puts("  Operations: #{opts.operations}")
    IO.puts("  Time: #{time_us / 1000} ms")
    IO.puts("  Ops/sec: #{ops_per_sec}")
    IO.puts("  Avg latency: #{avg_latency} μs")
    IO.puts("")
  end

  defp run_set_get_benchmark(opts) do
    IO.puts("💾 SET/GET Benchmark")

    # Generate test data
    keys = Enum.map(1..opts.operations, &"key#{&1}")
    values = Enum.map(1..opts.operations, &"value#{&1}_#{:rand.uniform(1000)}")

    # SET benchmark
    {set_time_us, _} = :timer.tc(fn ->
      Enum.zip(keys, values)
      |> Enum.each(fn {key, value} ->
        send_command("SET", [key, value])
      end)
    end)

    # GET benchmark
    {get_time_us, _} = :timer.tc(fn ->
      Enum.each(keys, fn key ->
        send_command("GET", [key])
      end)
    end)

    set_ops_per_sec = round(opts.operations / (set_time_us / 1_000_000))
    get_ops_per_sec = round(opts.operations / (get_time_us / 1_000_000))

    IO.puts("  SET Operations: #{opts.operations}")
    IO.puts("  SET Time: #{set_time_us / 1000} ms")
    IO.puts("  SET Ops/sec: #{set_ops_per_sec}")
    IO.puts("  GET Operations: #{opts.operations}")
    IO.puts("  GET Time: #{get_time_us / 1000} ms")
    IO.puts("  GET Ops/sec: #{get_ops_per_sec}")
    IO.puts("")
  end

  defp run_rpush_benchmark(opts) do
    IO.puts("📝 RPUSH Benchmark")

    list_keys = ["list1", "list2", "list3"]
    values = ["apple", "banana", "cherry", "date", "elderberry"]

    {time_us, _} = :timer.tc(fn ->
      Enum.each(1..opts.operations, fn i ->
        key = Enum.at(list_keys, rem(i, length(list_keys)))
        value = Enum.at(values, rem(i, length(values)))
        send_command("RPUSH", [key, value])
      end)
    end)

    ops_per_sec = round(opts.operations / (time_us / 1_000_000))
    avg_latency = round(time_us / opts.operations)

    IO.puts("  Operations: #{opts.operations}")
    IO.puts("  Time: #{time_us / 1000} ms")
    IO.puts("  Ops/sec: #{ops_per_sec}")
    IO.puts("  Avg latency: #{avg_latency} μs")
    IO.puts("")
  end

  defp run_mixed_workload_benchmark(opts) do
    IO.puts("🔀 Mixed Workload Benchmark")

    operations = [
      {:ping, []},
      {:echo, ["test"]},
      {:set, ["mixed_key", "mixed_value"]},
      {:get, ["mixed_key"]},
      {:rpush, ["mixed_list", "item"]}
    ]

    {time_us, _} = :timer.tc(fn ->
      Enum.each(1..opts.operations, fn i ->
        {cmd, args} = Enum.at(operations, rem(i, length(operations)))
        send_command(String.upcase(Atom.to_string(cmd)), args)
      end)
    end)

    ops_per_sec = round(opts.operations / (time_us / 1_000_000))
    avg_latency = round(time_us / opts.operations)

    IO.puts("  Operations: #{opts.operations}")
    IO.puts("  Time: #{time_us / 1000} ms")
    IO.puts("  Ops/sec: #{ops_per_sec}")
    IO.puts("  Avg latency: #{avg_latency} μs")
    IO.puts("")
  end

  defp run_concurrent_benchmark(opts) do
    IO.puts("🚀 Concurrent Benchmark")

    # Divide operations among concurrent workers
    ops_per_worker = div(opts.operations, opts.concurrent)

    {time_us, _} = :timer.tc(fn ->
      tasks = Enum.map(1..opts.concurrent, fn worker_id ->
        Task.async(fn ->
          Enum.each(1..ops_per_worker, fn i ->
            key = "concurrent_#{worker_id}_#{i}"
            value = "value_#{worker_id}_#{i}"
            send_command("SET", [key, value])
            send_command("GET", [key])
          end)
        end)
      end)

      # Wait for all tasks to complete
      Enum.each(tasks, &Task.await(&1, :infinity))
    end)

    total_ops = opts.concurrent * ops_per_worker * 2  # SET + GET
    ops_per_sec = round(total_ops / (time_us / 1_000_000))

    IO.puts("  Concurrent connections: #{opts.concurrent}")
    IO.puts("  Operations per connection: #{ops_per_worker * 2}")
    IO.puts("  Total operations: #{total_ops}")
    IO.puts("  Time: #{time_us / 1000} ms")
    IO.puts("  Ops/sec: #{ops_per_sec}")
    IO.puts("")
  end

  defp send_command(command, args \\ []) do
    case :gen_tcp.connect(~c"localhost", 6379, [:binary, active: false], 5000) do
      {:ok, socket} ->
        # Build RESP command
        resp_command = build_resp_command([command | args])

        # Send command
        :gen_tcp.send(socket, resp_command)

        # Read response
        case :gen_tcp.recv(socket, 0, 5000) do
          {:ok, _response} ->
            :gen_tcp.close(socket)
            :ok
          {:error, reason} ->
            :gen_tcp.close(socket)
            {:error, reason}
        end
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp build_resp_command(parts) do
    count = length(parts)
    array_header = "*#{count}\r\n"

    bulk_strings = Enum.map(parts, fn part ->
      "$#{byte_size(part)}\r\n#{part}\r\n"
    end)

    array_header <> Enum.join(bulk_strings)
  end
end

# Run if called directly
if __ENV__.file == Path.absname(__ENV__.file) do
  RedisBenchmark.main(System.argv())
end
