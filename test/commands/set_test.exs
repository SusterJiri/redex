defmodule Commands.SetTest do
  use ExUnit.Case

  setup do
    # Clean up any existing table
    if :ets.whereis(:redis_store) != :undefined do
      :ets.delete(:redis_store)
    end

    Store.setup_store()
    :ok
  end

  describe "basic SET command" do
    test "SET with key and value returns OK" do
      assert {:ok, "+OK\r\n"} = Commands.Set.execute(["mykey", "myvalue"])
    end

    test "SET stores the value correctly" do
      Commands.Set.execute(["testkey", "testvalue"])
      assert {:ok, "testvalue"} = Store.get("testkey")
    end

    test "SET with wrong number of arguments returns error" do
      assert {:error, _} = Commands.Set.execute(["only_key"])
      assert {:error, _} = Commands.Set.execute([])
      assert {:error, _} = Commands.Set.execute(["key", "value", "extra"])
    end
  end

  describe "SET with PX (milliseconds)" do
    test "SET with PX stores value with TTL" do
      assert {:ok, "+OK\r\n"} = Commands.Set.execute(["ttl_key", "ttl_value", "PX", "1000"])
      assert {:ok, "ttl_value"} = Store.get("ttl_key")
    end

    test "SET with px (lowercase) works" do
      assert {:ok, "+OK\r\n"} = Commands.Set.execute(["ttl_key", "ttl_value", "px", "1000"])
    end

    test "SET with PX and invalid TTL returns error" do
      assert {:error, "Invalid TTL value"} = Commands.Set.execute(["key", "value", "PX", "abc"])
      assert {:error, "Invalid TTL value"} = Commands.Set.execute(["key", "value", "PX", "-100"])
      assert {:error, "Invalid TTL value"} = Commands.Set.execute(["key", "value", "PX", "0"])
    end

    test "SET with PX and short TTL expires correctly" do
      Commands.Set.execute(["expire_key", "expire_value", "PX", "50"])
      assert {:ok, "expire_value"} = Store.get("expire_key")

      Process.sleep(100)
      assert {:error, :not_found} = Store.get("expire_key")
    end
  end

  describe "SET with EX (seconds)" do
    test "SET with EX stores value with TTL in seconds" do
      assert {:ok, "+OK\r\n"} = Commands.Set.execute(["ttl_key", "ttl_value", "EX", "1"])
      assert {:ok, "ttl_value"} = Store.get("ttl_key")
    end

    test "SET with ex (lowercase) works" do
      assert {:ok, "+OK\r\n"} = Commands.Set.execute(["ttl_key", "ttl_value", "ex", "1"])
    end

    test "SET with EX converts seconds to milliseconds" do
      # We can't easily test the exact conversion, but we can test behavior
      Commands.Set.execute(["ttl_key", "ttl_value", "EX", "1"])

      # Should still be available after 500ms (less than 1 second)
      Process.sleep(500)
      assert {:ok, "ttl_value"} = Store.get("ttl_key")
    end
  end

  describe "SET with invalid time units" do
    test "SET with invalid time unit returns error" do
      assert {:error, "Invalid time unit: INVALID"} =
        Commands.Set.execute(["key", "value", "INVALID", "100"])

      assert {:error, "Invalid time unit: MINUTES"} =
        Commands.Set.execute(["key", "value", "MINUTES", "5"])
    end
  end
end
