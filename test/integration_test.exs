defmodule RedisIntegrationTest do
  use ExUnit.Case

  setup do
    # Clean up any existing table
    if :ets.whereis(:redis_store) != :undefined do
      :ets.delete(:redis_store)
    end

    Store.setup_store()
    :ok
  end

  describe "end-to-end command execution" do
    test "PING command" do
      input = ["*1", "$4", "PING"]
      assert {:ok, ["PING"]} = Parser.parse_command(input)
      assert {:ok, "+PONG\r\n"} = Commands.Ping.execute([])
    end

    test "ECHO command flow" do
      input = ["*2", "$4", "ECHO", "$11", "Hello World"]
      assert {:ok, ["ECHO", "Hello World"]} = Parser.parse_command(input)
      assert {:ok, "$11\r\nHello World\r\n"} = Commands.Echo.execute(["Hello World"])
    end

    test "SET and GET flow" do
      # SET command
      set_input = ["*3", "$3", "SET", "$5", "mykey", "$7", "myvalue"]
      assert {:ok, ["SET", "mykey", "myvalue"]} = Parser.parse_command(set_input)
      assert {:ok, "+OK\r\n"} = Commands.Set.execute(["mykey", "myvalue"])

      # GET command
      get_input = ["*2", "$3", "GET", "$5", "mykey"]
      assert {:ok, ["GET", "mykey"]} = Parser.parse_command(get_input)
      assert {:ok, "$7\r\nmyvalue\r\n"} = Commands.Get.execute(["mykey"])
    end

    test "SET with TTL and GET flow" do
      # SET with PX
      set_input = ["*5", "$3", "SET", "$6", "ttlkey", "$8", "ttlvalue", "$2", "PX", "$4", "1000"]
      assert {:ok, ["SET", "ttlkey", "ttlvalue", "PX", "1000"]} = Parser.parse_command(set_input)
      assert {:ok, "+OK\r\n"} = Commands.Set.execute(["ttlkey", "ttlvalue", "PX", "1000"])

      # GET immediately (should work)
      get_input = ["*2", "$3", "GET", "$6", "ttlkey"]
      assert {:ok, ["GET", "ttlkey"]} = Parser.parse_command(get_input)
      assert {:ok, "$8\r\nttlvalue\r\n"} = Commands.Get.execute(["ttlkey"])
    end

    test "SET with short TTL and expiry" do
      # SET with very short TTL
      Commands.Set.execute(["expire_key", "expire_value", "PX", "50"])

      # Should exist initially
      assert {:ok, "$12\r\nexpire_value\r\n"} = Commands.Get.execute(["expire_key"])

      # Wait for expiry
      Process.sleep(100)

      # Should be gone
      assert {:ok, "$-1\r\n"} = Commands.Get.execute(["expire_key"])
    end

    test "mixed permanent and TTL keys" do
      # Set permanent key
      Commands.Set.execute(["permanent", "forever"])

      # Set TTL key
      Commands.Set.execute(["temporary", "short_lived", "PX", "50"])

      # Both should exist
      assert {:ok, "$7\r\nforever\r\n"} = Commands.Get.execute(["permanent"])
      assert {:ok, "$11\r\nshort_lived\r\n"} = Commands.Get.execute(["temporary"])

      # Wait for TTL expiry
      Process.sleep(100)

      # Only permanent should remain
      assert {:ok, "$7\r\nforever\r\n"} = Commands.Get.execute(["permanent"])
      assert {:ok, "$-1\r\n"} = Commands.Get.execute(["temporary"])
    end
  end

  describe "error handling integration" do
    test "invalid command parsing errors" do
      # Invalid RESP format
      invalid_input = ["#2", "$4", "PING"]
      assert {:error, _reason} = Parser.parse_command(invalid_input)
    end

    test "command execution errors" do
      # Wrong number of arguments
      assert {:error, _reason} = Commands.Get.execute([])
      assert {:error, _reason} = Commands.Set.execute(["only_key"])
      assert {:error, _reason} = Commands.Echo.execute(["arg1", "arg2"])
      assert {:error, _reason} = Commands.Ping.execute(["unexpected_arg"])
    end

    test "invalid TTL values" do
      assert {:error, "Invalid TTL value"} = Commands.Set.execute(["key", "value", "PX", "abc"])
      assert {:error, "Invalid TTL value"} = Commands.Set.execute(["key", "value", "PX", "-100"])
      assert {:error, "Invalid time unit: INVALID"} = Commands.Set.execute(["key", "value", "INVALID", "100"])
    end
  end
end
