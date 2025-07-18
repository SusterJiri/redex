defmodule ParserTest do
  use ExUnit.Case

  describe "RESP protocol parsing" do
    test "parse simple command" do
      input = ["*1", "$4", "PING"]
      assert {:ok, ["PING"]} = Parser.parse_command(input)
    end

    test "parse command with arguments" do
      input = ["*2", "$4", "ECHO", "$11", "Hello World"]
      assert {:ok, ["ECHO", "Hello World"]} = Parser.parse_command(input)
    end

    test "parse SET command" do
      input = ["*3", "$3", "SET", "$5", "mykey", "$7", "myvalue"]
      assert {:ok, ["SET", "mykey", "myvalue"]} = Parser.parse_command(input)
    end

    test "parse SET command with TTL" do
      input = ["*5", "$3", "SET", "$5", "mykey", "$7", "myvalue", "$2", "PX", "$4", "1000"]
      assert {:ok, ["SET", "mykey", "myvalue", "PX", "1000"]} = Parser.parse_command(input)
    end

    test "parse command with empty string argument" do
      input = ["*2", "$4", "ECHO", "$0", ""]
      assert {:ok, ["ECHO", ""]} = Parser.parse_command(input)
    end
  end

  describe "validation and error handling" do
    test "invalid array header returns error" do
      input = ["#2", "$4", "PING"]
      assert {:error, _} = Parser.parse_command(input)
    end

    test "mismatched argument count returns error" do
      input = ["*2", "$4", "PING"]  # Says 2 args but only provides 1
      assert {:error, _} = Parser.parse_command(input)
    end

    test "size mismatch returns error" do
      input = ["*2", "$4", "ECHO", "$5", "hi"]  # Says 5 bytes but "hi" is 2 bytes
      assert {:error, _} = Parser.parse_command(input)
    end

    test "invalid bulk string header returns error" do
      input = ["*2", "#4", "ECHO", "$5", "hello"]
      assert {:error, _} = Parser.parse_command(input)
    end

    test "empty input returns error" do
      assert {:error, _} = Parser.parse_command([])
    end
  end

  describe "edge cases" do
    test "handles unicode characters correctly" do
      emoji = "🎉"
      byte_size_emoji = byte_size(emoji)  # Should be 4 for emoji
      input = ["*2", "$4", "ECHO", "$#{byte_size_emoji}", emoji]
      assert {:ok, ["ECHO", ^emoji]} = Parser.parse_command(input)
    end

    test "handles large argument count" do
      # Generate a command with many arguments
      args = Enum.map(1..10, fn i -> "arg#{i}" end)
      arg_count = length(args) + 1  # +1 for command name

      input = ["*#{arg_count}", "$4", "TEST"] ++
              Enum.flat_map(args, fn arg -> ["$#{byte_size(arg)}", arg] end)

      expected = ["TEST"] ++ args
      assert {:ok, ^expected} = Parser.parse_command(input)
    end
  end
end
