defmodule Commands.EchoTest do
  use ExUnit.Case

  describe "ECHO command" do
    test "ECHO with message returns message in RESP format" do
      assert {:ok, "$5\r\nhello\r\n"} = Commands.Echo.execute(["hello"])
    end

    test "ECHO with empty string" do
      assert {:ok, "$0\r\n\r\n"} = Commands.Echo.execute([""])
    end

    test "ECHO with unicode characters" do
      assert {:ok, "$4\r\n🎉\r\n"} = Commands.Echo.execute(["🎉"])
    end

    test "ECHO with no arguments returns error" do
      assert {:error, "ECHO command expects exactly one argument"} = Commands.Echo.execute([])
    end

    test "ECHO with multiple arguments returns error" do
      assert {:error, "ECHO command expects exactly one argument"} =
        Commands.Echo.execute(["hello", "world"])
    end

    test "ECHO calculates correct byte size" do
      message = "test message"
      expected_size = byte_size(message)
      expected_response = "$#{expected_size}\r\n#{message}\r\n"
      assert {:ok, ^expected_response} = Commands.Echo.execute([message])
    end
  end
end
