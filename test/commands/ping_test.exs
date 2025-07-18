defmodule Commands.PingTest do
  use ExUnit.Case

  describe "PING command" do
    test "PING with no arguments returns PONG" do
      assert {:ok, "+PONG\r\n"} = Commands.Ping.execute([])
    end

    test "PING with arguments returns error" do
      assert {:error, "PING command expects no arguments"} = Commands.Ping.execute(["arg"])
      assert {:error, "PING command expects no arguments"} = Commands.Ping.execute(["arg1", "arg2"])
    end
  end
end
