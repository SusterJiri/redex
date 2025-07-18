defmodule Commands.GetTest do
  use ExUnit.Case

  setup do
    # Clean up any existing table
    if :ets.whereis(:redis_store) != :undefined do
      :ets.delete(:redis_store)
    end

    Store.setup_store()
    :ok
  end

  describe "GET command" do
    test "GET existing key returns value in RESP format" do
      Store.set("mykey", "myvalue")

      assert {:ok, "$7\r\nmyvalue\r\n"} = Commands.Get.execute(["mykey"])
    end

    test "GET non-existent key returns null bulk string" do
      assert {:ok, "$-1\r\n"} = Commands.Get.execute(["nonexistent"])
    end

    test "GET with wrong number of arguments returns error" do
      assert {:error, _} = Commands.Get.execute([])
      assert {:error, _} = Commands.Get.execute(["key1", "key2"])
    end

    test "GET calculates correct byte size for RESP format" do
      Store.set("test", "hello")
      assert {:ok, "$5\r\nhello\r\n"} = Commands.Get.execute(["test"])

      Store.set("unicode", "🎉")
      # Emoji is 4 bytes in UTF-8
      assert {:ok, "$4\r\n🎉\r\n"} = Commands.Get.execute(["unicode"])
    end

    test "GET expired key returns null bulk string" do
      Store.set_with_ttl("expire_key", "value", 50)
      assert {:ok, "$5\r\nvalue\r\n"} = Commands.Get.execute(["expire_key"])

      Process.sleep(100)
      assert {:ok, "$-1\r\n"} = Commands.Get.execute(["expire_key"])
    end
  end
end
