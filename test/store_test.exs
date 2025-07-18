defmodule StoreTest do
  use ExUnit.Case
  doctest Store

  setup do
    # Clean up any existing table
    if :ets.whereis(:redis_store) != :undefined do
      :ets.delete(:redis_store)
    end

    Store.setup_store()
    :ok
  end

  describe "basic operations" do
    test "set and get a value" do
      assert {:ok, "hello"} = Store.set("mykey", "hello")
      assert {:ok, "hello"} = Store.get("mykey")
    end

    test "get non-existent key returns error" do
      assert {:error, :not_found} = Store.get("nonexistent")
    end

    test "set overwrites existing value" do
      Store.set("mykey", "old_value")
      assert {:ok, "new_value"} = Store.set("mykey", "new_value")
      assert {:ok, "new_value"} = Store.get("mykey")
    end
  end

  describe "TTL operations" do
    test "set with TTL and retrieve before expiry" do
      ttl_ms = 1000  # 1 second
      assert {:ok, "value"} = Store.set_with_ttl("ttl_key", "value", ttl_ms)
      assert {:ok, "value"} = Store.get("ttl_key")
    end

    test "set with TTL and key expires" do
      ttl_ms = 50  # 50 milliseconds
      Store.set_with_ttl("expire_key", "value", ttl_ms)

      # Wait for expiry
      Process.sleep(100)

      assert {:error, :not_found} = Store.get("expire_key")
    end

    test "expired key is automatically deleted on access" do
      ttl_ms = 50
      Store.set_with_ttl("auto_delete", "value", ttl_ms)

      # Verify key exists in ETS before expiry
      assert [{_, _}] = :ets.lookup(:redis_store, "auto_delete")

      Process.sleep(100)

      # Access should delete the key
      Store.get("auto_delete")

      # Key should be gone from ETS
      assert [] = :ets.lookup(:redis_store, "auto_delete")
    end

    test "mix of TTL and non-TTL keys" do
      Store.set("permanent", "forever")
      Store.set_with_ttl("temporary", "short_lived", 50)

      assert {:ok, "forever"} = Store.get("permanent")
      assert {:ok, "short_lived"} = Store.get("temporary")

      Process.sleep(100)

      assert {:ok, "forever"} = Store.get("permanent")
      assert {:error, :not_found} = Store.get("temporary")
    end
  end
end
