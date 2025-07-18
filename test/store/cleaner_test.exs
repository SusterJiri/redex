defmodule Store.CleanerTest do
  use ExUnit.Case

  setup do
    # Clean up any existing table
    if :ets.whereis(:redis_store) != :undefined do
      :ets.delete(:redis_store)
    end

    Store.setup_store()

    # Start the cleaner GenServer for testing
    {:ok, _pid} = Store.Cleaner.start_link([])

    :ok
  end

  describe "expired key cleanup" do
    test "clean_expired_keys removes only expired keys" do
      # Set up test data
      Store.set("permanent", "forever")
      Store.set_with_ttl("expire_soon", "temp1", 50)
      Store.set_with_ttl("expire_later", "temp2", 2000)

      # Wait for first key to expire
      Process.sleep(100)

      # Manually call the cleanup function
      _cleaned_count = send(Store.Cleaner, :cleanup)

      # Give it time to process
      Process.sleep(50)

      # Check results
      assert {:ok, "forever"} = Store.get("permanent")
      assert {:error, :not_found} = Store.get("expire_soon")
      assert {:ok, "temp2"} = Store.get("expire_later")
    end

    test "clean_expired_keys handles empty store" do
      # Should not crash on empty store
      assert :ok = GenServer.cast(Store.Cleaner, :cleanup)
    end

    test "clean_expired_keys handles store with no expiring keys" do
      Store.set("key1", "value1")
      Store.set("key2", "value2")

      # Should not crash or remove permanent keys
      assert :ok = GenServer.cast(Store.Cleaner, :cleanup)

      assert {:ok, "value1"} = Store.get("key1")
      assert {:ok, "value2"} = Store.get("key2")
    end

    test "clean_expired_keys with mixed expiry times" do
      now = :os.system_time(:millisecond)

      # Already expired
      :ets.insert(:redis_store, {"expired1", {"value1", now - 1000}})
      :ets.insert(:redis_store, {"expired2", {"value2", now - 500}})

      # Future expiry
      Store.set_with_ttl("future", "value3", 10000)

      # Permanent
      Store.set("permanent", "value4")

      # Call cleanup
      send(Store.Cleaner, :cleanup)
      Process.sleep(50)

      # Check results
      assert {:error, :not_found} = Store.get("expired1")
      assert {:error, :not_found} = Store.get("expired2")
      assert {:ok, "value3"} = Store.get("future")
      assert {:ok, "value4"} = Store.get("permanent")
    end
  end
end
