ExUnit.start()

Application.put_env(:codecrafters_redis, :start_server, false)

# Configure ExUnit
ExUnit.configure(
  exclude: [:skip],
  timeout: 10_000,
  capture_log: true
)

# Helper functions for tests can go here
defmodule TestHelper do
  def wait_for_expiry(milliseconds \\ 100) do
    Process.sleep(milliseconds)
  end

  def setup_clean_store do
    if :ets.whereis(:redis_store) != :undefined do
      :ets.delete(:redis_store)
    end
    Store.setup_store()
  end
end
